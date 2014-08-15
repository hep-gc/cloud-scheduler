#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

# Copyright (C) 2009 University of Victoria
# You may distribute under the terms of either the GNU General Public
# License or the Apache v2 License, as specified in the README file.

## Auth.: Duncan Penfold-Brown. 8/07/2009.
## Auth.: Patrick Armstrong

## JOB SCHEDULER MANAGEMENT
##
## Implements the job scheduler component of the cloud scheduler. This file will
## contain classes and functions for querying, parsing, and storing job's as
## they appear in a configured  condor job pool (currently locally accessible).
##
## Methods will exist to poll the job pool, refreshing the pool of all total
## jobs known to the system. Support will be added for job sets, which will
## be subsets of the job pool that are provided for the scheduler to determine
## cloud scheduling schemes that best fit a job set.
##


##
## IMPORTS
##
from __future__ import with_statement
import os
import re
import sys
import shlex
import string
import logging
import datetime
import threading
import subprocess
from urllib2 import URLError
from StringIO import StringIO
from collections import defaultdict

import cloudscheduler.config as config
from cloudscheduler.utilities import determine_path
from cloudscheduler.utilities import get_cert_expiry_time
from cloudscheduler.utilities import splitnstrip
import job_containers
from decimal import *

##
## LOGGING
##

log = None


##
## CLASSES
##

class Job:
    """
    Job Class - Represents a job as read from the Job Scheduler


    """
    # A list of possible statuses for internal job representation
    SCHEDULED = "Scheduled"
    UNSCHEDULED = "Unscheduled"
    statuses = (SCHEDULED, UNSCHEDULED)

    def __init__(self, GlobalJobId="None", Owner="Default-User", JobPrio=1,
             JobStatus=0, ClusterId=0, ProcId=0, VMType=None, VMNetwork=None,
             VMCPUCores=None, VMName=None, VMLoc=None, VMAMI=None, VMMem=None,
             VMStorage=None, VMKeepAlive=0, VMHighPriority=0, RemoteHost=None,
             CSMyProxyCredsName=None, CSMyProxyServer=None, CSMyProxyServerPort=None,
             x509userproxysubject=None, x509userproxy=None,
             Iwd=None, SUBMIT_x509userproxy=None, CSMyProxyRenewalTime="12",
             VMInstanceType=None, 
             VMMaximumPrice=None, VMJobPerCore=False,
             TargetClouds=None, ServerTime=0, JobStartDate=0, VMHypervisor=None,
             VMProxyNonBoot=config.default_VMProxyNonBoot,
             VMImageProxyFile=None, VMTypeLimit=-1, VMImageID=None,
             VMInstanceTypeIBM=None, VMLocation=None, VMKeyName=None,
             VMSecurityGroup="", VMUserData="", VMAMIConfig=None, VMUseCloudInit=False, VMInjectCA=config.default_VMInjectCA, **kwargs):
        """
     Parameters:
     GlobalJobID  - (str) The ID of the job (via condor). Functions as name.
     User       - (str) The user that submitted the job to Condor
     Priority   - (int) The priority given in the job submission file (default = 1)
     VMType     - (str) The VMType required by the job (set in VM's condor_config file)
     VMNetwork  - (str) The network association the job requires. TODO: Should support "any"
     VMCPUArch  - (str) The CPU architecture the job requires in its run environment
     VMName     - (str) The name of the image the job is to run on
     VMLoc      - (str) The location (URL) of the image the job is to run on
     VMAMI      - (str) The Amazon AMI of the image to be run
     VMMem      - (int) The amount of memory in MB the job requires
     VMCPUCores - (int) The number of cpu cores the job requires
     VMStorage  - (int) The amount of storage space the job requires
     VMKeepAlive - (int) The Length of time to keep alive before idle shutdown
     VMHighPriority - (int) Indicates a High priority job / VM (default = 0)
     VMInstanceType - (str) The EC2 instance type of the VM requested
     VMMaximumPrice - (str) The maximum price in cents per hour for a VM (EC2 Only)
     VMUserData - (str) The EC2 user data passed into VM
     VMAMIConfig - (str) AMI Config file to use as part of contextualization
     CSMyProxyCredsName - (str) The name of the credentials to retreive from the myproxy server
     CSMyProxyServer - (str) The hostname of the myproxy server to retreive user creds from
     CSMyProxyServerPort - (str) The port of the myproxy server to retreive user creds from
     x509userproxysubject - (str) The DN of the authenticated user
     x509userproxy - (str) The user proxy certificate (full path)
     SUBMIT_x509userproxy - (str) The user proxy certificate (full path) as originally submitted
     Iwd - (str) The initial working directory (spool directory) of the job. Used in spooled jobs
     VMJobPerCore   - (boolean) Whether or not the machines you request will have
                                multiple slots. This is mostly an advanced feature
                                for when this can save you money (eg. with EC2)

     """

        global log
        log = logging.getLogger("cloudscheduler")
        if not VMType:
            VMType = config.default_VMType
        if not VMNetwork:
            VMNetwork = config.default_VMNetwork
        if not VMHypervisor:
            VMHypervisor = config.default_VMHypervisor
        if not VMName:
            VMName = config.default_VMName
        if not VMLoc:
            VMLoc = config.default_VMLoc
        if not VMAMI:
            VMAMI = _attr_list_to_dict(config.default_VMAMI)
        if not VMInstanceType:
            VMInstanceType = _attr_list_to_dict(config.default_VMInstanceTypeList)
        if not VMMem:
            VMMem = config.default_VMMem
        if not VMCPUCores:
            VMCPUCores = config.default_VMCPUCores
        if not VMStorage:
            VMStorage = config.default_VMStorage
        if not TargetClouds:
            TargetClouds = config.default_TargetClouds
        if not VMAMIConfig:
            VMAMIConfig = config.default_VMAMIConfig
        if not VMMaximumPrice:
            VMMaximumPrice = config.default_VMMaximumPrice
        if not VMJobPerCore:
            VMJobPerCore = config.default_VMJobPerCore
    
        self.id           = GlobalJobId
        self.user         = Owner
        self.uservmtype   = ':'.join([Owner, VMType])
        self.priority     = int(JobPrio)
        self.job_status   = int(JobStatus)
        self.cluster_id   = int(ClusterId)
        self.proc_id      = int(ProcId)
        self.req_vmtype   = VMType
        self.req_network  = VMNetwork
        self.req_image    = VMName
        self.req_imageloc = VMLoc
        self.req_ami      = VMAMI

        try:
            self.req_memory   = int(VMMem)
        except:
            log.exception("VMMem not int: %s" % VMMem)
            raise ValueError
        try:
            self.req_cpucores = int(VMCPUCores)
        except:
            log.exception("VMCPUCores not int: %s" % VMCPUCores)
            raise ValueError
        try:
            self.req_storage  = int(VMStorage)
        except:
            log.exception("VMStorage not int: %s" % VMStorage)
            raise ValueError
        try:
            self.keep_alive   = int(VMKeepAlive) * 60 # Convert to seconds
        except:
            log.exception("VMKeepAlive not int: %s" % VMKeepAlive)
            raise ValueError
        try:    
            self.high_priority = int(VMHighPriority)
        except:
            log.exception("VMHighPriority not int: %s" % VMHighPriority)
            raise ValueError
        self.instance_type = VMInstanceType
        try:
            self.maximum_price = float(VMMaximumPrice)
        except:
            log.exception("VMMaximumPrice not float: %s" % VMMaximumPrice)
            raise ValueError
        self.myproxy_server = CSMyProxyServer
        self.myproxy_server_port = CSMyProxyServerPort
        self.myproxy_creds_name = CSMyProxyCredsName
        self.x509userproxysubject = x509userproxysubject
        self.x509userproxy = x509userproxy
        self.original_x509userproxy = SUBMIT_x509userproxy
        self.spool_dir = Iwd
        self.req_cpuarch=None
        self.x509userproxy_expiry_time = None
        self.proxy_renew_time = CSMyProxyRenewalTime
        self.job_per_core = VMJobPerCore in ['true', "True", True]
        self.remote_host = RemoteHost
        self.running_cloud = ""
        self.running_vm = None
        self.servertime = ServerTime
        self.jobstarttime = JobStartDate
        self.banned = False
        self.ban_time = None
        self.machine_reserved = ""     #Used for FIFO scheduling to determine which, if any, machine is reserved (stores the "Name" dict key)
        self.req_hypervisor = [x.lower() for x in splitnstrip(',', VMHypervisor)]
        self.proxy_non_boot = VMProxyNonBoot in ['true', 'True', True, 'TRUE']
        self.vmimage_proxy_file = VMImageProxyFile
        try:
            self.usertype_limit = int(VMTypeLimit)
        except:
            log.exception("VMTypeLimit not int: %s" % VMTypeLimit)
            raise ValueError
        self.req_image_id = VMImageID
        self.req_instance_type_ibm = VMInstanceTypeIBM
        self.location = VMLocation
        self.key_name = VMKeyName
        self.req_security_group = splitnstrip(',', VMSecurityGroup)
        self.user_data = splitnstrip(',', VMUserData)
        self.ami_config = VMAMIConfig
        self.use_cloud_init = VMUseCloudInit in ['true', 'True', True, 'TRUE']
        self.inject_ca = VMInjectCA in ['true', 'True', True, 'TRUE']

        # Set the new job's status
        if self.job_status == 2:
            self.status = self.statuses[0]
        else:
            self.status = self.statuses[1]
        self.override_status = None
        self.block_time = None
        self.failed_boot = 0
        self.failed_boot_reason = set()
        self.last_boot_attempt = None
        self.blocked_clouds = []
        self.target_clouds = []
        try:
            if TargetClouds and len(TargetClouds) != 0:
                for cloud in TargetClouds.split(','):
                    self.target_clouds.append(cloud.strip())
        except:
            log.error("Failed to parse TargetClouds - use a comma separated list")

        #log.verbose("Job ID: %s, User: %s, Priority: %d, VM Type: %s, Network: %s, Image: %s, Image Location: %s, AMI: %s, Memory: %d" \
        #  % (self.id, self.user, self.priority, self.req_vmtype, self.req_network, self.req_image, self.req_imageloc, self.req_ami, self.req_memory))

    def __repr__(self):
        return "Job '%s'" % self.id


    
    def log(self):
        """Log a short string representing the job."""
        log.info("Job ID: %s, User: %s, Priority: %d, VM Type: %s, Image location: %s, Memory: %d, MyProxy creds: %s, MyProxyServer: %s:%s" \
          % (self.id, self.user, self.priority, self.req_vmtype, self.req_imageloc, self.req_memory, self.myproxy_creds_name, self.myproxy_server, self.myproxy_server_port))
    def log_dbg(self):
        """Log a longer string representing the job."""
        log.debug("Job ID: %s, User: %s, Priority: %d, VM Type: %s, Image location: %s, Memory: %d, MyProxy creds: %s, MyProxyServer: %s:%s" \
          % (self.id, self.user, self.priority, self.req_vmtype, self.req_imageloc, self.req_memory, self.myproxy_creds_name, self.myproxy_server, self.myproxy_server_port))
    def get_job_info(self):
        """Formatted job info output for cloud_status -q."""
        CONDOR_STATUS = ("New", "Idle", "Running", "Removed", "Complete", "Held", "Error")
        return "%-20s %-15s %-15s %-10s %-12s %-15s\n" % (self.id[-20:], self.user[:15], self.req_vmtype[:15], CONDOR_STATUS[self.job_status], self.status[:12] if not self.override_status else self.override_status[:12], self.running_cloud[:15])
    @staticmethod
    def get_job_info_header():
        """Job info Header output for cloud_status -q."""
        return "%-20s %-15s %-15s %-10s %-12s %-15s\n" % ("Global ID", "User", "VM Type", "Job Status", "Status", "Cloud")
    def get_job_info_pretty(self):
        """Job info with header output for a job."""
        output = self.get_job_info_header()
        output += self.get_job_info()
        return output

    def get_id(self):
        """Return the job id (Condor job id)."""
        return self.id

    def get_priority(self):
        """Return the condor job priority of the job."""
        return self.priority

    def set_status(self, status):
        """Sets the job's status to the given string

        Parameters:
            status   - (str) A string indicating the job's new status.
        
        Note: Status must be one of Scheduled, Unscheduled

        """
        if (status not in self.statuses):
            log.error("Error: incorrect status '%s' passed. Status must be one of: %s" % (status, "".join(self.statuses, ", ")))
            return
        self.status = status

    def get_myproxy_server(self):
        """Returns address of the myproxy server for job."""
        return self.myproxy_server

    def get_myproxy_server_port(self):
        """Returns the myproxy server port that was specified for job."""
        return self.myproxy_server_port

    def get_myproxy_creds_name(self):
        """Returns the username to use with myproxy for job."""
        return self.myproxy_creds_name

    def get_renew_time(self):
        """Return the MyProxy proxy renewal time associated with this Job."""
        return self.proxy_renew_time

    def set_myproxy_server(self, v):
        """Set the address of the myproxy server for job."""
        self.myproxy_server = v
        return

    def set_myproxy_server_port(self, v):
        """Set the myproxy server port for job."""
        self.myproxy_server_port = v
        return

    def set_myproxy_creds_name(self, v):
        """Set the username to use with myproxy for job."""
        self.myproxy_creds_name = v
        return

    def get_x509userproxy(self):
        """Returns path of the proxy file."""
        proxy = ""
        if self.spool_dir and self.original_x509userproxy:
            proxy += self.spool_dir + "/"

        log.verbose("spool: %s orig: %s x509prox: %s" % (self.spool_dir, self.original_x509userproxy, self.x509userproxy))

        if self.x509userproxy == None:
            proxy = None
        else:
            proxy += self.x509userproxy

        return proxy

    def get_x509userproxysubject(self):
        """Get the certificate DN of proxy for job."""
        return self.x509userproxysubject


    def get_x509userproxy_expiry_time(self):
        """
        Use this method to get the expiry time of the job's user proxy, if any.
        Note that lazy initialization is done;  the expiry time will be extracted from the
        user proxy the first time the method is called and then it will be cached in the
        instance variable.

        Returns the expiry time as a datetime.datetime instance (UTC), or None if there is no
        user proxy associated with this job.

        """
        if (self.x509userproxy_expiry_time == None) and (self.get_x509userproxy() != None):
            self.x509userproxy_expiry_time = get_cert_expiry_time(self.get_x509userproxy())
        return self.x509userproxy_expiry_time

    def reset_x509userproxy_expiry_time(self):
        """Use this method to trigger an update of the proxy expiry time next time it is checked.
           For example, this must be called right after the proxy has been renewed.
           See get_x509userproxy_expiry_time for more info about how the proxy expiry time is
           cached in memory.
        """
        self.x509userproxy_expiry_time = None

    def needs_proxy_renewal(self):
        """This method will test if a job's user proxy needs to be refreshed, according
           the job proxy refresh threshold found in the cloud scheduler configuration.
    
           Returns True if the proxy needs to be refreshed, or False otherwise (or if
           the job has no user proxy associated with it).
        """
        expiry_time = self.get_x509userproxy_expiry_time()
        if expiry_time == None:
            return False
        td = expiry_time - datetime.datetime.utcnow()
        td_in_seconds = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
        return td_in_seconds < config.job_proxy_renewal_threshold

    def is_proxy_expired(self):
        """This method will test if a job's user proxy is expired.
    
           Returns True if the proxy is expired, False otherwise.
        """
        expiry_time = self.get_x509userproxy_expiry_time()
        if expiry_time == None:
            return False
        return expiry_time <= datetime.datetime.utcnow()

    def has_same_reqs(self, job):
        """A method that will compare a job's requirements listed below with another job to see if they all match."""
        return self.req_vmtype == job.req_vmtype and self.req_cpucores == job.req_cpucores and self.req_memory == job.req_memory and self.req_storage == job.req_storage and self.req_cpuarch == job.req_cpuarch and self.req_network == job.req_network and self.user == job.user

    def get_vmimage_proxy_file_path(self):
        proxypath = []
        proxyfilepath= ''

        if self.spool_dir and self.vmimage_proxy_file:
            proxypath.append(self.spool_dir)
            if self.vmimage_proxy_file.startswith('/'):
                proxypath.append(self.vmimage_proxy_file)
            else:
                proxypath.append('/')
                proxypath.append(self.vmimage_proxy_file)
            
            proxyfilepath = ''.join(proxypath)
            if not os.path.isfile(proxyfilepath):
                log.debug("Could not locate the proxy file at %s. Trying alternate location." % proxyfilepath)
                proxyfilepath = self.vmimage_proxy_file
                if not os.path.isfile(proxyfilepath):
                    log.debug("Could not locate the proxy file at %s." % proxyfilepath)
                    proxyfilepath = ''
                    # going to try stripping any extra path from the entered value
                    proxy_file_name = self.vmimage_proxy_file.split('/')
                    if len(proxy_file_name) > 1:
                        proxy_file_name = proxy_file_name[-1]
                    else:
                        proxy_file_name = proxy_file_name[0]
                    proxyfilepath = ''.join([self.spool_dir, '/', proxy_file_name])
                    if not os.path.isfile(proxyfilepath):
                        log.debug("Could not locate the proxy file at %s either." % proxyfilepath)
                        proxyfilepath = ''
        elif self.vmimage_proxy_file:
            if os.path.isfile(self.vmimage_proxy_file):
                proxyfilepath = self.vmimage_proxy_file
        return proxyfilepath

    def get_ami_dict(self):
        return self.req_ami
    def get_type_dict(self):
        return self.instance_type

class JobPool:
    """ A pool of all jobs read from the job scheduler. Stores all jobs until they
 complete. Keeps scheduled and unscheduled jobs.
    """

    ## Instance Variables:

    # The job container that will hold and maintain the job instances.
    job_container = None


    ## Condor Job Status mapping
    NEW      = 0
    IDLE     = 1
    RUNNING  = 2
    REMOVED  = 3
    COMPLETE = 4
    HELD     = 5
    ERROR    = 6

    # The condor timeout is so huge because busy schedds with lots of jobs
    # can take a REALLY long time to return the XML list of jobs
    CONDOR_TIMEOUT = 1200 # seconds (20min)

    ## Instance Methods

    def __init__(self, name, condor_query_type=""):
        """Constructor for JobPool class
        
        Keyword arguments:
        name              - The name of the job pool being created
        condor_query_type - The method to use for querying condor
        
        """

        global log
        log = logging.getLogger("cloudscheduler")
        log.debug("New JobPool %s created" % name)
        self.job_container = job_containers.HashTableJobContainer()

        self.name = name
        self.last_query = None
        self.write_lock = threading.RLock()

        if not condor_query_type:
            condor_query_type = config.condor_retrieval_method

        if condor_query_type.lower() == "local":
            self.job_query = self.job_query_local
        else:
            log.error("Can't use '%s' retrieval method. Using local method." % condor_query_type)
            self.job_query = self.job_query_local
            
        if config.job_distribution_type.lower() == "normal":
            #self.job_type_distribution = self.job_type_distribution_normal
            self.job_type_distribution = self.job_usertype_distribution_normal
        elif config.job_distribution_type.lower() == "split":
            #self.job_type_distribution = self.job_type_distribution_multi_vmtype
            self.job_type_distribution = self.job_usertype_distribution_multi_vmtype

    def get_all_jobs(self):
        """Method to get all jobs in the JobPool

           Returns a list of Job instances, or [] if there are no jobs in the the JobPool.
        """

        jobs = []
        for job_list in self.new_jobs.values():
            jobs.extend(job_list)
        for job_list in self.sched_jobs.values():
            jobs.extend(job_list)
        for job_list in self.high_jobs.values():
            jobs.extend(job_list)
        return jobs

    def job_query_local(self):
        """job_query_local -- query and parse condor_q for job information."""
        log.verbose("Querying Condor scheduler daemon (schedd) with %s" % config.condor_q_command)
        try:
            condor_q = shlex.split(config.condor_q_command)
            sp = subprocess.Popen(condor_q, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (condor_out, condor_err) = sp.communicate(input=None)
            returncode = sp.returncode
        except:
            log.exception("Problem running %s, unexpected error" % string.join(condor_q, " "))
            return None

        if returncode != 0:
            log.error("Got non-zero return code '%s' from '%s'. stderr was: %s" %
                              (returncode, string.join(condor_q, " "), condor_err))
            return None

        job_ads = self._condor_q_to_job_list(condor_out)
        self.last_query = datetime.datetime.now()
        return job_ads

    @staticmethod
    def _condor_q_to_job_list(condor_q_output):
        """
        _condor_q_to_job_list - Converts the output of condor_q
                to a list of Job Objects

                returns [] if there are no jobs
        """

        def _attribute_from_requirements(requirements, attribute):
            regex = "%s\s=\?=\s\"(?P<value>[^\"].+?)\"" % attribute
            match = re.search(regex, requirements)
            if match:
                return match.group("value")
            else:
                return ""

        def _attribute_from_requirements_alt(requirements, attribute):
            regex = "%s\s[<>=][<>=]\s(?P<value>[^\"].+?)\s" % attribute
            match = re.search(regex, requirements)
            if match:
                return match.group("value")
            else:
                return ""

        def _attribute_from_list(classad, attribute):
            try:
                attr_list = classad[attribute]
                try:
                    attr_dict = _attr_list_to_dict(attr_list)
                    classad[attribute] = attr_dict
                except ValueError:
                    log.exception("Problem extracting %s attribute '%s'" % (attribute, attr_list))
            except:
                pass

        jobs = []

        # The first three lines look like:
        # \n\n\t-- Submitter: hostname : <ip> : hostname
        # we can just strip these.
        condor_q_output = re.sub('\n\n.*?Submitter:.*?\n', "", condor_q_output, re.MULTILINE)

        # Each classad is seperated by '\n\n'
        raw_job_classads = condor_q_output.split("\n\n")
        # Empty condor pools give us an empty string in our list
        raw_job_classads = filter(lambda x: x != "", raw_job_classads)

        for raw_classad in raw_job_classads:
            classad = {}
            classad_lines = raw_classad.splitlines()
            for classad_line in classad_lines:
                classad_line = classad_line.strip()
                (classad_key, classad_value) = classad_line.split(" = ", 1)
                classad_value = classad_value.strip('"')
                classad[classad_key] = classad_value

            try:
                classad["VMType"] = _attribute_from_requirements(classad["Requirements"], "VMType")
            except:
                log.exception("Problem extracting VMType from Requirements")

            if config.vm_reqs_from_condor_reqs:
                if not classad.has_key("VMMem"):
                    try:
                        classad["VMMem"] = int(_attribute_from_requirements_alt(classad["Requirements"], "Memory"))
                    except:
                        log.exception("Problem extracting Memory from Requirements")
                if not classad.has_key("VMStorage"):
                    try:
                        classad["VMStorage"] = int(_attribute_from_requirements_alt(classad["Requirements"], "Disk")) / 1000000
                        if classad["VMStorage"] < 1:
                            classad["VMStorage"] = 1
                    except:
                        log.exception("Problem extracting Disk from Requirements")
                if not classad.has_key("VMCPUCores"):
                    try:
                        classad["VMCPUCores"] = int(_attribute_from_requirements_alt(classad["Requirements"], "Cpus"))
                    except:
                        log.exception("Problem extracting Cpus from Requirements")
            # VMAMI requires special fiddling
            _attribute_from_list(classad, "VMAMI")
            _attribute_from_list(classad, "VMInstanceType")
            try:            
                jobs.append(Job(**classad))
            except ValueError:
                log.exception("Failed to add job: %s due to Value Errors in jdl." % classad["GlobalJobId"])
            except:
                log.exception("Failed to add job: %s due to unspecified exception." % classad["GlobalJobId"])
        return jobs
 
    def update_jobs(self, query_jobs):
        """Updates the system jobs:
            - Removes finished or deleted jobs from the system
            - Ignores jobs already in the system and still in Condor
            - Adds all new jobs to the system
           Keywords:
            - query_jobs - (list of Job objects) The jobs received from a condor query
        """
        # If no jobs recvd, remove all jobs from the system (all have finished or have been removed)
        if (query_jobs == []):
            log.debug("No jobs received from job query. Removing all jobs from the system.")
            self.job_container.clear()
            return

        # Filter out any jobs in an error status (from the given job list)
        jobs_removed_due_status = 0
        for job in reversed(query_jobs):
            if job.job_status >= self.REMOVED:
                jobs_removed_due_status += 1
                query_jobs.remove(job)
        log.verbose("Jobs removed due to status held, removed, error, complete: %i" % jobs_removed_due_status)
        # Update all system jobs:
        #   - remove jobs already in the system from the jobs list
        #   - remove finished jobs (job in system, not in jobs list)

        # DBG: print both jobs dicts before updating system.
        #log.verbose("System jobs prior to system update:")
        #log.verbose("Unscheduled Jobs (new_jobs):")
        #self.log_unsched_jobs()
        #log.verbose("Scheduled Jobs (sched_jobs):")
        #self.log_sched_jobs()
        #log.verbose("High Priority Jobs (high_jobs):")
        #self.log_high_jobs()

        # Lets remove all jobs in the container that do not appear in the
        # given condor job list.
        # Keep a list of the removed jobs
        removed = self.job_container.remove_all_not_in(query_jobs)
        self.track_run_time(removed)

        # Now lets loop through the remaining jobs given by condor and
        # check if each job is in the job container.
        # If yes, then it means the job is not new (the container already
        # knows about it) and we simply need to update it.
        # In that case, we also remove it from the query_jobs, so that
        # only new jobs will remain in query_jobs after this step.
        jobs_to_update = []
        new_jobs = []
        for job in query_jobs:
            if self.job_container.has_job(job.id):
                jobs_to_update.append(job)
            else:
                new_jobs.append(job)
        query_jobs = new_jobs

        # Add all jobs remaining in jobs list to the Unscheduled job set (new_jobs)
        for job in query_jobs:
            if job.high_priority == 0 or  not config.high_priority_job_support:
                self.add_new_job(job)
            else:
                self.add_high_job(job)
        del query_jobs


        # Update job status of all the non-new jobs
        log.verbose("Updating job status of %d jobs" % (len(jobs_to_update)))
        for job in jobs_to_update:
            self.update_job_status(job)
            #print job.get_ami_dict()
            #print job.get_type_dict()
        del jobs_to_update

        # DBG: print both jobs dicts before updating system.
        #log.verbose("System jobs after system update:")
        #log.verbose("Unscheduled Jobs (new_jobs):")
        #self.log_unsched_jobs()
        #log.verbose("Scheduled Jobs (sched_jobs):")
        #self.log_sched_jobs()
        #log.verbose("High Priority Jobs (high_jobs):")
        #self.log_high_jobs()

    def add_new_job(self, job):
        """Add New Job
            Add a new job to the system (in the new_jobs set)
            Added in order (of priority)
        """
        self.job_container.add_job(job)

    
    def add_sched_job(self, job):
        """Add a job to the scheduled jobs set in the system."""
        self.job_container.add_job(job)


    def add_high_job(self, job):
        """Add High(Priority) Job (in the high_jobs set)."""
        self.job_container.add_job(job)


    def remove_system_job(self, job):
        """Remove System Job.

        Attempts to remove a given job from the JobPool unscheduled
        or scheduled job dictionaries.
           Keywords:
                job - (Job) the job to be removed from the system
        No return (if job does not exist in system, error message logged)
        """
        self.job_container.remove_job(job)


    def update_job_status(self, target_job):
        """Update the status of a job.

        Keywords:
            target_job - the job to update
        Returns
            True - updated
            False - failed
        """
        return self.job_container.update_job_status(target_job.id, int(target_job.job_status), target_job.remote_host, target_job.servertime, target_job.jobstarttime)

    def schedule(self, job):
        """Makes all changes to a job to indicate that the job has been scheduled.

            Keywords:
                job - (Job object) The job to mark as scheduled
        """
        self.job_container.schedule_job(job.id)

    def unschedule(self, job):
        """Makes all changes to a job to indicate that the job has been unscheduled.

            Keywords:
                job - (Job object) The job to mark as unscheduled
        """
        self.job_container.unschedule_job(job.id)

    def get_required_vmtypes(self):
        """Get a list of required VM types.

           Returns a list (of strings) containing the unique required VM types
           gathered from all jobs in the job pool (scheduled and unscheduled)
        Returns:
           required_vmtypes - (list of strings) A list of required VM types

        """
        required_vmtypes = []
        for job in self.job_container.get_all_jobs():
            if job.req_vmtype not in required_vmtypes and job.job_status <= self.RUNNING \
            and not job.banned:
                required_vmtypes.append(job.req_vmtype)

        log.verbose("get_required_vmtypes - Required VM types: " + ", ".join(required_vmtypes))
        return required_vmtypes

    def get_required_uservmtypes(self):
        """Get a list of the required uservmtype.

           Returns a list (of strings) containing the unique required VM types
           paired with the username in form 'user:vmtype' gathered from all
           jobs in the job pool (scheduled and unscheduled)
        Returns:
            required_vmtypes - (list of strings) A list of required VM types

        """
        required_vmtypes = []
        for job in self.job_container.get_all_jobs():
            if job.uservmtype not in required_vmtypes and job.job_status <= self.RUNNING \
               and not job.banned:
                required_vmtypes.append(job.uservmtype)

        log.verbose("get_required_uservmtypes - Required VM types: " + ", ".join(required_vmtypes))
        return required_vmtypes

    def get_required_vmtypes_dict(self):
        """Get required VM types dictionary containing a count of each vmtype.
    
            Returns a dictionary containing the unique required VM types as a key
            gathered from all jobs in the job pool (scheduled and unscheduled), and
            count of the number of jobs needing that type as the value.
        Returns:
            required_vmtypes - (dictionary, string key, int value)

        """
        required_vmtypes = defaultdict(int)
        for job in self.job_container.get_all_jobs():
            if job.job_status <= self.RUNNING and not job.banned:
                required_vmtypes[job.req_vmtype] += 1
        log.verbose("get_required_vm_types_dict - Required VM Type : Count " + str(required_vmtypes))
        return required_vmtypes

    def get_required_uservmtypes_dict(self):
        """Returns a dictionary containing the unique required VM types.
            Same function as get_required_vmtypes_dict() but has user:vmtype as
            the a key instead of just vmtype.
        Returns:
            required_vmtypes - (dictionary, string key, int value) A dict of required VM types
        """
        required_vmtypes = defaultdict(int)
        for job in self.job_container.get_all_jobs():
            if job.job_status <= self.RUNNING and not job.banned:
                required_vmtypes[job.uservmtype] += 1
        log.verbose("get_required_vm_usertypes_dict - Required VM Type : Count " + str(required_vmtypes))
        return required_vmtypes

    def job_type_distribution_normal(self):
        """Determine a 'fair' distribution of VMs based on jobs in the new_job queue.

        The 'normal' distribution treats a user who has submitted multiple vmtypes
        in whatever order they appear in (or priority).
        """

        type_desired = defaultdict(int)
        new_jobs_by_users = self.job_container.get_unscheduled_jobs_by_users(prioritized = True)
        high_priority_jobs_by_users = self.job_container.get_unscheduled_high_priority_jobs_by_users(prioritized = True)
        held_user_adjust = 0
        for user in new_jobs_by_users.keys():
            vmtype = None
            for job in new_jobs_by_users[user]:
                if job.job_status <= self.RUNNING and not job.banned:
                    vmtype = job.req_vmtype
                    break
            if vmtype == None:
                held_user_adjust -= 1 #This user is completely held
                break
            type_desired[vmtype] += 1 * (1 / Decimal(config.high_priority_job_weight) if high_priority_jobs_by_users else 1)
        for user in high_priority_jobs_by_users.keys():
            vmtype = None
            for job in high_priority_jobs_by_users[user]:
                if job.job_status <= self.RUNNING and not job.banned:
                    vmtype = job.req_vmtype
                    break
            if vmtype == None:
                held_user_adjust -= 1 # this user is completely held
                break
            type_desired[vmtype] += 1 * config.high_priority_job_weight
        num_users = Decimal(held_user_adjust + len(new_jobs_by_users.keys()) + len(high_priority_jobs_by_users.keys()))
        if num_users == 0:
            log.verbose("All users held, completed, or banned")
            return {}
        for vmtype in type_desired.keys():
            type_desired[vmtype] = type_desired[vmtype] / num_users
        return type_desired

    def job_usertype_distribution_normal(self):
        """Determine a 'fair' distribution of VMs based on jobs in the new_job queue.

        The 'normal' distribution treats a user who has submitted multiple vmtypes
        in whatever order they appear in (or priority).
        """
        type_desired = defaultdict(int)
        new_jobs_by_users = self.job_container.get_unscheduled_jobs_by_users(prioritized = True)
        high_priority_jobs_by_users = self.job_container.get_unscheduled_high_priority_jobs_by_users(prioritized = True)
        held_user_adjust = 0
        for user in new_jobs_by_users.keys():
            vmtype = None
            for job in new_jobs_by_users[user]:
                if job.job_status <= self.RUNNING and not job.banned:
                    vmtype = job.uservmtype
                    break
            if vmtype == None:
                held_user_adjust -= 1 #This user is completely held
                continue
            type_desired[vmtype] += 1 * (1 / Decimal(config.high_priority_job_weight) if high_priority_jobs_by_users else 1)
        for user in high_priority_jobs_by_users.keys():
            vmtype = None
            for job in high_priority_jobs_by_users[user]:
                if job.job_status <= self.RUNNING and not job.banned:
                    vmtype = job.uservmtype
                    break
            if vmtype == None:
                held_user_adjust -= 1 # this user is completely held
                continue
            type_desired[vmtype] += 1 * config.high_priority_job_weight
        num_users = Decimal(held_user_adjust + len(new_jobs_by_users.keys()) + len(high_priority_jobs_by_users.keys()))
        if num_users == 0:
            log.verbose("All users held, completed, or banned")
            return {}
        for vmtype in type_desired.keys():
            type_desired[vmtype] = type_desired[vmtype] / num_users
        return type_desired

    def job_type_distribution_multi_vmtype(self):
        """Determine a 'fair' distribution of VMs based on jobs in the new_job queue.

        The 'multi_vmtype' distribution treats a user who has submitted multiple vmtypes
        equally(based on priority) and will split the users share of resources between
        the vmtypes.
        """
        type_desired = {}
        new_jobs_by_users = self.job_container.get_unscheduled_jobs_by_users(prioritized = True)
        high_priority_jobs_by_users = self.job_container.get_unscheduled_high_priority_jobs_by_users(prioritized = True)
        held_user_adjust = 0
        user_types = {}
        high_user_types = {}
        # Want to check all jobs of the highest priority in their list
        for user in new_jobs_by_users.keys():
            vmtypes = set()
            highest_priority = new_jobs_by_users[user][0].priority
            for job in new_jobs_by_users[user]:
                if job.job_status <= self.RUNNING and job.priority == highest_priority and not job.banned:
                    vmtypes.add(job.req_vmtype)
            if len(vmtypes) == 0: # user is held / complete
                held_user_adjust -= 1
            else:
                user_types[user] = vmtypes
        for user in high_priority_jobs_by_users.keys():
            vmtypes = set()
            highest_priority = high_priority_jobs_by_users[user][0].priority
            for job in high_priority_jobs_by_users[user]:
                if job.job_status <= self.RUNNING and job.priority == highest_priority and not job.banned:
                    vmtypes.add(job.req_vmtype)
            if len(vmtypes) == 0: # user is held / complete
                held_user_adjust -= 1
            else:
                high_user_types[user] = vmtypes
        # Types for users gathered - figure out distributions
        for user in user_types.keys():
            for vmtype in user_types[user]:
                if vmtype in type_desired.keys():
                    type_desired[vmtype] += Decimal('1.0') / len(user_types[user]) * (1 / Decimal(config.high_priority_job_weight) if high_priority_jobs_by_users else 1)
                else:
                    type_desired[vmtype] = Decimal('1.0') / len(user_types[user]) * (1 / Decimal(config.high_priority_job_weight) if high_priority_jobs_by_users else 1)
        for user in high_user_types.keys():
            for vmtype in high_user_types[user]:
                if vmtype in type_desired.keys():
                    type_desired[vmtype] += Decimal('1.0') / len(high_user_types[user]) * config.high_priority_job_weight
                else:
                    type_desired[vmtype] = Decimal('1.0') / len(high_user_types[user]) * config.high_priority_job_weight
        num_users = held_user_adjust + len(set(user_types.keys() + high_user_types.keys()))
        if num_users != 0:
            num_users = Decimal('1.0') / num_users
        else:
            log.verbose("All users' jobs held, complete, or banned")
            return {}
        for vmtype in type_desired.keys():
            type_desired[vmtype] *= num_users
        return type_desired

    def job_usertype_distribution_multi_vmtype(self):
        """Determine a 'fair' distribution of VMs based on jobs in the new_job queue.

        The 'multi_vmtype' distribution treats a user who has submitted multiple vmtypes
        equally(based on priority) and will split the users share of resources between
        the vmtypes.
        """

        type_desired = {}
        new_jobs_by_users = self.job_container.get_unscheduled_jobs_by_users(prioritized = True)
        high_priority_jobs_by_users = self.job_container.get_unscheduled_high_priority_jobs_by_users(prioritized = True)
        held_user_adjust = 0
        user_types = {}
        high_user_types = {}
        # Want to check all jobs of the highest priority in their list
        for user in new_jobs_by_users.keys():
            vmtypes = set()
            highest_priority = new_jobs_by_users[user][0].priority
            for job in new_jobs_by_users[user]:
                if job.job_status <= self.RUNNING and job.priority == highest_priority and not job.banned:
                    vmtypes.add(job.uservmtype)
            if len(vmtypes) == 0: # user is held / complete
                held_user_adjust -= 1
            else:
                user_types[user] = vmtypes
        for user in high_priority_jobs_by_users.keys():
            vmtypes = set()
            highest_priority = high_priority_jobs_by_users[user][0].priority
            for job in high_priority_jobs_by_users[user]:
                if job.job_status <= self.RUNNING and job.priority == highest_priority and not job.banned:
                    vmtypes.add(job.uservmtype)
            if len(vmtypes) == 0: # user is held / complete
                held_user_adjust -= 1
            else:
                high_user_types[user] = vmtypes
        # Types for users gathered - figure out distributions
        for user in user_types.keys():
            for vmtype in user_types[user]:
                if vmtype in type_desired.keys():
                    type_desired[vmtype] += Decimal('1.0') / len(user_types[user]) * (1 / Decimal(config.high_priority_job_weight) if high_priority_jobs_by_users else 1)
                else:
                    type_desired[vmtype] = Decimal('1.0') / len(user_types[user]) * (1 / Decimal(config.high_priority_job_weight) if high_priority_jobs_by_users else 1)
        for user in high_user_types.keys():
            for vmtype in high_user_types[user]:
                if vmtype in type_desired.keys():
                    type_desired[vmtype] += Decimal('1.0') / len(high_user_types[user]) * config.high_priority_job_weight
                else:
                    type_desired[vmtype] = Decimal('1.0') / len(high_user_types[user]) * config.high_priority_job_weight
        num_users = held_user_adjust + len(set(user_types.keys() + high_user_types.keys()))
        if num_users != 0:
            num_users = Decimal('1.0') / num_users
        else:
            log.verbose("All users' jobs held, complete, or banned")
            return {}
        for vmtype in type_desired.keys():
            type_desired[vmtype] *= num_users
        return type_desired

    def get_jobs_of_type_for_user(self, vmtype, user):
        """
        get_jobs_of_type_for_user -- get a list of jobs of a VMtype for a user

        returns a list of Job objects.
        """
        jobs = self.job_container.get_jobs_for_user(user)
        return jobs


    def get_usertype_limits(self):
        """
        get_usertype_limits - get a dict of all the usertype limits in the job pool

        returns a dict of uservmtypes with their limits
        """
        jobs = self.job_container.get_all_jobs()
        limits = {}
        for job in jobs:
            if job.usertype_limit > -1:
                limits[job.uservmtype] = job.usertype_limit
        return limits

    def job_hold_local(self, jobs):
        """job_query_local -- query and parse condor_q for job information."""
        log.verbose("Holding Condor jobs with %s" % config.condor_hold_command)
        try:
            condor_out = ""
            condor_err = ""
            log.verbose("Holding jobs via condor_hold.")
            condor_hold = shlex.split(config.condor_hold_command)
            job_ids = [str(job.cluster_id)+"."+str(job.proc_id) for job in jobs]
            condor_hold.extend(job_ids)
            log.verbose("Popen condor_hold command")
            sp = subprocess.Popen(condor_hold, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            log.verbose("Popen communicate condor_hold.")
            (condor_out, condor_err) = sp.communicate(input=None)
            returncode = sp.returncode
        except:
            if condor_err:
                log.exception("Problem running %s, unexpected error" % string.join(condor_err, " "))
            else:
                log.exception("Problem running condor_hold, unexpected error.")
            return None

        if returncode != 0:
            log.error("Got non-zero return code '%s' from '%s'. stderr was: %s" %
                              (returncode, string.join(condor_out, " "), condor_err))
            return None
        return returncode

    def job_release_local(self, jobs):
        """job_query_local -- query and parse condor_q for job information."""
        log.verbose("Releasing Condor jobs with %s" % config.condor_release_command)
        try:
            condor_release = shlex.split(config.condor_release_command)
            job_ids = [str(job.cluster_id)+"."+str(job.proc_id) for job in jobs]
            condor_release.extend(job_ids)
            sp = subprocess.Popen(condor_release, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (condor_out, condor_err) = sp.communicate(input=None)
            returncode = sp.returncode
        except:
            log.exception("Problem running %s, unexpected error" % string.join(condor_release, " "))
            return None

        if returncode != 0:
            log.error("Got non-zero return code '%s' from '%s'. stderr was: %s" %
                              (returncode, string.join(condor_release, " "), condor_err))
            return None
        return returncode

    def track_run_time(self, removed):
        """Keeps track of the approximate run time of jobs on each VM."""
        for job in removed:
            # If job has completed and been removed it's last state should
            # have been running
            if job.job_status == self.RUNNING:
                if int(job.jobstarttime) > 0:
                    if job.running_vm != None:
                        job.running_vm.job_run_times.append(int(job.servertime) - int(job.jobstarttime))

    ##
    ## JobPool Private methods (Support methods)
    ##

    def parse_classAd_requirements(self, requirements):
        """
        Parse classAd Requirements string.
        Takes the Requirements string from a condor job classad and retrieves the
        VMType string. Returns null object if no VMtype is specified.
        NOTE: Could be expanded to return a name=>value dictionary of all Requirements
              fields. (Not currently necessary).
        Parameters:
          requirements - (str) The Requirements string from a condor job classAd, or a
                         VMType string, or None (the null object)
        Return:
          The VMType string or None (null object)
        """

        # Match against the Requirements string
        req_re = "(VMType\s=\?=\s\"(?P<vm_type>.+?)\")"
        match = re.search(req_re, requirements)
        if match:
            log.verbose("parse_classAd_requirements - VMType parsed from "
              + "Requirements string: %s" % match.group('vm_type'))
            return match.group('vm_type')
        else:
            log.verbose("parse_classAd_requirements - No VMType specified. Returning None.")
            return None

    ## Log methods

    def log_jobs(self):
        """Log Job Lists (short)."""
        self.log_sched_jobs()
        self.log_unsched_jobs()

    def log_sched_jobs(self):
        """Log scheduled jobs (short)."""
        for job in self.job_container.get_scheduled_jobs():
            job.log_dbg()

    def log_unsched_jobs(self):
        """Log unscheduled Jobs (short)."""
        for job in self.job_container.get_unscheduled_jobs():
            job.log_dbg()


    def log_high_jobs(self):
        """Log high priority Jobs (short)."""
        for job in self.job_container.get_high_priority_jobs():
            job.log_dbg()

    def log_jobs_list(self, jobs):
        """Log a list of jobs."""
        if jobs == []:
            log.verbose("(none)")
        for job in jobs:
            log.verbose("\tJob: %s, %10s, %4d, %10s" % (job.id, job.user, job.priority, job.req_vmtype))



# utility parsing methods

def _attr_list_to_dict(attr_list):
    """
    _attr_list_to_dict -- parse a string like: host:ami, ..., host:ami into a
    dictionary of the form:
    {
        host: ami
        host: ami
    }

    if the string is in the form "ami" then parse to format
    {
        default: ami
    }

    raises ValueError if list can't be parsed
    """

    attr_dict = {}
    for host_attr in attr_list.split(","):
        host_attr = host_attr.split(":")
        if len(host_attr) == 1:
            attr_dict["default"] = host_attr[0].strip()
        elif len(host_attr) == 2:
            attr_dict[host_attr[0].strip()] = host_attr[1].strip()
        else:
            raise ValueError("Can't split '%s' into suitable host attribute pair" % host_attr)

    return attr_dict
