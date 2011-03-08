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
try:
    from lxml import etree
except:
    print >> sys.stderr, "Couldn't import lxml. You should install it from http://codespeak.net/lxml/, or your package manager."
    sys.exit(1)
try:
    from suds.client import Client
except:
    print >> sys.stderr, "Couldn't import Suds. You should install it from https://fedorahosted.org/suds/, or your package manager"
    sys.exit(1)

import cloudscheduler.config as config
from cloudscheduler.utilities import determine_path
from cloudscheduler.utilities import get_cert_expiry_time
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
    statuses = ("Unscheduled", "Scheduled")

    def __init__(self, GlobalJobId="None", Owner="Default-User", JobPrio=1,
             JobStatus=0, ClusterId=0, ProcId=0, VMType=config.default_VMType,
             VMNetwork=config.default_VMNetwork, VMCPUArch=config.default_VMCPUArch, 
             VMName=config.default_VMName, VMLoc=config.default_VMLoc, 
             VMAMI={"default": config.default_VMAMI}, VMMem=config.default_VMMem, 
             VMCPUCores=config.default_VMCPUCores, VMStorage=config.default_VMStorage, 
             VMKeepAlive=1, VMHighPriority=0, RemoteHost=None,
             CSMyProxyCredsName=None, CSMyProxyServer=None, CSMyProxyServerPort=None,
             x509userproxysubject=None, x509userproxy=None,
             VMInstanceType=config.default_VMInstanceType, 
             VMMaximumPrice=config.default_VMMaximumPrice, VMJobPerCore=False,
             TargetClouds="", **kwargs):
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
     CSMyProxyCredsName - (str) The name of the credentials to retreive from the myproxy server
     CSMyProxyServer - (str) The hostname of the myproxy server to retreive user creds from
     CSMyProxyServerPort - (str) The port of the myproxy server to retreive user creds from
     x509userproxysubject - (str) The DN of the authenticated user
     x509userproxy - (str) The user proxy certificate (full path)
     VMJobPerCore   - (boolean) Whether or not the machines you request will have
                                multiple slots. This is mostly an advanced feature
                                for when this can save you money (eg. with EC2)

     """
     #TODO: Set default job properties in the cloud scheduler main config file
     #     (Have option to set them there, and default values)
        self.id           = GlobalJobId
        self.user         = Owner
        self.priority     = int(JobPrio)
        self.job_status   = int(JobStatus)
        self.cluster_id   = int(ClusterId)
        self.proc_id      = int(ProcId)
        self.req_vmtype   = VMType
        self.req_network  = VMNetwork
        self.req_cpuarch  = VMCPUArch
        self.req_image    = VMName
        self.req_imageloc = VMLoc
        self.req_ami      = VMAMI
        self.req_memory   = int(VMMem)
        self.req_cpucores = int(VMCPUCores)
        self.req_storage  = int(VMStorage)
        self.keep_alive   = int(VMKeepAlive) * 60 # Convert to seconds
        self.high_priority = int(VMHighPriority)
        self.instance_type = VMInstanceType
        self.maximum_price = int(VMMaximumPrice)
        self.myproxy_server = CSMyProxyServer
        self.myproxy_server_port = CSMyProxyServerPort
        self.myproxy_creds_name = CSMyProxyCredsName
        self.x509userproxysubject = x509userproxysubject
        self.x509userproxy = x509userproxy
        self.x509userproxy_expiry_time = None
        self.job_per_core = VMJobPerCore in ['true', "True", True]
        self.remote_host = RemoteHost
        self.running_cloud = ""

        # Set the new job's status
        self.status = self.statuses[0]

        global log
        log = logging.getLogger("cloudscheduler")

        try:
            if len(TargetClouds) != 0:
                self.target_clouds = re.sub(r'\s', '', TargetClouds).split(',')
            else:
                self.target_clouds = []
        except:
            self.target_clouds = []
            log.error("Failed to parse TargetClouds - use a comma separated list")

        log.verbose("New Job ID: %s, User: %s, Priority: %d, VM Type: %s, Network: %s, Image: %s, Image Location: %s, AMI: %s, Memory: %d" \
          % (self.id, self.user, self.priority, self.req_vmtype, self.req_network, self.req_image, self.req_imageloc, self.req_ami, self.req_memory))


    # Log a short string representing the job
    def log(self):
        log.info("Job ID: %s, User: %s, Priority: %d, VM Type: %s, Image location: %s, CPU: %s, Memory: %d, MyProxy creds: %s, MyProxyServer: %s:%s" \
          % (self.id, self.user, self.priority, self.req_vmtype, self.req_imageloc, self.req_cpuarch, self.req_memory, self.myproxy_creds_name, self.myproxy_server, self.myproxy_server_port))
    def log_dbg(self):
        log.debug("Job ID: %s, User: %s, Priority: %d, VM Type: %s, Image location: %s, CPU: %s, Memory: %d, MyProxy creds: %s, MyProxyServer: %s:%s" \
          % (self.id, self.user, self.priority, self.req_vmtype, self.req_imageloc, self.req_cpuarch, self.req_memory, self.myproxy_creds_name, self.myproxy_server, self.myproxy_server_port))
    def get_job_info(self):
        CONDOR_STATUS = ("New", "Idle", "Running", "Removed", "Complete", "Held", "Error")
        return "%-20s %-15s %-15s %-10s %-12s %-15s\n" % (self.id[-20:], self.user[:15], self.req_vmtype[:15], CONDOR_STATUS[self.job_status], self.status[:12], self.running_cloud[:15])
    @staticmethod
    def get_job_info_header():
        return "%-20s %-15s %-15s %-10s %-12s %-15s\n" % ("Global ID", "User", "VM Type", "Job Status", "Status", "Cloud")
    def get_job_info_pretty(self):
        output = self.get_job_info_header()
        output += self.get_job_info()
        return output
    # Get ID
    # Returns the job's id string
    def get_id(self):
        return self.id

    # Get priority
    def get_priority(self):
        return self.priority

    # Set priority
    # Prio must be an integer.
    def set_priority(self, prio):
        try:
            self.priority = int(prio)
        except:
            log.warning("set_priority - Incorrect argument given to set Job priority")
            return

    # Set status
    # Sets the job's status to the given string
    # Parameters:
    #   status   - (str) A string indicating the job's new status.
    # Note: Status must be one of Scheduled, Unscheduled
    def set_status(self, status):
        if (status not in self.statuses):
            log.debug("Error: incorrect status '%s' passed" % status)
            log.debug("Status must be one of: " + string.join(self.statuses, ", "))
            return
        self.status = status

    def get_myproxy_server(self):
        return self.myproxy_server

    def get_myproxy_server_port(self):
        return self.myproxy_server_port

    def get_myproxy_creds_name(self):
        return self.myproxy_creds_name

    def set_myproxy_server(self, v):
        self.myproxy_server = v
        return

    def set_myproxy_server_port(self, v):
        self.myproxy_server_port = v
        return

    def set_myproxy_creds_name(self, v):
        self.myproxy_creds_name = v
        return

    def get_x509userproxy(self):
        return self.x509userproxy

    def get_x509userproxysubject(self):
        return self.x509userproxysubject

    # Use this method to get the expiry time of the job's user proxy, if any.
    # Note that lazy initialization is done;  the expiry time will be extracted from the
    # user proxy the first time the method is called and then it will be cached in the
    # instance variable.
    #
    # Returns the expiry time as a datetime.datetime instance (UTC), or None if there is no
    # user proxy associated with this job.
    def get_x509userproxy_expiry_time(self):
        if (self.x509userproxy_expiry_time == None) and (self.get_x509userproxy() != None):
            self.x509userproxy_expiry_time = get_cert_expiry_time(self.get_x509userproxy())
        return self.x509userproxy_expiry_time

    # Use this method to trigger an update of the proxy expiry time next time it is checked.
    # For example, this must be called right after the proxy has been renewed.
    # See get_x509userproxy_expiry_time for more info about how the proxy expiry time is
    # cached in memory.
    def reset_x509userproxy_expiry_time(self):
        self.x509userproxy_expiry_time = None

    # This method will test if a job's user proxy needs to be refreshed, according
    # the job proxy refresh threshold found in the cloud scheduler configuration.
    #
    # Returns True if the proxy needs to be refreshed, or False otherwise (or if
    # the job has no user proxy associated with it).
    def needs_proxy_renewal(self):
        expiry_time = self.get_x509userproxy_expiry_time()
        if expiry_time == None:
            return False
        td = expiry_time - datetime.datetime.utcnow()
        td_in_seconds = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
        log.debug("needs_proxy_renewal td: %d, threshold: %d" % (td_in_seconds, config.job_proxy_renewal_threshold))
        return td_in_seconds < config.job_proxy_renewal_threshold

    # A method that will compare a job's requirements listed below with another job to see if they
    # all match.
    def has_same_reqs(self, job):
        return self.req_vmtype == job.req_vmtype and self.req_cpucores == job.req_cpucores and self.req_memory == job.req_memory and self.req_storage == job.req_storage and self.req_cpuarch == job.req_cpuarch and self.req_network == job.req_network


# A pool of all jobs read from the job scheduler. Stores all jobs until they
# complete. Keeps scheduled and unscheduled jobs.
class JobPool:

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

    # Constructor
    # name       - The name of the job pool being created
    # last_query - A timestamp for the last time the scheduler was queried,
    #              or its creation time
    def __init__(self, name, condor_query_type=""):
        global log
        log = logging.getLogger("cloudscheduler")
        log.debug("New JobPool %s created" % name)
        self.job_container = job_containers.HashTableJobContainer()

        self.name = name
        self.last_query = None
        self.write_lock = threading.RLock()

        _schedd_wsdl  = "file://" + determine_path() \
                        + "/wsdl/condorSchedd.wsdl"
        self.condor_schedd = Client(_schedd_wsdl,
                                    location=config.condor_webservice_url)
        self.condor_schedd_as_xml = Client(_schedd_wsdl,
                                    location=config.condor_webservice_url,
                                    retxml=True, timeout=self.CONDOR_TIMEOUT)

        if not condor_query_type:
            condor_query_type = config.condor_retrieval_method

        if condor_query_type.lower() == "local":
            self.job_query = self.job_query_local
        elif condor_query_type.lower() == "soap":
            self.job_query = self.job_query_SOAP
        else:
            log.error("Can't use '%s' retrieval method. Using SOAP method." % condor_query_type)
            self.job_query = self.job_query_SOAP
            
        if config.job_distribution_type.lower() == "normal":
            self.job_type_distribution = self.job_type_distribution_normal
        elif config.job_distribution_type.lower() == "split":
            self.job_type_distribution = self.job_type_distribution_multi_vmtype

    # Method to get all jobs in the JobPool
    # Returns a list of Job instances, or [] if there are no jobs in the the JobPool.
    def get_all_jobs(self):
        jobs = []
        for job_list in self.new_jobs.values():
            jobs.extend(job_list)
        for job_list in self.sched_jobs.values():
            jobs.extend(job_list)
        for job_list in self.high_jobs.values():
            jobs.extend(job_list)
        return jobs

    def job_query_local(self):
        """
        job_query_local -- query and parse condor_q for job information
        """
        log.debug("Querying Condor scheduler daemon (schedd) with %s" % config.condor_q_command)
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


    def job_query_SOAP(self):
        log.debug("Querying Condor scheduler daemon (schedd)")

        # Get job classAds from the condor scheduler
        try:
            job_ads = self.condor_schedd_as_xml.service.getJobAds(None, None)
        except URLError, e:
            log.error("There was a problem connecting to the "
                      "Condor scheduler web service (%s) for the following "
                      "reason: %s"
                      % (config.condor_webservice_url, e.reason))
            return None
        except:
            log.error("There was a problem connecting to the "
                      "Condor scheduler web service (%s). Unknown reason."
                      % (config.condor_webservice_url))
            return None

        # Create the condor_jobs list to store jobs
        log.debug("Parsing Condor job data from schedd")
        condor_jobs = self._condor_job_xml_to_job_list(job_ads)
        del job_ads
        # When querying finishes successfully, reset last query timestamp
        self.last_query = datetime.datetime.now()
        log.debug("Done parsing jobs from Condor Schedd SOAP (%d job(s) parsed)" % len(condor_jobs))

        # Return condor_jobs list
        return condor_jobs

    @staticmethod
    def _condor_q_to_job_list(condor_q_output):
        """
        _condor_q_to_job_list - Converts the output of condor_q
                to a list of Job Objects

                returns [] if there are no jobs
        """

        def _attribute_from_requirements(requirements, attribute):
            regex = "%s\s=\?=\s\"(?P<value>.+?)\"" % attribute
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

            # VMAMI requires special fiddling
            _attribute_from_list(classad, "VMAMI")
            _attribute_from_list(classad, "VMInstanceType")

            jobs.append(Job(**classad))
        return jobs

    @staticmethod
    def _condor_job_xml_to_job_list(condor_xml):
        """
        _condor_job_xml_to_job_list - Converts Condor SOAP XML from Condor
                to a list of Job Objects

                returns [] if there are no jobs
        """
        def _job_attribute(xml, element):
            try:
                return xml.xpath(".//item[name='%s']/value" % element)[0].text
            except:
                return ""

        def _add_if_exists(xml, dictionary, attribute):
            job_value = string.strip(_job_attribute(xml, attribute))
            if job_value:
                dictionary[attribute] = job_value

        def _add_dict_if_exists(xml, dictionary, attribute):
            attr_list = _job_attribute(xml_job, attribute)
            if attr_list:
                try:
                    attr_dict = _attr_list_to_dict(attr_list)
                    dictionary[attribute] = attr_dict
                except:
                    log.exception("Problem extracting %s attribute '%s'" % (attribute, attr_list))

        def _attribute_from_requirements(requirements, attribute):
            regex = "%s\s=\?=\s\"(?P<value>.+?)\"" % attribute
            match = re.search(regex, requirements)
            if match:
                return match.group("value")
            else:
                return ""

        jobs = []

        context = etree.iterparse(StringIO(condor_xml))
        for action, elem in context:
            if elem.tag == "item" and elem.getparent().tag == "classAdArray":
                xml_job = elem
                job_dictionary = {}
                # Mandatory parameters
                job_dictionary['GlobalJobId'] = _job_attribute(xml_job, "GlobalJobId")
                job_dictionary['Owner'] = _job_attribute(xml_job, "Owner")
                job_dictionary['JobPrio'] = _job_attribute(xml_job, "JobPrio")
                job_dictionary['JobStatus'] = _job_attribute(xml_job, "JobStatus")
                job_dictionary['ClusterId'] = _job_attribute(xml_job, "ClusterId")
                job_dictionary['ProcId'] = _job_attribute(xml_job, "ProcId")

                # Optional parameters
                _add_if_exists(xml_job, job_dictionary, "VMNetwork")
                _add_if_exists(xml_job, job_dictionary, "VMCPUArch")
                _add_if_exists(xml_job, job_dictionary, "VMName")
                _add_if_exists(xml_job, job_dictionary, "VMLoc")
                _add_if_exists(xml_job, job_dictionary, "VMMem")
                _add_if_exists(xml_job, job_dictionary, "VMCPUCores")
                _add_if_exists(xml_job, job_dictionary, "VMStorage")
                _add_if_exists(xml_job, job_dictionary, "VMKeepAlive")
                _add_if_exists(xml_job, job_dictionary, "VMMaximumPrice")
                _add_if_exists(xml_job, job_dictionary, "CSMyProxyCredsName")
                _add_if_exists(xml_job, job_dictionary, "CSMyProxyServer")
                _add_if_exists(xml_job, job_dictionary, "CSMyProxyServerPort")
                _add_if_exists(xml_job, job_dictionary, "x509userproxysubject")
                _add_if_exists(xml_job, job_dictionary, "x509userproxy")
                _add_if_exists(xml_job, job_dictionary, "VMHighPriority")
                _add_if_exists(xml_job, job_dictionary, "VMJobPerCore")
                _add_if_exists(xml_job, job_dictionary, "RemoteHost")
                _add_if_exists(xml_job, job_dictionary, "TargetClouds")

                # Requirements requires special fiddling
                requirements = _job_attribute(xml_job, "Requirements")
                if requirements:
                    vmtype = _attribute_from_requirements(requirements, "VMType")
                    if vmtype:
                        job_dictionary['VMType'] = vmtype

                # VMAMI requires special fiddling
                _add_dict_if_exists(xml_job, job_dictionary, "VMAMI")
                _add_dict_if_exists(xml_job, job_dictionary, "VMInstanceType")

                jobs.append(Job(**job_dictionary))

                elem.clear()
        return jobs


 
    # Updates the system jobs:
    #   - Removes finished or deleted jobs from the system
    #   - Ignores jobs already in the system and still in Condor
    #   - Adds all new jobs to the system
    # Parameters:
    #   jobs - (list of Job objects) The jobs received from a condor query
    def update_jobs(self, query_jobs):
        # If no jobs recvd, remove all jobs from the system (all have finished or have been removed)
        if (query_jobs == []):
            log.debug("No jobs received from job query. Removing all jobs from the system.")
            self.job_container.clear()
            return

        # Filter out any jobs in an error status (from the given job list)
        for job in query_jobs:
            if job.job_status >= self.ERROR or job.job_status == self.REMOVED or job.job_status == self.COMPLETE:
                query_jobs.remove(job)

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
        self.job_container.remove_all_not_in(query_jobs)

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
            if job.high_priority == 0:
                self.add_new_job(job)
            else:
                self.add_high_job(job)
            log.verbose("Job %s added to unscheduled jobs list" % job.id)
        del query_jobs


        # Update job status of all the non-new jobs
        log.debug("Updating job status of %d jobs" % (len(jobs_to_update)))
        for job in jobs_to_update:
            self.update_job_status(job)
        del jobs_to_update

        # DBG: print both jobs dicts before updating system.
        #log.verbose("System jobs after system update:")
        #log.verbose("Unscheduled Jobs (new_jobs):")
        #self.log_unsched_jobs()
        #log.verbose("Scheduled Jobs (sched_jobs):")
        #self.log_sched_jobs()
        #log.verbose("High Priority Jobs (high_jobs):")
        #self.log_high_jobs()

    # Add New Job
    # Add a new job to the system (in the new_jobs set)
    # Added in order (of priority)
    def add_new_job(self, job):
        self.job_container.add_job(job)

    # Add a job to the scheduled jobs set in the system
    def add_sched_job(self, job):
        self.job_container.add_job(job)

    # Add High(Priority) Job
    # Add a new job to the system (in the high_jobs set)
    def add_high_job(self, job):
        self.job_container.add_job(job)


    # Remove System Job
    # Attempts to remove a given job from the JobPool unscheduled
    # or scheduled job dictionaries.
    # Parameters:
    #   job - (Job) the job to be removed from the system
    # No return (if job does not exist in system, error message logged)
    def remove_system_job(self, job):
        self.job_container.remove_job(job)


    # Update Job Status
    # Updates the status of a job
    # Parameters:
    #   job - the job to update
    # Returns
    #   True - updated
    #   False - failed
    def update_job_status(self, target_job):
        return self.job_container.update_job_status(target_job.id, int(target_job.job_status), target_job.remote_host)

    # Mark job scheduled
    # Makes all changes to a job to indicate that the job has been scheduled
    # Currently: moves passed job from unscheduled to scheduled job list,
    # and changes job status to "Scheduled"
    # Parameters:
    #   job   - (Job object) The job to mark as scheduled
    def schedule(self, job):
        self.job_container.schedule_job(job.id)

    def unschedule(self, job):
        self.job_container.unschedule_job(job.id)

    # Get required VM types
    # Returns a list (of strings) containing the unique required VM types
    # gathered from all jobs in the job pool (scheduled and unscheduled)
    # Returns:
    #   required_vmtypes - (list of strings) A list of required VM types
    def get_required_vmtypes(self):
        required_vmtypes = []
        for job in self.job_container.get_all_jobs():
            if job.req_vmtype not in required_vmtypes and job.job_status != self.HELD \
            and job.job_status != self.COMPLETE:
                required_vmtypes.append(job.req_vmtype)

        log.debug("get_required_vmtypes - Required VM types: " + ", ".join(required_vmtypes))
        return required_vmtypes

    # Get required VM types
    # Returns a dictionary containing the unique required VM types as a key
    # gathered from all jobs in the job pool (scheduled and unscheduled), and
    # count of the number of jobs needing that type as the value.
    # Returns:
    #   required_vmtypes - (dictionary, string key, int value) A dict of required VM types
    def get_required_vmtypes_dict(self):
        required_vmtypes = {}
        for job in self.job_container.get_all_jobs():
            if job.req_vmtype not in required_vmtypes and job.job_status != self.HELD \
               and job.job_status != self.COMPLETE:
                required_vmtypes[job.req_vmtype] = 1
            elif job.job_status != self.HELD and job.job_status != self.COMPLETE:
                required_vmtypes[job.req_vmtype] += 1
        log.debug("get_required_vm_types_dict - Required VM Type : Count " + str(required_vmtypes))
        return required_vmtypes


    # Get desired vmtype distribution
    # Based on top jobs in user new_job queue determine
    # a 'fair' distribution of vmtypes
    def job_type_distribution_normal(self):
        type_desired = {}
        new_jobs_by_users = self.job_container.get_unscheduled_jobs_by_users(prioritized = True)
        high_priority_jobs_by_users = self.job_container.get_high_priority_jobs_by_users(prioritized = True)
        held_user_adjust = 0
        for user in new_jobs_by_users.keys():
            vmtype = None
            for job in new_jobs_by_users[user]:
                if job.job_status != self.HELD and job.job_status != self.COMPLETE:
                    vmtype = job.req_vmtype
                    break
            if vmtype == None:
                held_user_adjust -= 1 #This user is completely held
                break
            if vmtype in type_desired.keys():
                type_desired[vmtype] += 1 * (1 / Decimal(config.high_priority_job_weight) if high_priority_jobs_by_users else 1)
            else:
                type_desired[vmtype] = 1 * (1 / Decimal(config.high_priority_job_weight) if high_priority_jobs_by_users else 1)
        for user in high_priority_jobs_by_users.keys():
            vmtype = None
            for job in high_priority_jobs_by_users[user]:
                if job.job_status != self.HELD and job.job_status != self.COMPLETE:
                    vmtype = job.req_vmtype
                    break
            if vmtype == None:
                held_user_adjust -= 1 # this user is completely held
                break
            if vmtype in type_desired.keys():
                type_desired[vmtype] += 1 * config.high_priority_job_weight
            else:
                type_desired[vmtype] = 1 * config.high_priority_job_weight
        num_users = Decimal(held_user_adjust + len(new_jobs_by_users.keys()) + len(high_priority_jobs_by_users.keys()))
        for vmtype in type_desired.keys():
            type_desired[vmtype] = type_desired[vmtype] / num_users
        return type_desired

    def job_type_distribution_multi_vmtype(self):
        type_desired = {}
        new_jobs_by_users = self.job_container.get_unscheduled_jobs_by_users(prioritized = True)
        high_priority_jobs_by_users = self.job_container.get_high_priority_jobs_by_users(prioritized = True)
        held_user_adjust = 0
        user_types = {}
        high_user_types = {}
        # Want to check all jobs of the highest priority in their list
        for user in new_jobs_by_users.keys():
            vmtypes = set()
            highest_priority = new_jobs_by_users[user][0].priority
            for job in new_jobs_by_users[user]:
                if job.job_status <= self.RUNNING and job.priority == highest_priority:
                    vmtypes.add(job.req_vmtype)
            if len(vmtypes) == 0: # user is held / complete
                held_user_adjust -= 1
            else:
                user_types[user] = vmtypes
        for user in high_priority_jobs_by_users.keys():
            vmtypes = set()
            highest_priority = high_priority_jobs_by_users[user][0].priority
            for job in high_priority_jobs_by_users[user]:
                if job.job_status <= self.RUNNING and job.priority == highest_priority:
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
        for vmtype in type_desired.keys():
            type_desired[vmtype] *= num_users
        return type_desired

    def get_jobs_of_type_for_user(self, type, user):
        """
        get_jobs_of_type_for_user -- get a list of jobs of a VMtype for a user

        returns a list of Job objects.
        """
        jobs = self.job_container.get_jobs_for_user(user)
        return jobs




    # Attempts to place a list of jobs into a Hold Status to prevent running
    # If a job fails to be held it is placed in a list and failed jobs are returned
    def hold_jobSOAP(self, jobs):
        log.debug("Holding Jobs via Condor SOAP API")
        failed = []
        for job in jobs:
            try:
                job_ret = self.condor_schedd.service.holdJob(None, job.cluster_id, job.proc_id, None, False, False, True)
                if job_ret.code != "SUCCESS":
                    failed.append(job)
            except URLError, e:
                log.error("There was a problem connecting to the "
                      "Condor scheduler web service (%s) for the following "
                      "reason: %s"
                      % (config.condor_webservice_url, e.reason))
                return None
            except:
                log.error("There was a problem connecting to the "
                      "Condor scheduler web service (%s). Unknown reason."
                      % (config.condor_webservice_url))
                return None
        return failed

    # Attempts to release a list of jobs that have been previously held
    # If a job fails to be released it is placed in a list and returned
    def release_jobSOAP(self, jobs):
        log.debug("Releasing Jobs via Condor SOAP API")
        failed = []
        for job in jobs:
            try:
                job_ret = self.condor_schedd.service.releaseJob(None, job.cluster_id, job.proc_id, None, False, False)
                if job_ret.code != "SUCCESS":
                    failed.append(job)
            except URLError, e:
                log.error("There was a problem connecting to the "
                      "Condor scheduler web service (%s) for the following "
                      "reason: %s"
                      % (config.condor_webservice_url, e.reason))
                return None
            except:
                log.error("There was a problem connecting to the "
                      "Condor scheduler web service (%s). Unknown reason."
                      % (config.condor_webservice_url))
                return None
        return failed

    def hold_user(self, user):
        jobs = []
        for job in self.job_container.get_jobs_for_user(user):
            if job.job_status != self.RUNNING:
                jobs.append(job)
        return self.hold_jobSOAP(jobs)


    def release_user(self, user):
        jobs = []
        for job in self.job_container.get_jobs_for_user(user):
            if job.job_status == self.HELD:
                jobs.append(job)
        return self.release_jobSOAP(jobs)

    def hold_vmtype(self, vmtype):
        jobs = []
        for job in self.job_container.get_all_jobs():
            if job.req_vmtype == vmtype and job.job_status != self.RUNNING:
                jobs.append(job)
        ret = self.hold_jobSOAP(jobs)
        return ret

    def release_vmtype(self, vmtype):
        jobs = []
        for job in self.job_container.get_all_jobs():
            if job.req_vmtype == vmtype and job.job_status == self.HELD:
                jobs.append(job)
        ret = self.release_jobSOAP(jobs)
        return ret

    ##
    ## JobPool Private methods (Support methods)
    ##

    # Parse classAd Requirements string.
    # Takes the Requirements string from a condor job classad and retrieves the
    # VMType string. Returns null object if no VMtype is specified.
    # NOTE: Could be expanded to return a name=>value dictionary of all Requirements
    #       fields. (Not currently necessary).
    # Parameters:
    #   requirements - (str) The Requirements string from a condor job classAd, or a
    #                  VMType string, or None (the null object)
    # Return:
    #   The VMType string or None (null object)
    def parse_classAd_requirements(self, requirements):

        # Match against the Requirements string
        req_re = "(VMType\s=\?=\s\"(?P<vm_type>.+?)\")"
        match = re.search(req_re, requirements)
        if match:
            log.debug("parse_classAd_requirements - VMType parsed from "
              + "Requirements string: %s" % match.group('vm_type'))
            return match.group('vm_type')
        else:
            log.debug("parse_classAd_requirements - No VMType specified. Returning None.")
            return None

    ## Log methods

    # Log Job Lists (short)
    def log_jobs(self):
        self.log_sched_jobs()
        self.log_unsched_jobs()

    # log scheduled jobs (short)
    def log_sched_jobs(self):
        for job in self.job_container.get_scheduled_jobs():
            job.log_dbg()

    # log unscheduled Jobs (short)
    def log_unsched_jobs(self):
        for job in self.job_container.get_unscheduled_jobs():
            job.log_dbg()


    # log high priority Jobs (short)
    def log_high_jobs(self):
        for job in self.job_container.get_high_priority_jobs():
            job.log_dbg()

    def log_jobs_list(self, jobs):
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




# A class to contain a subset of all jobs obtained from the scheduler, to be
# considered by a scheduler to determine possible schedules (resource environments)
# NOTE: All jobs taken into the job set will be left in the passed in JobPool
#       until explicitly removed via a method call on the jobset indicating that
#       the jobs have been scheduled (at which point, the JobPool will move the
#       jobs into the 'Scheduled' list
class JobSet:

    ## Instance Variables

    # The job_set list is the list of jobs considered in set evaluations
    job_set = []

    ## Instance Methods

    # Constructor
    # Parameters:
    #   name     - (str) The name of the job set
    #   pool     - (JobPool) The JobPool object from which a subset of jobs are
    #              taken to create the job set
    #   size     - (int) The number of jobs to take in to the job set (if the
    #              pool is too small, as many jobs as possible will be taken)
    # Variables:
    #   set_time - (datetime) The time at which the job set was created
    def __init__(self, name, pool):
        global log
        log = logging.getLogger("cloudscheduler")
        log.info("New JobSet %s created" % name)
        self.name = name
        set_time = datetime.datetime.now()
        # Take a slice (subset) of the passed in JobPool
        # TODO: Call a JobPool method (random subset, highpriority, etc.)
        #       Choose (parameter?): priority or arbitrary?


    # Drop a job from the job set (leave in the JobPool)
    # Parameters:
    #   job   - (Job) The job to drop from the job set
    # Returns:
    #   0     - Job dropped successfully
    #   1     - Job doesn't exist in job set
    #   2     - Job failed to drop
    def drop_job(self, job):
        if not (job in self.job_set):
            log.error("(drop_job) passed job not in job set...")
            return (1)
        self.job_set.remove(job)
        return (0)

    # log a short form list of the job set
    def log(self):
        if len(self.job_set) == 0:
            log.debug("Job set %s is empty..." % self.name)
            return
        else:
            log.debug("Job set %s:" % self.name)
            for job in self.job_set:
                job.log_dbg()


