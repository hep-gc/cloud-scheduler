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
             JobStatus=0, ClusterId=0, ProcId=0, VMType="default",
             VMNetwork="", VMCPUArch="x86", VMName="Default-Image",
             VMLoc="", VMAMI="", VMMem=512, VMCPUCores=1, VMStorage=1, 
             VMKeepAlive=0, VMHighPriority=0,
             VMInstanceType="", VMMaximumPrice=0, VMSlotForEachCore=False):
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
     VMSlotForEachCore - (boolean) Whether or not the machines you request will have
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
        self.slot_for_each_core = VMSlotForEachCore in ['true', "True", True]

        # Set the new job's status
        self.status = self.statuses[0]

        global log
        log = logging.getLogger("cloudscheduler")

        log.verbose("New Job ID: %s, User: %s, Priority: %d, VM Type: %s, Network: %s, Image: %s, Image Location: %s, AMI: %s, Memory: %d" \
          % (self.id, self.user, self.priority, self.req_vmtype, self.req_network, self.req_image, self.req_imageloc, self.req_ami, self.req_memory))


    # Log a short string representing the job
    def log(self):
        log.info("Job ID: %s, User: %s, Priority: %d, VM Type: %s, Image location: %s, CPU: %s, Memory: %d" \
          % (self.id, self.user, self.priority, self.req_vmtype, self.req_imageloc, self.req_cpuarch, self.req_memory))
    def log_dbg(self):
        log.debug("Job ID: %s, User: %s, Priority: %d, VM Type: %s, Image location: %s, CPU: %s, Memory: %d" \
          % (self.id, self.user, self.priority, self.req_vmtype, self.req_imageloc, self.req_cpuarch, self.req_memory))
    def get_job_info(self):
        CONDOR_STATUS = ("New", "Idle", "Running", "Removed", "Complete", "Held", "Error")
        return "%-15s %-10s %-10s %-15s %-25s\n" % (self.id[-15:], self.user[-10:], self.req_vmtype[-10:], CONDOR_STATUS[self.job_status], self.status[-25:])
    @staticmethod
    def get_job_info_header(self):
        return "%-15s %-10s %-10s %-15s %-25s\n" % ("Global ID", "User", "VM Type", "Job Status", "Status")
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



# A pool of all jobs read from the job scheduler. Stores all jobs until they
# complete. Keeps scheduled and unscheduled jobs.
class JobPool:

    ## Instance Variables:

    # Create the new_jobs (unscheduled jobs) and sched_jobs (scheduled jobs) dictionaries.
    # Dictionaries contain (username, [list of jobs]) entries.
    new_jobs = {}
    sched_jobs = {}
    high_jobs = {}

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
        except:
            log.exception("Problem running %s, unexpected error" % string.join(condor_q, " "))
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
        log.debug("Done parsing jobs from Condor Schedd SOAP")

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
                _add_if_exists(xml_job, job_dictionary, "VMAMI")
                _add_if_exists(xml_job, job_dictionary, "VMMem")
                _add_if_exists(xml_job, job_dictionary, "VMCPUCores")
                _add_if_exists(xml_job, job_dictionary, "VMStorage")
                _add_if_exists(xml_job, job_dictionary, "VMKeepAlive")
                _add_if_exists(xml_job, job_dictionary, "VMInstanceType")
                _add_if_exists(xml_job, job_dictionary, "VMMaximumPrice")
                _add_if_exists(xml_job, job_dictionary, "VMHighPriority")

                # Requirements requires special fiddling
                requirements = _job_attribute(xml_job, "Requirements")
                if requirements:
                    vmtype = _attribute_from_requirements(requirements, "VMType")
                    if vmtype:
                        job_dictionary['VMType'] = vmtype

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
            for jobset in (self.new_jobs.values() + self.sched_jobs.values() + self.high_jobs.values()):
                for job in jobset:
                    self.remove_system_job(job)
                    log.info("Job %s finished or removed. Cleared job from system." % job.id)
            return

        # Filter out any jobs in an error status
        for job in reversed(query_jobs):
            if job.job_status >= self.ERROR:
                self.remove_job(query_jobs, job)

        # Update all system jobs:
        #   - remove jobs already in the system from the jobs list
        #   - remove finished jobs (job in system, not in jobs list)

        # DBG: print both jobs dicts before updating system.
        log.verbose("System jobs prior to system update:")
        log.verbose("Unscheduled Jobs (new_jobs):")
        self.log_jobs_dict(self.new_jobs)
        log.verbose("Scheduled Jobs (sched_jobs):")
        self.log_jobs_dict(self.sched_jobs)
        log.verbose("High Priority Jobs (high_jobs):")
        self.log_jobs_dict(self.high_jobs)

        jobs_to_remove = []
        for jobset in (self.new_jobs.values() + self.sched_jobs.values() + self.high_jobs.values()):
            for sys_job in reversed(jobset):

                # DBG: print job details in loop
                log.verbose("system job loop - %s, %10s, %4d, %10s" % (sys_job.id, sys_job.user, sys_job.priority, sys_job.req_vmtype))

                # If the sys job is not in the query jobs, sys job has finished / been removed
                if not (self.has_job(query_jobs, sys_job)):
                    self.remove_system_job(sys_job)
                    log.info("Job %s finished or removed. Cleared job from system." % sys_job.id)

                # Otherwise, the system job is in the condor queue - remove it from condor_jobs
                # and append a job to update to list
                else:
                    jobs_to_remove.append(sys_job.id)

        jobs_to_update = self._remove_jobs_by_id(query_jobs, jobs_to_remove, self.write_lock)
        # Add all jobs remaining in jobs list to the Unscheduled job set (new_jobs)
        for job in query_jobs:
            if job.high_priority == 0:
                self.add_new_job(job)
            else:
                self.add_high_job(job)
            log.verbose("Job %s added to unscheduled jobs list" % job.id)
        del query_jobs
        # Update job status of all the non-new jobs
        log.debug("Updating Job Status")
        for job in jobs_to_update:
            self.update_job_status(job)
        del jobs_to_update

        # DBG: print both jobs dicts before updating system.
        log.verbose("System jobs after system update:")
        log.verbose("Unscheduled Jobs (new_jobs):")
        self.log_jobs_dict(self.new_jobs)
        log.verbose("Scheduled Jobs (sched_jobs):")
        self.log_jobs_dict(self.sched_jobs)
        log.verbose("High Priority Jobs (high_jobs):")
        self.log_jobs_dict(self.high_jobs)


    # Add New Job
    # Add a new job to the system (in the new_jobs set)
    # Added in order (of priority)
    def add_new_job(self, job):
        if job.user in self.new_jobs:
            self.insort_job(self.new_jobs[job.user], job)
        else:
            self.new_jobs[job.user] = [job]


    # Add a job to the scheduled jobs set in the system
    def add_sched_job(self, job):
        if job.user in self.sched_jobs:
            self.sched_jobs[job.user].append(job)
        else:
            self.sched_jobs[job.user] = [job]

    # Add High(Priority) Job
    # Add a new job to the system (in the high_jobs set)
    def add_high_job(self, job):
        if job.user in self.high_jobs:
            self.insort_job(self.high_jobs[job.user], job)
        else:
            self.high_jobs[job.user] = [job]

    # Adds a job to a given list of job objects
    # in order of priority. The list runs front to back, high to low
    # priority.
    # Note: job_list MUST be a list of Job objects
    def insort_job(self, job_list, job):
        ## Heuristics:
        # Check if list is empty
        if (job_list == []):
            job_list.append(job)
            return
        # Check if job has highest priority in list
        elif (job_list[0].priority < job.priority):
            with self.write_lock:
                job_list.insert(0, job)
            return
        # Check back of the list - equal priorites, append
        elif (job_list[-1].priority >= job.priority):
            job_list.append(job)
            return
        ## Otherwise, do a linear insert
        # Move from back to front, as new entry is inserted AFTER entries of same priority
        else:
            i = len(job_list)
            while (i != 0):
                i = i-1
                if (job_list[i].priority >= job.priority):
                    with self.write_lock:
                        job_list.insert(i+1, job)
                    break
            return


    # Remove System Job
    # Attempts to remove a given job from the JobPool unscheduled
    # or scheduled job dictionaries.
    # Parameters:
    #   job - (Job) the job to be removed from the system
    # No return (if job does not exist in system, error message logged)
    def remove_system_job(self, job):

        # Check for job in unscheduled job set
        # if (job.user in self.new_jobs) and (job in self.new_jobs[job.user]):
        if (job.user in self.new_jobs) and (self.has_job(self.new_jobs[job.user], job)):
            with self.write_lock:
                self.remove_job(self.new_jobs[job.user], job)
                log.verbose("remove_system_job - Removing job %s from unscheduled jobs."
                          % job.id)

                # If user's job list is empty, remove entry from the new_jobs dict
                if (self.new_jobs[job.user] == []):
                    del self.new_jobs[job.user]
                    log.debug("User %s has no more jobs in the Unscheduled Jobs set. Removing user from queue."
                              % job.user)
        # Check for job in scheduled job set
        # elif (job.user in self.sched_jobs) and (job in self.sched_jobs[job.user]):
        elif (job.user in self.sched_jobs) and (self.has_job(self.sched_jobs[job.user], job)):
            with self.write_lock:
                self.remove_job(self.sched_jobs[job.user], job)
                log.verbose("remove_system_job - Removing job %s from scheduled jobs."
                          % job.id)

                # If user's job list is empty, remove entry from sched_jobs
                if (self.sched_jobs[job.user] == []):
                    del self.sched_jobs[job.user]
                    log.debug("User %s has no more jobs in the Scheduled Jobs set. Removing user from queue."
                              % job.user)
        elif (job.user in self.high_jobs) and (self.has_job(self.high_jobs[job.user], job)):
            with self.write_lock:
                self.remove_job(self.high_jobs[job.user], job)
                log.verbose("remove_system_job - Removing job %s from high_jobs."
                            % job.id)
                if (self.high_jobs[job.user] == []):
                    del self.high_jobs[job.user]
                    log.debug("User %s has no more jobs in the High Priority Jobs set. Removing user from queue."
                              % job.user)
        else:
            log.warning("remove_system_job - Job does not exist in system."
                      + " Doing nothing.")


    # Remove job by id
    # Attempts to remove a job with a given job id from a given
    # list of Jobs.
    # Note: The job_list MUST be a list of Job objects
    # Parameters:
    #   job_list - (list of Job) the list from which to remove jobs
    #   target_job   - (Job object) the job to be removed
    # Returns:
    #   removed_list - (list of Job) The removed Jobs
    def remove_job(self, job_list, target_job):
        log.verbose("(remove_job) - Target job: %s" % target_job.id)
        removed_list = []
        target_job_id = target_job.id
        removed = False
        i = len(job_list)
        while (i != 0):
            i = i-1
            if (target_job_id == job_list[i].id):
                log.verbose("(remove_job) - Matching job found: %s" % job_list[i].id)
                removed_list.append(job_list[i])
                job_list.remove(job_list[i])
                removed = True
        removed_list = filter(lambda job: job.id == target_job_id, job_list)
        if removed_list == []:
            log.verbose("(remove_job) - Job %s does not exist in given list. Doing nothing." % target_job.id )
        return removed_list

    def _remove_jobs_by_id(self, job_list, job_ids_to_remove, write_lock=None):
        """
        _remove_jobs_by_id - remove jobs from a list, with optional write lock

        params:
        job_list - list of job objects that you want manipulated
        job_ids_to_remove - list of job ids that you want removed
        write_lock - optional write lock for thread safety
        """
        job_ids = set(job_ids_to_remove)
        removed_list = filter(lambda job: job.id in job_ids, job_list)
        if write_lock:
            with write_lock:
                for job in removed_list:
                    job_list.remove(job)
        else:
            for job in removed_list:
                job_list.remove(job)
        return removed_list

    # Update Job Status
    # Updates the status of a job
    # Parameters:
    #   job - the job to update
    # Returns
    #   True - updated
    #   False - failed
    def update_job_status(self, target_job):
        ret = False
        if (target_job.user in self.new_jobs) and (self.has_job(self.new_jobs[target_job.user], target_job)):
            with self.write_lock:
                for job in self.new_jobs[target_job.user]:
                    if target_job.id == job.id:
                        job.job_status = int(target_job.job_status)
                        ret = True
                        break
        elif (target_job.user in self.sched_jobs) and (self.has_job(self.sched_jobs[target_job.user], target_job)):
            with self.write_lock:
                for job in self.sched_jobs[target_job.user]:
                    if target_job.id == job.id:
                        job.job_status = int(target_job.job_status)
                        ret = True
                        break
        elif (target_job.user in self.high_jobs) and (self.has_job(self.high_jobs[target_job.user], target_job)):
            with self.write_lock:
                for job in self.high_jobs[target_job.user]:
                    if target_job.id == job.id:
                        job.job_status = int(target_job.job_status)
                        ret = True
                        break
        else:
            log.warning("update_job_status - Job does not exist in system."
                      + " Doing nothing.")
        return ret

    # Checks to see if the given job ID is in the given job list
    # Note: The job_list MUST be a list of Job objects.
    # Parameters:
    #   job_list - (list of Jobs) The list of jobs in which to check for the given ID
    #   target_job   - (Job Object) The job to check for
    # Returns:
    #   True   - The job exists in the checked lists
    #   False  - The job does not exist in the checked lists
    def has_job(self, job_list, target_job):
        target_job_id = target_job.id
        for job in job_list:
            if (target_job_id == job.id):
                return True
        return False


    # Mark job scheduled
    # Makes all changes to a job to indicate that the job has been scheduled
    # Currently: moves passed job from unscheduled to scheduled job list,
    # and changes job status to "Scheduled"
    # Parameters:
    #   job   - (Job object) The job to mark as scheduled
    def schedule(self, job):
        if not ( ((job.user in self.new_jobs) and (job in self.new_jobs[job.user])) 
                 or ((job.user in self.high_jobs) and (job in self.high_jobs[job.user])) ):
            log.error("(schedule) - Error: job %s not in the system's Unscheduled jobs" % job.id)
            log.error("(schedule) - Cannot mark job as scheduled")
            return

        self.remove_system_job(job)
        job.set_status("Scheduled")
        self.add_sched_job(job)
        log.debug("(schedule) - Job %s marked as scheduled." % job.id)

    def unschedule(self, job):
        if not ( (job.user in self.sched_jobs) and (job in self.sched_jobs[job.user]) ):
            log.error("(unschedule) - Error: job %s not in the system's Scheduled jobs" % job.id)
            log.error("(unschedule) - Cannot mark job as Unscheduled")
            return

        self.remove_system_job(job)
        job.set_status("Unscheduled")
        if job.high_priority == 0:
            self.add_new_job(job)
        else:
            self.add_high_job(job)
        log.debug("(unschedule) Job %s marked as Unscheduled." % job.id)

    # Get required VM types
    # Returns a list (of strings) containing the unique required VM types
    # gathered from all jobs in the job pool (scheduled and unscheduled)
    # Returns:
    #   required_vmtypes - (list of strings) A list of required VM types
    def get_required_vmtypes(self):
        required_vmtypes = []
        for jobset in (self.new_jobs.values() + self.sched_jobs.values() +
                       self.high_jobs.values()):
            for job in jobset:
                if job.req_vmtype not in required_vmtypes:
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
        for jobset in (self.new_jobs.values() + self.sched_jobs.values()
                       + self.high_jobs.values()):
            for job in jobset:
                if job.req_vmtype not in required_vmtypes:
                    required_vmtypes[job.req_vmtype] = 1
                else:
                    required_vmtypes[job.req_vmtype] = required_vmtypes[job.req_vmtype] + 1
        log.debug("get_required_vm_types_dict - Required VM Type : Count " + str(required_vmtypes))
        return required_vmtypes

    # Get desired vmtype distribution
    # Based on top jobs in user new_job queue determine
    # a 'fair' distribution of vmtypes
    def job_type_distribution(self):
        type_desired = {}
        for user in self.new_jobs.keys():
            vmtype = self.new_jobs[user][0].req_vmtype
            if vmtype in type_desired.keys():
                type_desired[vmtype] += 1 * (1 / Decimal(config.high_priority_job_weight))
            else:
                type_desired[vmtype] = 1 * (1 / Decimal(config.high_priority_job_weight))
        for user in self.high_jobs.keys():
            vmtype = self.high_jobs[user][0].req_vmtype
            if vmtype in type_desired.keys():
                type_desired[vmtype] += 1 * config.high_priority_job_weight
            else:
                type_desired[vmtype] = 1 * config.high_priority_job_weight
        num_users = Decimal(len(self.new_jobs.keys()) + len(self.high_jobs.keys()))
        for vmtype in type_desired.keys():
            type_desired[vmtype] = type_desired[vmtype] / num_users
        return type_desired

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
        if self.sched_jobs.has_key(user):
            for job in self.sched_jobs[user]:
                if job.job_status != self.RUNNING:
                    jobs.append(job)
        if self.new_jobs.has_key(user):
            for job in self.new_jobs[user]:
                if job.job_status != self.RUNNING:
                    jobs.append(job)
        return self.hold_jobSOAP(jobs)


    def release_user(self, user):
        jobs = []
        if self.sched_jobs.has_key(user):
            for job in self.sched_jobs[user]:
                if job.job_status == self.HELD:
                    jobs.append(job)
        if self.new_jobs.has_key(user):
            for job in self.new_jobs[user]:
                if job.job_status == self.HELD:
                    jobs.append(job)
        return self.release_jobSOAP(jobs)

    def hold_vmtype(self, vmtype):
        jobs = []
        for user in self.new_jobs.keys():
            for job in self.new_jobs[user]:
                if job.req_vmtype == vmtype and job.job_status != self.RUNNING:
                    jobs.append(job)
        for user in self.sched_jobs.keys():
            for job in self.sched_jobs[user]:
                if job.req_vmtype == vmtype and job.job_status != self.RUNNING:
                    jobs.append(job)
        ret = self.hold_jobSOAP(jobs)
        return ret

    def release_vmtype(self, vmtype):
        jobs = []
        for user in self.new_jobs.keys():
            for job in self.new_jobs[user]:
                if job.req_vmtype == vmtype and job.job_status == self.HELD:
                    jobs.append(job)
        for user in self.sched_jobs.keys():
            for job in self.sched_jobs[user]:
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
        if len(self.sched_jobs) == 0:
            log.debug("Scheduled job list in %s is empty" % self.name)
            return
        else:
            log.debug("Scheduled jobs in %s:" % self.name)
            for user in self.sched_jobs.keys():
                for job in self.sched_jobs[user]:
                    job.log_dbg()

    # log unscheduled Jobs (short)
    def log_unsched_jobs(self):
        if len(self.new_jobs) == 0:
            log.debug("Unscheduled job list in %s is empty" % self.name)
            return
        else:
            log.debug("Unscheduled jobs in %s:" % self.name)
            for user in self.new_jobs.keys():
                for job in self.new_jobs[user]:
                    job.log_dbg()

    # log high priority Jobs (short)
    def log_high_jobs(self):
        if len(self.high_jobs) == 0:
            log.debug("High Priority job list in %s is empty" % self.name)
            return
        else:
            log.debug("High Priority jobs in %s:" % self.name)
            for user in self.high_jobs.keys():
                for job in self.high_jobs[user]:
                    job.log_dbg()

    def log_jobs_list(self, jobs):
        if jobs == []:
            log.verbose("(none)")
        for job in jobs:
            log.verbose("\tJob: %s, %10s, %4d, %10s" % (job.id, job.user, job.priority, job.req_vmtype))

    def log_jobs_dict(self, jobs):
        if jobs == {}:
            log.verbose("(none)")
        for jobset in jobs.values():
            for job in jobset:
                log.verbose("\tJob: %s, %10s, %4d, %10s" % (job.id, job.user, job.priority, job.req_vmtype))



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


