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
import datetime
import subprocess
import string
import re
import logging
import sys
from suds.client import Client
from urllib2 import URLError
from bisect import insort

from cloudscheduler.utilities import determine_path
import cloudscheduler.config as config

##
## LOGGING
##

log = None


##
## CLASSES
##

# The storage structure for individual jobs read from the job scheduler
class Job:

    ## Instance Variables

    # A list of possible statuses for internal job representation
    statuses = ["Unscheduled", "Scheduled"]

    ## Instance Methods

    # Constructor
    # Parameters:
    # GlobalJobID  - (str) The ID of the job (via condor). Functions as name.
    # User       - (str) The user that submitted the job to Condor
    # Priority   - (int) The priority given in the job submission file (default = 1)
    # VMType     - (str) The VMType required by the job (set in VM's condor_config file)
    # VMNetwork  - (str) The network association the job requires. TODO: Should support "any"
    # VMCPUArch  - (str) The CPU architecture the job requires in its run environment
    # VMName     - (str) The name of the image the job is to run on
    # VMLoc      - (str) The location (URL) of the image the job is to run on
    # VMMem      - (int) The amount of memory in MB the job requires
    # VMCPUCores - (int) The number of cpu cores the job requires
    # VMStorage  - (int) The amount of storage space the job requires
    # NOTE: The image field is used as a name field for the image the job will
    #   run on. The cloud scheduler will eventually be able to search a set of
    #   repositories for this image name. Currently, imageloc MUST be set.
    #
    # TODO: Set default job properties in the cloud scheduler main config file
    #      (Have option to set them there, and default values)
    def __init__(self, GlobalJobId="None", Owner="Default-User", JobPrio=1,
             VMType="canfarbase", VMNetwork="private", VMCPUArch="x86", VMName="Default-Image",
             VMLoc="http://vmrepo.phys.uvic.ca/vms/canfarbase_i386.dev.img.gz",
             VMMem=512, VMCPUCores=1, VMStorage=1):
        self.id   = GlobalJobId
        self.user = Owner
        self.priority     = int(JobPrio)
        self.req_vmtype   = VMType
        self.req_network  = VMNetwork
        self.req_cpuarch  = VMCPUArch
        self.req_image    = VMName
        self.req_imageloc = VMLoc
        self.req_memory   = int(VMMem)
        self.req_cpucores = int(VMCPUCores)
        self.req_storage  = int(VMStorage)

        # Set the new job's status
        self.status = self.statuses[0]

        global log
        log = logging.getLogger("cloudscheduler")

        log.debug("New Job object created:")
        log.debug("ID: %s, User: %s, Priority: %d, VM Type: %s, Network: %s, Image: %s, Image Location: %s, Memory: %d" \
          % (self.id, self.user, self.priority, self.req_vmtype, self.req_network, self.req_image, self.req_imageloc, self.req_memory))
    
        
    # Log a short string representing the job
    def log(self):
        log.info("Job ID: %s, User: %s, Priority: %d, VM Type: %s, Image location: %s, CPU: %s, Memory: %d" \
          % (self.id, self.user, self.priority, self.req_vmtype, self.req_imageloc, self.req_cpuarch, self.req_memory))
    def log_dbg(self):
        log.debug("Job ID: %s, User: %s, Priority: %d, VM Type: %s, Image location: %s, CPU: %s, Memory: %d" \
          % (self.id, self.user, self.priority, self.req_vmtype, self.req_imageloc, self.req_cpuarch, self.req_memory))

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

    ## Instance Methods

    # Constructor
    # name       - The name of the job pool being created
    # last_query - A timestamp for the last time the scheduler was queried,
    #              or its creation time
    def __init__(self, name):
        global log
        log = logging.getLogger("cloudscheduler")
        log.info("New JobPool %s created" % name)
        self.name = name
        self.last_query = datetime.datetime.now()

        _schedd_wsdl  = "file://" + determine_path() \
                        + "/wsdl/condorSchedd.wsdl"
        self.condor_schedd = Client(_schedd_wsdl,
                                    location=config.condor_webservice_url)


    def job_querySOAP(self):
        log.debug("Querying job pool with Condor SOAP API")

        # Get job classAds from the condor scheduler
        try:
            job_ads = self.condor_schedd.service.getJobAds(None, None)
        except URLError, e:
            log.error("There was a problem connecting to the "
                      "Condor scheduler web service (%s) for the following "
                      "reason: %s"
                      % (config.condor_webservice_url, e.reason[1]))
            return
        except:
            log.error("There was a problem connecting to the "
                      "Condor scheduler web service (%s) for the following "
                      "reason: "
                      % (config.condor_webservice_url))
            raise
            sys.exit(1)

        # Create the condor_jobs list to store jobs
        condor_jobs = []

        # If the query succeeds and there are jobs present, process them
        if job_ads.status.code == "SUCCESS" and job_ads.classAdArray:

            # Convert ugly data structure from soap into list of dictionaries
            classads = []
            for soap_classad in job_ads.classAdArray.item:
                new_classad = {}
                for attribute in soap_classad.item:
                    attribute_key = str(attribute.name)
                    new_classad[attribute_key] = attribute.value
                classads.append(new_classad)
          
            for classad in classads:
                job_dict = self.make_job_dict(classad)

                # Create Jobs from the classAd data
                # Note: using the '**' operator, which calls a named-parameter function with the values
                # of dictionary keys of the same name (as the function parameters)
                con_job = Job(**job_dict)
                condor_jobs.append(con_job)
            
            # DBG: Print condor jobs recvd from scheduler.
            log.debug("Jobs read from condor scheduler, stored in condor jobs list: ")
            self.log_jobs_list(condor_jobs)

        # Otherwise, if the query succeeds but there are no jobs in the queue
        elif job_ads.status.code == "SUCCESS" and not job_ads.classAdArray:
            log.debug("Job query (status: %s) returned no jobs." % job_ads.status.code)
            
        # Otherwise, log an error and return None
        else:
            log.error("Job query (status: %s) failed." % job_ads.status.code)
            return None

        # When querying finishes successfully, reset last query timestamp
        last_query = datetime.datetime.now()

        # Return condor_jobs list
        return condor_jobs


    # Updates the system jobs:
    #   - Removes finished or deleted jobs from the system
    #   - Ignores jobs already in the system and still in Condor
    #   - Adds all new jobs to the system
    # Parameters:
    #   jobs - (list of Job objects) The jobs received from a condor query
    def update_jobs(self, query_jobs):
        
        # DBG: Print query_jobs immediately
        log.debug("Jobs received from condor query:")
        self.log_jobs_list(query_jobs)
        
        # If no jobs recvd, remove all jobs from the system (all have finished or have been removed)
        if (query_jobs == []):
            log.debug("No jobs received from job query. Removing all jobs from the system.")
            for jobset in (self.new_jobs.values() + self.sched_jobs.values()):
                for job in jobset:
                    self.remove_system_job(job)
                    log.info("Job %s finished or removed. Cleared job from system." % job.id)
            return
        
        # Update all system jobs:
        #   - remove jobs already in the system from the jobs list
        #   - remove finished jobs (job in system, not in jobs list)
        
        # DBG: print both jobs dicts before updating system.
        log.debug("System jobs prior to system update:")
        log.debug("Unscheduled Jobs (new_jobs):")
        self.log_jobs_dict(self.new_jobs)
        log.debug("Scheduled Jobs (sched_jobs):")
        self.log_jobs_dict(self.sched_jobs)
        
        for jobset in (self.new_jobs.values() + self.sched_jobs.values()):
            for sys_job in jobset:
                
                # DBG: print job details in loop
                log.debug("system job loop - %s, %10s, %4d, %10s" % (sys_job.id, sys_job.user, sys_job.priority, sys_job.req_vmtype))
                
                # If system job is in the jobs list, remove from the jobs list
                if (self.has_job(query_jobs, sys_job)):
                    log.debug("Job %s is already in the system." % sys_job.id)
                    #self.remove_job(query_jobs, sys_job)
                    
                    # Manually remove job from query jobs inline. Removal is weird.
                    log.debug("Removal target job: %s" % sys_job.id)
                    for rem_job in query_jobs:
                        if (sys_job.id == rem_job.id):
                            log.debug("Matching job found: %s" % rem_job.id)
                            query_jobs.remove(rem_job)
                            break
                    
                    # DBG: Print query_jobs after modification by update loop
                    log.debug("Query jobs after removal (system already has job):")
                    self.log_jobs_list(query_jobs)
                
                # Otherwise, if system job is not in recvd jobs, remove job from system
                else:
                    log.info("Job %s finished or removed. Clearing job from system." % sys_job.id)
                    self.remove_system_job(sys_job)
        
        # Add all jobs remaining in jobs list to the Unscheduled job set (new_jobs)
        for job in query_jobs:
            self.add_new_job(job)
            log.info("Job %s added to unscheduled jobs list" % job.id)    
                    
        # DBG: print both jobs dicts before updating system.
        log.debug("System jobs after system update:")
        log.debug("Unscheduled Jobs (new_jobs):")
        self.log_jobs_dict(self.new_jobs)
        log.debug("Scheduled Jobs (sched_jobs):")
        self.log_jobs_dict(self.sched_jobs)
        

    # Query Job Scheduler via command line condor tools
    # Gets a list of jobs from the job scheduler, and updates internal scheduled
    # and unscheduled job lists with the scheduler information.
    def job_queryCMD(self):
        log.warning("job_queryCMD is DEPRECATED... using job_querySOAP instead."
	            + " Note that this requires the condor SOAP API (wsdl files).")
        job_querySOAP(self)


    # Add New Job
    # Add a new job to the system, via add_job_ordered
    def add_new_job(self, job):
        self.add_job_ordered(self.new_jobs, job)
        
        
    # Add Job (Ordered)
    # Adds a given job to the given dictionary. Stores job in the list
    # associated with the job's username as a key in the dictionary.
    # Uses ordered insertion on jobs by priority number.
    def add_job_ordered(self, job_dict, job):
        
        # TODO: Will this work? A call by reference / call by value problem.
        # If I add things to job_dict, will they be added to the global dict passed
        # in? We'll see. Otherwise, right methods to specifically populate new_jobs 
        # and sched_jobs.
        
        if job.user in job_dict:
            insort(job_dict[job.user], job)
        else:
            job_dict[job.user] = [job]


    # Add Job (Unordered)
    # Adds a given job to the given dictionary. Stores job in the list
    # associated with the job's username.
    def add_job_unordered(self, job_dict, job):
        
        # TODO: Will this work? A call by reference / call by value problem.
        # If I add things to job_dict, will they be added to the global dict passed
        # in? We'll see. Otherwise, right methods to specifically populate new_jobs 
        # and sched_jobs.
        
        # If user key exists in dict, add to existing list
        if job.user in job_dict:
            job_dict[job.user].append(job)
        else:
            job_dict[job.user] = [job]
        

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
            self.new_jobs[job.user].remove(job)
            log.debug("remove_system_job - Removing job %s from unscheduled jobs."
                      % job.id)
                      
            # If user's job list is empty, remove entry from the new_jobs dict
            if (self.new_jobs[job.user] == []):
                del self.new_jobs[job.user]
                log.info("User %s has no more jobs in the Unscheduled Jobs set. Removing user from queue."
                          % job.user)
                
        # Check for job in scheduled job set
        # elif (job.user in self.sched_jobs) and (job in self.sched_jobs[job.user]):
        elif (job.user in self.sched_jobs) and (self.has_job(self.sched_jobs[job.user], job)):
            self.sched_jobs[job.user].remove(job)
            log.debug("remove_system_job - Removing job %s from scheduled jobs."
                      % job.id)
            
            # If user's job list is empty, remove entry from sched_jobs
            if (self.sched_jobs[job.user] == []):
                del self.sched_jobs[job.user]
                log.info("User %s has no more jobs in the Scheduled Jobs set. Removing user from queue."
                          % job.user)                     
        else:
            log.warning("remove_system_job - Job does not exist in system."
                      + " Doing nothing.")

    # Remove job by id
    # Attempts to remove a job with a given job id from a given
    # list of Jobs. Note: Will remove all jobs with the given ID
    # Note: The job_list MUST be a list of Job objects
    # Parameters:
    #   job_list - (list of Job) the list from which to remove jobs
    #   target_job   - (Job object) the job to be removed
    # No return (if job does not exist in given list, error message logged)
    def remove_job(self, job_list, target_job):
        log.debug("(remove_job) - Target job: %s" % target_job.id)
        
        removals = []
        for job in job_list:
            if (target_job.id == job.id):
                removals.append(job)
                log.debug("(remove_job) - Matching job found: %s" % job.id)
        
        if (len(removals) == 0):
            log.debug("remove_job - Job %s does not exist in given list. Doing nothing." % target_job.id)
            return
        
        for dead_job in removals:
            log.debug("(remove_job) - Removing job in removals list: %s" % dead_job.id)
            job_list.remove(dead_job)

        
        #i = len(job_list)
        #while (i != 0):
        #    i = i-1
        #    if (target_job.id == job_list[i].id):
        #        log.debug("(remove_job) - Matching job found: %s" % job_list[i].id)
        #        job_list.remove(job_list[i])
        #        return   
        

    # Checks to see if the given job ID is in the given job list
    # Note: The job_list MUST be a list of Job objects.
    # Parameters:
    #   job_list - (list of Jobs) The list of jobs in which to check for the given ID
    #   target_job   - (Job Object) The job to check for
    # Returns:
    #   True   - The job exists in the checked lists
    #   False  - The job does not exist in the checked lists
    def has_job(self, job_list, target_job):
        for job in job_list:
            if (target_job.id == job.id):
                return True
        return False


    # Mark job scheduled
    # Makes all changes to a job to indicate that the job has been scheduled
    # Currently: moves passed job from unscheduled to scheduled job list,
    # and changes job status to "Scheduled"
    # Parameters:
    #   job   - (Job object) The job to mark as scheduled
    def schedule(self, job):
        if not ( (job.user in self.new_jobs) and (job in self.new_jobs[job.user]) ):
            log.error("(schedule) - Error: job %s not in the system's Unscheduled jobs" % job.id)
            log.error("(schedule) - Cannot mark job as scheduled")
            return
        
        self.remove_system_job(job)
        self.add_job_unordered(self.sched_jobs, job)
        job.set_status("Scheduled")
        log.debug("(schedule) - Job %s marked as scheduled." % job.id)
    
    
    # Get required VM types
    # Returns a list (of strings) containing the unique required VM types
    # gathered from all jobs in the job pool (scheduled and unscheduled)
    # Returns:
    #   required_vmtypes - (list of strings) A list of required VM types
    def get_required_vmtypes(self):
        required_vmtypes = []
        for jobset in (self.new_jobs.values() + self.sched_jobs.values()):
            for job in jobset:
                if job.req_vmtype not in required_vmtypes:
                    required_vmtypes.append(job.req_vmtype)
        
        log.debug("get_required_vmtypes - Required VM types: " + ", ".join(required_vmtypes))
        return required_vmtypes
                  

    ##
    ## JobPool Private methods (Support methods)
    ##

    # Create a dictionary for Job creation from a full Condor classAd job dictionary
    # If a key for a required job parameter does not exist, add nothing to the new
    # job dictionary (non-present parameters will invoke the default function parameter
    # value).
    # Parameters:
    #   job_classad (dict) - A dictionary of ALL the Condor job classad fields
    # Return:
    #   Returns a dictionary of the Job object parameters the exist in the given
    #   Condor job classad
    #
    # TODO: This needs to be replaced with something more efficient.
    def make_job_dict(self, job_classad):
        log.debug("make_job_dict - Cutting Condor classad dictionary into Job dictionary.")

        job = {}

        # Check for all required Job fields. Add to job dict. if present.
        if ('GlobalJobId' in job_classad):
            job['GlobalJobId'] = job_classad['GlobalJobId']
        if ('Owner' in job_classad):
            job['Owner'] = job_classad['Owner']
        if ('JobPrio' in job_classad):
            job['JobPrio'] = job_classad['JobPrio']
        if ('Requirements' in job_classad):
            vmtype = self.parse_classAd_requirements(job_classad['Requirements'])
            # If vmtype exists (is not None), store it in job dictionary
            if (vmtype):
                job['VMType'] = vmtype
            # else, no VMType field in Job dictionary. Job constructor uses its default value.
        if ('VMNetwork' in job_classad):
            job['VMNetwork'] = job_classad['VMNetwork']
        if ('VMCPUArch' in job_classad):
            job['VMCPUArch'] = job_classad['VMCPUArch']
        if ('VMName' in job_classad):
            job['VMName'] = job_classad['VMName']
        if ('VMLoc' in job_classad):
            job['VMLoc'] = job_classad['VMLoc']
        if ('VMMem' in job_classad):
            job['VMMem'] = job_classad['VMMem']
        if ('VMCPUCores' in job_classad):
            job['VMCPUCores'] = job_classad['VMCPUCores']
        if ('VMStorage' in job_classad):
            job['VMStorage'] = job_classad['VMStorage']

        return job

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
    
    def log_jobs_list(self, jobs):
        if jobs == []:
            log.debug("(none)")
        for job in jobs:
            log.debug("\tJob: %s, %10s, %4d, %10s" % (job.id, job.user, job.priority, job.req_vmtype))
    
    def log_jobs_dict(self, jobs):
        if jobs == {}:
            log.debug("(none)")
        for jobset in jobs.values():
            for job in jobset:
                log.debug("\tJob: %s, %10s, %4d, %10s" % (job.id, job.user, job.priority, job.req_vmtype))


    # A function to encapsulate command execution via Popen.
    # condor_execwait executes the given cmd list, waits for the process to finish,
    # and returns the return code of the process. STDOUT and STDERR are returned
    # Parameters:
    #    cmd   - A list of strings containing the command to be executed
    # Returns:
    #    ret   - The return value of the executed command
    #    out   - The STDOUT of the executed command
    #    err   - The STDERR of the executed command
    # The return of this function is a 3-tuple
    def condor_execwait(self, cmd):
        try:
            sp = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, \
                     stderr=subprocess.PIPE)
            ret = sp.wait()
            (out, err) = sp.communicate(input=None)
            return (ret, out, err)
        except OSError:
            log.error("Couldn't run the following command: '%s' Are the Condor binaries in your $PATH?"
                      % string.join(cmd, " "))
            raise SystemExit
        except:
            log.error("Couldn't run %s command." % string.join(cmd, " "))
            raise


    # Return an arbitrary subset of the jobs list (unscheduled jobs)
    # Parameters:
    #   size   - (int) The number of jobs to return
    # Returns:
    #   subset - (Job list) A list of the jobs selected from the 'jobs' list
    def jobs_subset(self, size):
        # TODO: Write method
        log.warning("jobs_subset - Method not yet implemented")


    # Return a subset of size 'size' of the highest priority jobs from the list
    # of unscheduled jobs
    # Parameters:
    #   size   - (int) The number of jobs to return
    # Returns:
    #   subset - (Job list) A list of the highest priority unscheduled jobs (of
    #            length 'size)
    def jobs_priorityset(self, size):
        # TODO: Write method
        log.warning("jobs_priorityset - Method not yet implemented")



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


