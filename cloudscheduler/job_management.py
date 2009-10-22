#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

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
from cloudscheduler.utilities import determine_path

##
## LOGGING
##

log = logging.getLogger("CloudLogger")


##
## CLASSES
##

# The storage structure for individual jobs read from the job scheduler
class Job:

    ## Instance Variables

    # A list of possible statuses for internal job representation
    statuses = ["Unscheduled", "Scheduled"]
    
    id = "default_jobID"
    status       = statuses[0]  # Set initial status to 'Unscheduled'            
    req_network  = "default_network"
    req_cpuarch  = "x86"
    req_image    = "default_image"           # Currently not considered. Imageloc
    req_imageloc = "default_imagelocation"   # is required
    req_memory   = 0
    req_cpucores = 0            # Currently not considered
    req_storage  = 0            # Currently not considered


    ## Instance Methods

    # Constructor
    # Parameters:
    # id       - (str) The ID of the job (via condor). Functions as name.
    # network  - (str) The network association the job requires. TODO: Should support "any"
    # cpuarch  - (str) The CPU architecture the job requires in its run environment
    # image    - (str) The name of the image the job is to run on
    # imageloc - (str) The location (URL) of the image the job is to run on
    # memory   - (int) The amount of memory in MB the job requires
    # cpucores - (int) The number of cpu cores the job requires
    # storage  - (int) The amount of storage space the job requires
    # NOTE: The image field is used as a name field for the image the job will
    #   run on. The cloud scheduler will eventually be able to search a set of 
    #   repositories for this image name. Currently, imageloc MUST be set. 
    def __init__(self, id, network, cpuarch, image, imageloc, memory, cpucores, storage):
        log.debug("New Job object created:")
        log.debug("(Job) - ID: %s, Network: %s, Image:%s, Image Location: %s, Memory: %d" \
          % (id, network, image, imageloc, memory))
        
        self.id = id
        self.req_network  = network
        self.req_cpuarch  = cpuarch
        self.req_image    = image
        self.req_imageloc = imageloc
        self.req_memory   = memory
        self.req_cpucores = cpucores
        self.req_storage  = storage

        # Set the new job's status
        self.status = self.statuses[0]

    def print_short(self, spacer):
        log.warning("print_short is DEPRECATED use log_job_short instead")
        log_job_short(self)

    # log_job_short
    # Log a short string representing the job
    def log_job_short(self):
        log.debug("Job ID: %s, Image: %s, Image location: %s, CPU: %s, Memory: %d" \
          % (self.id, self.req_image, self.req_imageloc, self.req_cpuarch, self.req_memory))


    # Get ID
    # Returns the job's id string
    def get_id(self):
        return self.id

    # Set status
    # Sets the job's status to the given string
    # Parameters:
    #   status   - (str) A string indicating the job's new status.
    # Note: Status must be one of Scheduled, Unscheduled
    def set_status(self, status):
        if not (status in self.statuses):
            log.debug("(Job:set_status) - Error: incorrect status '%s' passed" % status)
            log.debug("Status must be one of: " + string.join(self.statuses, ", "))
            return
        self.status = status

# A pool of all jobs read from the job scheduler. Stores all jobs until they
# complete. Keeps scheduled and unscheduled jobs.
class JobPool:

    ## Instance Variables

    # Default constant variables for Job creation.
    # TODO: Remove when these options are supported
    DEF_CPUCORES   = 1
    DEF_STORAGE    = 0

    # The 'jobs' list holds all unscheduled jobs in the system. When a job is
    # scheduled by any scheduler that job is moved to 'scheduled_jobs'.

    # ISSUE: How to represent scheduler having started resources for a job, but
    # that job not having started on those resources?
    # Need to avoid thrashing, and starting resources for idle jobs simply because
    # the job scheduler hasn't had time to push them to resources yet.
    #
    # Need two representations? 
    # 1) tracks job status via job scheduler: idle/running
    # 2) tracks cloud scheduler's job treatment: unscheduled/scheduled
    #
    # Sol'n: Cloud scheduler schedules resources for jobs, and moves jobs to 'Scheduled' list.
    #        'Scheduled' list is polled every while (decent amount of time), and if jobs in it are still
    #        idle, return them to the 'Unscheduled' list.
    #
    # Or: Can we just have the cloud scheduler put the jobs in the 'scheduled' list and assume
    # that eventually they'll be run on some resources, and have that be the end of it?
    # We know the resources will have been created or are there, so we could just wait.
    jobs = []
    scheduled_jobs = []


    ## Instance Methods

    # Constructor
    # name       - The name of the job pool being created
    # last_query - A timestamp for the last time the scheduler was queried,
    #              or its creation time
    def __init__(self, name):
        log.debug("dbg - New JobPool %s created" % name)
        self.name = name
        last_query = datetime.datetime.now()

        _condor_url   = "http://canfarpool.phys.uvic.ca:8080"
        _schedd_wsdl  = "file://" + determine_path() \
                        + "/wsdl/condorSchedd.wsdl"
        self.condor_schedd = Client(_schedd_wsdl, location=_condor_url)

    def job_querySOAP(self):
        log.debug("Querying job pool with Condor SOAP API")

        try:
            job_ads = self.condor_schedd.service.getJobAds(None, None)
        except:
            log.error("job_querySOAP - There was a problem connecting to the Condor scheduler Webservice.")
            raise
            sys.exit()

        if job_ads.status.code == "SUCCESS" and job_ads.classAdArray:
            # convert ugly data structure from soap into array of dictionaries
            classads = []
            for soap_classad in job_ads.classAdArray.item:
                new_classad = {}
                for attribute in soap_classad.item:
                    new_classad[attribute.name] = attribute.value
                classads.append(new_classad)

            # Check and see if there are any new classads
            for classad in classads:
                if self.has_job(self.jobs, classad['GlobalJobId']):
                    log.debug("job_querySOAP - Job %s is already in the jobs list" % classad['GlobalJobId'])
                    continue
                if self.has_job(self.scheduled_jobs, classad['GlobalJobId']):
                    log.debug("job_querySOAP - Job %s is already scheduled" % classad['GlobalJobId'])
                    continue

                new_job = Job(classad['GlobalJobId'], classad['VMNetwork'],
                              classad['VMCPUArch'],   classad['VMName'], 
                              classad['VMLoc'],       int(classad['VMMem']),
                              self.DEF_CPUCORES,      self.DEF_STORAGE,)
                self.jobs.append(new_job)
                log.debug("job_querySOAP - New job created successfully,"
                          + " added to jobs list.")

        # When querying is finished, reset last query timestamp
        last_query = datetime.datetime.now()

        # print updated job lists
        log.debug("job_querySOAP - Updated job lists:")
        self.log_jobs()

    # Query Job Scheduler via command line condor tools
    # Gets a list of jobs from the job scheduler, and updates internal scheduled
    # and unscheduled job lists with the scheduler information.
    def job_queryCMD(self):
        log.debug("dbg - JobPool job query method")

        # The regular expression to match and parse the following condor cmd
        # NOTE: This regexp MUST match the (correct) output format of the condor cmd
        job_regex = r"^VMName:\s(\S*)\sVMLoc:\s(\S*)\sVMNetwork:\s(\S*)\sVMCPUArch:" + \
          r"\s(\S*)\sVMMem:\s(\S*)\sOwner:\s(\S*)\sJobId:\s(\S*)"
        
        # The condor_q command to execute to retrieve jobs
        # NOTE: name currently hardcoded - should add as constructor param and cmd_line input (parsed in main)
        # TODO: Remove hardcoded job server name (take as cmd-line parameter in main?)
        condor_cmd = ["condor_q",
          "-name", "canfarpool.phys.uvic.ca",
          "-format", 'VMName: %s ',     'VMName',
          "-format", 'VMLoc: %s ',      'VMLoc',
          "-format", 'VMNetwork: %s ',  'VMNetwork',
          "-format", 'VMCPUArch: %s ',  'VMCPUArch',
          "-format", 'VMMem: %s ',      'VMMem',
          "-format", 'Owner: %s ',      'Owner',
          "-format", 'JobId: %s \\n',      'GlobalJobId',
        ]

        # Execute the condor_cmd, storing the return in a string
        (condor_ret, condor_out, condor_err) = \
          self.condor_execwait(condor_cmd)
        
        if (condor_ret != 0):
            log.error("(job_queryCMD) - Job query command failed. Printing stderr" + \
              "and returning...")
            log.error("STDERR:\n%s" % condor_err)
            return
        
        ## Parse the correct condor output
        log.debug("(job_queryCMD) - Job query command completed. Parsing output...")

        # Strip the trailing newline from output and stderr
        condor_out = condor_out.rstrip()
        condor_err = condor_err.rstrip()
        
        # Split the command output into lines (each line corresponds to a job)
        job_lines = condor_out.split("\n")

        for line in job_lines:
            # Check line validity (via regexp). If invalid, continue
            match = re.search(job_regex, line)
            if not match:
                log.debug("(job_queryCMD) - Parsing condor output line failed" +\
                  "Regexp failed to match.")
                log.debug("(job_queryCMD) - Line '%s' failed to match" % line)
                log.debug("(job_queryCMD) - Regexp to match: %s" % job_regex)
                continue
            
            # Store match groups (regexp captures) locally, temporarily
            (tmp_image, tmp_imageloc, tmp_network, tmp_cpuarch, tmp_memory, \
              tmp_owner, tmp_id) = match.groups()
            
            # Check if job ID is already in the system. If so, continue
            if self.has_job(self.jobs, tmp_id):
                log.debug("(job_queryCMD) - Job %s is already in the 'jobs' list" \
                  % tmp_id)
                continue
            if self.has_job(self.scheduled_jobs, tmp_id):
                log.debug("(job_queryCMD) - Job %s is already in the 'scheduled_jobs' list" % tmp_id)
                continue
            
            # Check if job is Running (status == 'R'). If so, continue
            # TODO: Add status retrieval to condor cmd and parsing
            # ALT: If job is running, add to scheduled jobs list?

            # Create a new job from the parsed condor job line
            # Note: convert appropriate fields to integers
            new_job = Job(tmp_id, tmp_network, tmp_cpuarch, tmp_image, \
              tmp_imageloc, int(tmp_memory), self.DEF_CPUCORES, self.DEF_STORAGE)

            # Add the new job to the JobPool's unscheduled jobs list ('jobs')
            self.jobs.append(new_job)
            log.debug("(job_queryCMD) - New job created successfully, added to jobs list.")
        
        # When querying is finished, reset last query timestamp
        last_query = datetime.datetime.now()
        
        # After parsing the new condor cmd, print updated job lists
        log.debug("(job_queryCMD) - Updated job lists:")
        self.log_jobs()
        

    # Checks to see if the given job ID is in the given job list
    # Note: The job_list MUST be a list of Job objects. 
    # Parameters:
    #   job_list - (list of Jobs) The list of jobs in which to check for the given ID
    #   job_id   - (str) The ID of the job to search for
    # Returns:
    #   True   - The job exists in the checked lists
    #   False  - The job does not exist in the checked lists
    def has_job(self, job_list, job_id):
        for job in job_list:
            if (job_id == job.id):
                return True
        
        return False

    # Mark job scheduled
    # Makes all changes to a job to indicate that the job has been scheduled
    # Currently: moves passed job from unscheduled to scheduled job list, 
    # and changes job status to "Scheduled"
    # Parameters:
    #   job   - (Job object) The job to mark as scheduled
    def schedule(self, job):
        if not (job in self.jobs):
            log.error("(schedule) - Error: job %s not in unscheduled jobs list")
            log.error("(schedule) - Cannot mark job as scheduled. Returning")
            return
        self.jobs.remove(job)
        self.scheduled_jobs.append(job)
        job.set_status("Scheduled")
        log.debug( "(schedule) - Job %s marked as scheduled." % job.get_id())
    
    
    # Return an arbitrary subset of the jobs list (unscheduled jobs)
    # Parameters:
    #   size   - (int) The number of jobs to return
    # Returns:
    #   subset - (Job list) A list of the jobs selected from the 'jobs' list
    def jobs_subset(self, size):
        # TODO: Write method
        log.debug( "Method not yet implemented")

    # Return a subset of size 'size' of the highest priority jobs from the list
    # of unscheduled jobs
    # Parameters:
    #   size   - (int) The number of jobs to return
    # Returns:
    #   subset - (Job list) A list of the highest priority unscheduled jobs (of
    #            length 'size)
    def jobs_priorityset(self, size):
      # TODO: Write method
      log.debug( "Method not yet implemented")

    def print_jobs(self):
        log.warning("print_jobs is DEPRECATED, use log_jobs instead")
        log_jobs(self)

    # Log Job Lists (short)
    def log_jobs(self):
        self.log_sched_jobs()
        self.log_unsched_jobs()

    # Print Scheduled Jobs (short)
    def print_sched_jobs(self):
        log.warning("print_sched_jobs is DEPRECATED, use log_sched_jobs instead")
        log_sched_jobs(self)

    # log scheduled jobs (short)
    def log_sched_jobs(self):
        if len(self.scheduled_jobs) == 0:
            log.debug( "Scheduled job list in %s is empty" % self.name)
            return
        else:
            log.debug( "Scheduled jobs in %s:" % self.name)
            for job in self.scheduled_jobs:
                job.log_job_short()

    # Print Unscheduled Jobs (short)
    def print_unsched_jobs(self):
        log.warning("print_unsched_jobs is DEPRECATED, use log_unshed_jobs instead")
        log_unsched_jobs(self)

    # log Unscheduled Jobs (short)
    def log_unsched_jobs(self):
        if len(self.jobs) == 0:
            log.debug( "Unscheduled job list in %s is empty" % self.name)
            return
        else:
            log.debug("Unscheduled jobs in %s:" % self.name)
            for job in self.jobs:
                job.log_job_short()


    ## JobPool private methods

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
        log.debug("New JobSet %s created" % name)
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

    # Print a short form list of the job set
    def print_short(self):
        log.warning("print_short is DEPRECATED, use log_short instead")
        log_short(self)

    # log a short form list of the job set
    def log_short(self):
        if len(self.job_set) == 0:
            log.debug("Job set %s is empty..." % self.name)
            return
        else:
            log.debug("Job set %s:" % self.name)
            for job in self.job_set:
                job.log_job_short("\t")


