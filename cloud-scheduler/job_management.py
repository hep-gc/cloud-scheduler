#!/usr/bin/python

## Auth.: Duncan Penfold-Brown. 8/07/2009.

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
    req_storage  = 0		# Currently not considered


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
        print "dbg - New Job object created:"
	print "(Job) - Name: %s, Network: %s, Image:%s, Image Location: %s, Memory: %d" \
	  % (name, network, image, imageloc, memory)
	
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

    # Short Print
    # Print a short string representing the job
    # Parameters:
    #   spacer  - (str) A string to prepend to each printed line
    def print_short(self, spacer):
        print spacer + "Job ID: %s, Image: %s, Image location: %s, CPU: %s, Memory: %d" \
	  % (self.id, self.req_image, self.req_imageloc, self.req_cpuarch, self.req_memory)

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
        if not (status in statuses):
	    print "(Job:set_status) - Error: incorrect status '%s' passed" % status
	    print "Status must be one of: " + string.join(self.statuses, ", ")
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
        print "dbg - New JobPool %s created" % name
	self.name = name
        last_query = datetime.datetime.now()


    # Query Job Scheduler via command line condor tools
    # Gets a list of jobs from the job scheduler, and updates internal scheduled
    # and unscheduled job lists with the scheduler information.
    # TODO: Add method job_querySOAP using SOAP calls and the Condor SOAP API
    def job_queryCMD(self):
        print "dbg - JobPool job query method"

	# The regular expression to match and parse the following condor cmd
	# NOTE: This regexp MUST match the (correct) output format of the condor cmd
        job_regex = r"^VMName:\s(\S*)\sVMLoc:\s(\S*)\sVMNetwork:\s(\S*)\sVMCPUArch:" + \
          r"\s(\S*)\sVMMem:\s(\S*)\sOwner:\s(\S*)\sJobId:\s(\S*)"
	
	# The condor_q command to execute to retrieve jobs
        condor_cmd = ["condor_q",
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
	    print "(job_queryCMD) - Job query command failed. Printing stderr" + \
	      "and returning..."
	    print "STDERR:\n%s" % condor_err
	    return
	
	## Parse the correct condor output
	print "(job_queryCMD) - Job query command completed. Parsing output..."

	# Strip the trailing newline from output and stderr
	condor_out = condor_out.rstrip()
	condor_err = condor_err.rstrip()
	
	# Split the command output into lines (each line corresponds to a job)
	job_lines = condor_out.split("\n")

	for line in job_lines:
	    # Check line validity (via regexp). If invalid, continue
            match = re.search(job_regex, line)
	    if not match:
	        print "(job_queryCMD) - Parsing condor output line failed" +\
		  "Regexp failed to match."
		print "(job_queryCMD) - Line '%s' failed to match" % line
		print "(job_queryCMD) - Regexp to match: %s" % job_regex
		continue
	    
	    # Store match groups (regexp captures) locally, temporarily
	    (tmp_image, tmp_imageloc, tmp_network, tmp_cpuarch, tmp_memory, \
	      tmp_owner, tmp_id) = match.groups()
	    
	    # Check if job ID is already in the system. If so, continue
	    if self.has_job(self.jobs, tmp_id):
	        print "(job_queryCMD) - Job %s is already in the 'jobs' list" \
		  % tmp_id
		continue
            if self.has_job(self.scheduled_jobs, tmp_id):
	        print "(job_queryCMD) - Job %s is already in the 'scheduled" +\
		  "_jobs' list" % tmp_id
		continue
            
	    # Check if job is Running (status == 'R'). If so, continue
            # TODO: Add status retrieval to condor cmd and parsing
	    # ALT: If job is running, add to scheduled jobs list?

	    # Create a new job from the parsed condor job line
	    # Note: convert appropriate fields to integers
            new_job = Job(tmp_id, tmp_network, tmp_cpuarch, tmp_image, \
	      tmp_imageloc, int(tmp_memory), DEF_CPUCORES, DEF_STORAGE)

            # Add the new job to the JobPool's unscheduled jobs list ('jobs')
	    self.jobs.append(new_job)
            print "(job_queryCMD) - New job created successfully, added to jobs list."
        
	# When querying is finished, reset last query timestamp
        last_query = datetime.datetime.now()
	
	# After parsing the new condor cmd, print updated job lists
        print "(job_queryCMD) - Updated job lists:"
	self.print_jobs()
        

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
	    print "(schedule) - Error: job %s not in unscheduled jobs list"
	    print "(schedule) - Cannot mark job as scheduled. Returning"
	    return
	self.jobs.remove(job)
	self.scheduled_jobs.append(job)
	job.set_status("Scheduled")
	print "(schedule) - Job %s marked as scheduled." % job.get_id()
    
    
    # Return an arbitrary subset of the jobs list (unscheduled jobs)
    # Parameters:
    #   size   - (int) The number of jobs to return
    # Returns:
    #   subset - (Job list) A list of the jobs selected from the 'jobs' list
    def jobs_subset(self, size):
        # TODO: Write method
	print "Method not yet implemented"

    # Return a subset of size 'size' of the highest priority jobs from the list
    # of unscheduled jobs
    # Parameters:
    #   size   - (int) The number of jobs to return
    # Returns:
    #   subset - (Job list) A list of the highest priority unscheduled jobs (of
    #            length 'size)
    def jobs_priorityset(self, size):
      # TODO: Write method
      print "Method not yet implemented"

    # Print Job Lists (short)
    def print_jobs(self):
	self.print_sched_jobs()
	self.print_unsched_jobs()


    # Print Scheduled Jobs (short)
    def print_sched_jobs(self):
        if len(self.scheduled_jobs) == 0:
	    print "Scheduled job list in %s is empty" % self.name
	    return
	else:
	    print "Scheduled jobs in %s:" % self.name
	    for job in self.scheduled_jobs:
	        job.print_short("\t")

    # Print Unscheduled Jobs (short)
    def print_unsched_jobs(self):
        if len(self.jobs) == 0:
	    print "Unscheduled job list in %s is empty" % self.name
	    return
	else:
	    print "Unscheduled jobs in %s:" % self.name
	    for job in self.scheduled_jobs:
	        job.print_short("\t")


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
        sp = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, \
	  stderr=subprocess.PIPE)
	ret = sp.wait()
	(out, err) = sp.communicate(input=None)
	return (ret, out, err)




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
        print "dbg - New JobSet %s created" % name
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
	    print "(drop_job) - Error: passed job not in job set..."
	    return (1)
	self.job_set.remove(job)
	return (0)

    # Print a short form list of the job set
    def print_short(self):
        if len(self.job_set) == 0:
	    print "Job set %s is empty..." % self.name
	    return
	else:
	    print "Job set %s:" % self.name
	    for job in self.job_set:
	        job.print_short("\t")


