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

##
## CLASSES
##

# The storage structure for individual jobs read from the job scheduler
class Job:

    ## Instance Variables

    #TODO: Add other fields after examining job files and condor pool
    name = "default_job"
    # Job status is used by the scheduler to indicate a job's internal status
    # (Unscheduled, Scheduled)
    status       = "Unscheduled"            
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
    # name     - (str) The name of the job (colloquial)
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
    def __init__(self, name, network, cpuarch, image, imageloc, memory, cpucores, storage):
        print "dbg - New Job object created:"
	print "(Job) - Name: %s, Network: %s, Image:%s, Image Location: %s, Memory: %d" \
	  % (name, network, image, imageloc, memory)
	
	self.name = name
	self.req_network  = network
	self.req_cpuarch  = cpuarch
	self.req_image    = image
	self.req_imageloc = imageloc
        self.req_memory   = memory
	self.req_cpucores = cpucores
	self.req_storage  = storage

	# Set the new job's status
	self.status = "Unscheduled"

    # Short Print
    # Print a short string representing the job
    # Parameters:
    #   spacer  - (str) A string to prepend to each printed line
    def print_short(self, spacer):
        print spacer + "Job Name: %s, Image: %s, Image location: %s, CPU: %s, Memory: %d" \
	  % (self.name, self.req_image, self.req_imageloc, self.req_cpuarch, self.req_memory)


# A pool of all jobs read from the job scheduler. Stores all jobs until they
# complete. Keeps scheduled and unscheduled jobs.
class JobPool:

    ## Instance Variables

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
        print "dbg - New JobPool " + name " created"
	self.name = name
        last_query = datetime.datetime.now()


    # Query Job Scheduler
    # Gets a list of jobs from the job scheduler, and updates internal scheduled
    # and unscheduled job lists with the scheduler information.
    # TODO: Use SOAP calls or local command line interface + parsing
    def job_query(self):
        # TODO: Write method


    # Return an arbitrary subset of the jobs list (unscheduled jobs)
    # Parameters:
    #   size   - (int) The number of jobs to return
    # Returns:
    #   subset - (Job list) A list of the jobs selected from the 'jobs' list
    def jobs_subset(self, size):
        # TODO: Write method

    # Return a subset of size 'size' of the highest priority jobs from the list
    # of unscheduled jobs
    # Parameters:
    #   size   - (int) The number of jobs to return
    # Returns:
    #   subset - (Job list) A list of the highest priority unscheduled jobs (of
    #            length 'size)
    def jobs_priorityset(self, size):
        # TODO: Write method

    
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
	    for job in self.scheduled_jobs
	        job.print_short("\t")

    # Print Unscheduled Jobs (short)
    def print_unsched_jobs(self):
        if len(self.jobs) == 0:
	    print "Unscheduled job list in %s is empty" % self.name
	    return
	else:
	    print "Unscheduled jobs in %s:" % self.name
	    for job in self.scheduled_jobs
	        job.print_short("\t")

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
	    for job in self.job_set
	        job.print_short("\t")


