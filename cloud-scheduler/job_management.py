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

##
## GLOBALS
##

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

class JobPool:


class JobSet:





