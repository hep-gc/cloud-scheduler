#!/usr/bin/python

## Auth: Duncan Penfold-Brown. 6/15/2009

## CLOUD SCHEDULER
##
## The main body for the cloud scheduler, that encapsulates and organizes
## all cloud scheduler functionality.
##
## Using optparse for command line options (http://docs.python.org/library/optparse.html)
##


## Imports

import sys
import getopt
import threading
import time  # for testing purposes only (sleeps)
import string # for debugging purposes
from optparse import OptionParser
import cloud_management


## GLOBAL VARIABLES

usage_str = "cloud_scheduler [-c FILE | --cluster-config FILE]"
version_str = "Cloud Scheduler v 0.1"


## Thread Bodies

# Polling:
# The resource & running vm polling thread. Inherits from Thread class.
# Polling thread will iterate through the resource pool, updating resource
#   status based on vm_poll calls to each vm in a cluster's 'vms' list (of 
#   running vms)
# Constructed with argument 'resource_pool'
class PollingTh(threading.Thread):
    
    def __init__(self, resource_pool):
        threading.Thread.__init__(self)
	self.resource_pool = resource_pool

    def run(self):
	# TODO: Implement polling thread functionality here.
        print "dbg - Poll - Starting polling thread..."

# Scheduling:
# Scheduling thread will match jobs to available resources and start vms
# (for now, will contain test create/destroy functions)
class SchedulingTh(threading.Thread):
    
    def __init__(self, resource_pool):
        threading.Thread.__init__(self)
	self.resource_pool = resource_pool

    def run(self):
        print "dbg - Sched - Starting scheduling thread..."
        
        ## Initial tests:

	# TODO: Simulate job requirements for VMs (simple parameters only)
	# TODO: Scan through resource pool to find fitting cluster
	# TODO: Start a VM with simulated parameters on fitting cluster

	# VM create parameters (name, networktype, cpuarch, imageloc, mem)
	# Note: These are the only currently used fields that would be retrieved
	#       from the job scheduler + job description files
        req_name = "test-vm-nimbus-01"
	req_network = "public"
	req_cpuarch = "x86"
	req_imageloc = "file://sl53base_i386.img"
	req_mem = 128

	## Create
	
	# Create a VM (DRYRUN) on first cluster in resource pool's 'resources' list
        print "dbg - Sched - Selecting arbitrary resource for test run."
        print "dbg - Sched - Simulated job VM parameters: "
	print "\tname: %s\n\tnetwork assoc.: %s\n\tcpu arch.: %s\n\timage: %s\n\tmemory: %d" \
	  % (req_name, req_network, req_cpuarch, req_imageloc, req_mem)
        target_rsrc = self.resource_pool.get_resource()
        print "dbg - Sched - open resource selected:"
	target_rsrc.print_short()
	target_rsrc.vm_create(req_name, req_network, req_cpuarch, req_imageloc, req_mem)

	# Check that create is reflected internally
	print "dbg - Sched - Print updated cluster information (after create):"
	print "target resource's running VMs: " + string.join(target_rsrc.vms, " ")
	target_rsrc.print_short()
	
	## Wait...
        print "dbg - Sched - Waiting..."
	time.sleep(5)

        ## Poll...

	## Destroy...

        # Call vm_destroy on the first entry in the target resource's 'vms' list
	print "dbg - Sched - Destroying created VM..."
	target_rsrc.vm_destroy(target_rsrc.vms[0])
	print "target resource's running VMs: " + string.join(target_rsrc.vms, " ")
	target_rsrc.print_short()



## Functions

def main():

    # Create a parser and process commandline arguments
    # TODO: Halt(w/error msg) on run without a config file OR an MDS
    parser = OptionParser(usage=usage_str, version=version_str)
    set_options(parser)
    (options, args) = parser.parse_args()

    # Create a resource pool
    cloud_resources = cloud_management.ResourcePool("Testpool")

    # If the cluster config options was passed, read in the config file
    if options.cloud_conffile:
        if read_cloud_config(options.cloud_conffile, cloud_resources):
	    print "Reading cloud configuration file failed. Exiting..."
	    sys.exit(1)

    # TODO: Add code to query an MDS to get initial cluster/cloud information
    #       Should spec. the MDS address on command line, with tag?

    # Print the resource pool
    cloud_resources.print_pool()

    # TODO: Resolve issue of atomicity / reliability when 2 threads are working
    #       on the same resource pool data. Does it matter (best effort!)?
    
    # Create the Polling thread (pass resource pool)
    #poller = PollingTh(cloud_resources)
    #poller.start()

    # Create the Scheduling thread (pass resource pool)
    scheduler = SchedulingTh(cloud_resources)
    scheduler.start()

    print "dbg - Scheduling and Polling threads started."

    # Wait on the scheduler to finish before exiting main
    # (Unnecessary? The scheduler and the poller are the only things that need
    # to remain running)
    print "dbg - Waiting for the scheduler to finish..."
    scheduler.join()


# Sets the command-line options for a passed in OptionParser object (via optparse)
def set_options(parser):

    # Option attributes: action, type, dest, help. See optparse documentation.
    # Defaults: action=store, type=string, dest=[name of the option] help=none
    parser.add_option("-c", "--cloud-config", dest="cloud_conffile", metavar="FILE", \
      help="Designate a config file from which cloud cluster information is obtained")


# Reads in a cmdline passed configuration file containing cloud cluster information
# Stores cluster information in the ResourcePool parameter rsrc_pool
# (see the sample_cloud example configuration file for more information)
def read_cloud_config(config_file, rsrc_pool):

    print "Attempting to read cloud configuration file: " + config_file

    # Open config file for reading
    cloud_conf = open(config_file, 'r')

    # Read in config file, parse into Cluster objects
    for line in cloud_conf:
        
        # Check for commented or blank lines
        if line[0] == "#" or line == "\n" :
            continue

        cluster_attr = line.split('/') 
        print "dbg - %s" % (cluster_attr)

        # Create a new cluster according to cloud_type
	# TODO: A more dynamic format would be helpful here - current solution is hardcoded
        if "Nimbus" in cluster_attr:
	    print "dbg - found new Nimbus cluster"
	    new_cluster = cloud_management.NimbusCluster()
	elif "OpenNebula" in cluster_attr:
	    print "dbg - found new OpenNebula cluster"
	    new_cluster = cloud_management.Cluster()   # TODO: Use superclass for now
	elif "Eucalyptus" in cluster_attr:
	    print "dbg - found new Eucalyptus cluster"
	    new_cluster = cloud_management.Cluster()   # TODO: Use superclass for now
	
	# Use superclass methods for population and print
        new_cluster.populate(cluster_attr)
        new_cluster.print_cluster()
        
        # Add the new cluster to a resource pool
        rsrc_pool.add_resource(new_cluster)

    print "Cloud configuration read succesfully from " + config_file 
    return (0)
    

##
## Main Functionality
##

main()







