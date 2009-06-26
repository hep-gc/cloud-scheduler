#!/usr/bin/python

## Auth: Duncan Penfold-Brown. 6/15/2009

##
## The main body for the cloud scheduler, that encapsulates and organizes
## all cloud scheduler functionality.
##

usage = """Usage: cloud_scheduler CLOUD_CONFIG"""


##
## Imports
##

import sys
import getopt
import cloud_management

##
## Class Definitions
##


# Move to cloud_management?
class ResourcePool:
    
    # Instance variables    
    name = "default"
    resources = []
    
    # Instance methods

    # Constructor
    def __init__(self, name):
        print "dbg - New ResourcePool " + name+ " created"
	self.name = name

    # Add a cluster resource to the pool's resource list
    def add_resource(self, cluster):
        self.resources.append(cluster)

    # Print the name+address of every cluster in the resource pool
    def print_pool(self, ):
        print "Resource pool " + self.name + ":"
        if len(self.resources) == 0:
	    print "Pool is empty..."
	else:
	    for cluster in self.resources:
	        print "\t" + cluster.name + ", " + cluster.network_address
	    

##
## Functions
##

def main(argv = sys.argv):

    # Check cmdline arguments (cloud config, )
    if len(argv) < 2:
        print usage
        print "Error in command-line parameters. Exiting..."
        sys.exit(1)
    
    cloud_conffile = argv[1]

    # Create a resource pool
    cloud_resources = ResourcePool("Testpool")

    # Read the cloud config file into a resource pool
    if readCloudConfig(cloud_conffile, cloud_resources):
        print "Reading cloud configuration file failed. Exiting..."
        sys.exit(1)

    # Print the resource pool
    cloud_resources.print_pool()

    print "dbg - tmp. done..."


def readCloudConfig(config_file, rsrc_pool):

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

        # Currently working with only general clusters. Support for checking
        # which type of cluster to create coming in future (add as a config file parameter: Nimbus, OpenNebula...)
        new_cluster = cloud_management.Cluster()
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







