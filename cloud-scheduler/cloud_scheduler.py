#!/usr/bin/python

## GIT test for commit on new branch

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

class Cluster:
    
    # Instance variables (preset to defaults)
    name            = 'def_cluster'
    network_address = '0.0.0.0'
    vm_slots        = 0
    cpu_cores       = 0
    storageGB       = 0
    memoryMB        = 0
    x86             = 'no'
    x86_64          = 'no'
    network_public  = 'no'
    network_private = 'no'

    # Instance methods

    # Constructor
    def __init__(self, ):
        print "dbg - New Cluster created"

    # Set Cluster attributes from a parameter list
    def populate(self, attr_list):
        (self.name, self.network_address, self.vm_slots, self.cpu_cores, \
          self.storageGB, self.memoryMB, self.x86, self.x86_64, \
          self.network_public, self.network_private) = attr_list;
        
        self.network_private = self.network_private.rstrip("\n");
        print "dbg - Cluster populated successfully"
        
    # Print cluster information
    def print_cluster(self):
        print "-" * 80
        print "Name:\t\t%s" % (self.name)
        print "Address:\t%s" % (self.network_address)
        print "VM Slots:\t%s" % (self.vm_slots)
        print "CPU Cores:\t%s" % (self.cpu_cores)
        print "Storage:\t%s" % (self.storageGB)
        print "Memory:\t\t%s" % (self.memoryMB)
        print "x86:\t\t%s" % (self.x86)
        print "x86_64:\t\t%s" % (self.x86_64)
        print "Network Pub:\t%s" % (self.network_public)
        print "Network Priv:\t%s" % (self.network_private)
        print "-" * 80
        

class ResourcePool:
    
    # Instance variables    
    resources = []
    
    # Instance methods

    # Constructor
    def __init__(self, ):
        print "dbg - New ResourcePool created"

    # Add a cluster resource to the pool's resource list
    def add_resource(self, cluster):
        self.resources.append(cluster)


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

    # Create a cloud manager
    testCloud = cloud_management.NimbusCloud()

    # Create a resource pool
    cloud_resources = ResourcePool()

    # Read the cloud config file into a resource pool
    if readCloudConfig(cloud_conffile, cloud_resources):
        print "Reading cloud configuration file failed. Exiting..."
        sys.exit(1)

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

        new_cluster = Cluster()
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







