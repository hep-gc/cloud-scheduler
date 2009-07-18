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
from optparse import OptionParser
import cloud_management


## GLOBAL VARIABLES

usage_str = "cloud_scheduler [-c FILE | --cluster-config FILE]"
version_str = "Cloud Scheduler v 0.1"

## Functions

def main():

    # Create a parser and process commandline arguments
    parser = OptionParser(usage=usage_str, version=version_str)
    set_options(parser)
    (options, args) = parser.parse_args()

    # Create a resource pool
    cloud_resources = cloud_management.ResourcePool("Testpool")

    # If the cluster config options was passed, read in the config file
    if options.cloud_conffile:
        if readCloudConfig(options.cloud_conffile, cloud_resources):
	    print "Reading cloud configuration file failed. Exiting..."
	    sys.exit(1)

    # TODO: Add code to query an MDS to get initial cluster/cloud information
    #       Should spec. the MDS address on command line, with tag?

    # Print the resource pool
    cloud_resources.print_pool()

    print "dbg - tmp. done..."


# Sets the command-line options for a passed in OptionParser object (via optparse)
def set_options(parser):

    # Option attributes: action, type, dest, help. See optparse documentation.
    # Defaults: action=store, type=string, dest=[name of the option] help=none
    parser.add_option("-c", "--cloud-config", dest="cloud_conffile", metavar="FILE", \
      help="Designate a config file from which cloud cluster information is obtained")


# Reads in a cmdline passed configuration file containing cloud cluster information
# Stores cluster information in the ResourcePool parameter rsrc_pool
# (see the sample_cloud example configuration file for more information)
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







