#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

# Copyright (C) 2009 University of Victoria
# You may distribute under the terms of either the GNU General Public
# License or the Apache v2 License, as specified in the README file.

## Auth: Duncan Penfold-Brown. 6/15/2009.

## CLOUD MANAGEMENT
##

##
## IMPORTS
##

import os
import sys
import logging

import ConfigParser
import cluster_tools

##
## GLOBALS
##
log = None

##
## CLASSES
##

# A class that stores and organises a list of Cluster resources

class ResourcePool:

    ## Instance variables
    resources = []

    ## Instance methods

    # Constructor
    # name   - The name of the ResourcePool being created
    def __init__(self, name):
        global log
        log = logging.getLogger("cloudscheduler") 
        log.info("New ResourcePool " + name + " created")
        self.name = name

    # Read in defined clouds from cloud definition file
    def setup(self, config_file):

        log.info("Reading cloud configuration file %s" % config_file)
        # Check for config files with ~ in the path
        config_file = os.path.expanduser(config_file)

        cloud_config = ConfigParser.ConfigParser()
        try:
            cloud_config.read(config_file)
        except ConfigParser.ParsingError:
            print >> sys.stderr, "Cloud config problem: Couldn't " \
                  "parse your cloud config file. Check for spaces " \
                  "before or after variables."
            raise


        # Read in config file, parse into Cluster objects
        for cluster in cloud_config.sections():

            cloud_type = cloud_config.get(cluster, "cloud_type")

            # Create a new cluster according to cloud_type
            if cloud_type == "Nimbus":
                new_cluster = cluster_tools.NimbusCluster(name = cluster,
                               host = cloud_config.get(cluster, "host"),
                               cloud_type = cloud_config.get(cluster, "cloud_type"),
                               memory = map(int, cloud_config.get(cluster, "memory").split(",")),
                               cpu_archs = cloud_config.get(cluster, "cpu_archs").split(","),
                               networks = cloud_config.get(cluster, "networks").split(","),
                               vm_slots = cloud_config.getint(cluster, "vm_slots"),
                               cpu_cores = cloud_config.getint(cluster, "cpu_cores"),
                               storage = cloud_config.getint(cluster, "storage"),
                               )

            elif cloud_type == "AmazonEC2" or cloud_type == "Eucalyptus":
                new_cluster = cluster_tools.EC2Cluster(name = cluster,
                               host = cloud_config.get(cluster, "host"),
                               cloud_type = cloud_config.get(cluster, "cloud_type"),
                               memory = map(int, cloud_config.get(cluster, "memory").split(",")),
                               cpu_archs = cloud_config.get(cluster, "cpu_archs").split(","),
                               networks = cloud_config.get(cluster, "networks").split(","),
                               vm_slots = cloud_config.getint(cluster, "vm_slots"),
                               cpu_cores = cloud_config.getint(cluster, "cpu_cores"),
                               storage = cloud_config.getint(cluster, "storage"),
                               access_key_id = cloud_config.get(cluster, "access_key_id"),
                               secret_access_key = cloud_config.get(cluster, "secret_access_key"),
                               )

            else:
                log.error("ResourcePool.setup doesn't know what to do with the"
                          + "%s cloud_type" % cloud_type)
                continue

            # Add the new cluster to a resource pool
            if new_cluster:
                self.add_resource(new_cluster)
        #END For


    # Add a cluster resource to the pool's resource list
    def add_resource(self, cluster):
        self.resources.append(cluster)

    # Log a list of clusters.
    # Supports independently logging a list of clusters for specific ResourcePool
    # functionality (such a printing intermediate working cluster lists)
    def log_list(self, clusters):
        for cluster in clusters:
            cluster.log()

    # Log the name and address of every cluster in the resource pool
    def log_pool(self, ):
        log.debug(self.get_pool_info())

    # Print the name and address of every cluster in the resource pool
    def get_pool_info(self, ):
        output = "Resource pool " + self.name + ":\n"
        output += "%-15s  %-10s %-15s \n" % ("NAME", "CLOUD TYPE", "NETWORK ADDRESS")
        if len(self.resources) == 0:
            output += "Pool is empty..."
        else:
            for cluster in self.resources:
                output += "%-15s  %-10s %-15s \n" % (cluster.name, cluster.cloud_type, cluster.network_address)
        return output

    # Return an arbitrary resource from the 'resources' list. Does not remove
    # the returned element from the list.
    # (Currently, the first cluster in the list is returned)
    def get_resource(self, ):
        if len(self.resources) == 0:
            log.debug("Pool is empty... Cannot return resource.")
            return None

        return (self.resources[0])

    # Return the first resource that fits the passed in VM requirements. Does
    # not remove the element returned.
    # Built to support "First-fit" scheduling.
    # Parameters:
    #    network  - the network assoication required by the VM
    #    cpuarch  - the cpu architecture that the VM must run on
    #    memory   - the amount of memory (RAM) the VM requires
    #    cpucores  - the number of cores that a VM requires (dedicated? or general?)
    #    storage   - the amount of scratch space the VM requires
    # Return: returns a Cluster object if one is found that fits VM requirments
    #         Otherwise, returns the 'None' object
    def get_resourceFF(self, network, cpuarch, memory, cpucores, storage):
        if len(self.resources) == 0:
            log.debug("Pool is empty... Cannot return FF resource")
            return None

        for cluster in self.resources:
            # If the cluster has no open VM slots
            if (cluster.vm_slots <= 0):
                continue
            # If the cluster does not have the required CPU architecture
            if not (cpuarch in cluster.cpu_archs):
                continue
            # If required network is NOT in cluster's network associations
            if not (network in cluster.network_pools):
                continue
            # If the cluster has no sufficient memory entries for the VM
            if (cluster.find_mementry(memory) < 0):
                continue
            # If the cluster does not have sufficient CPU cores
            if (cpucores > cluster.cpu_cores):
                continue
            # If the cluster does not have sufficient storage capacity
            if (storage > cluster.storageGB):
                continue

            # Return the cluster as an available resource (meets all job reqs)
            return cluster

        # If no clusters are found (no clusters can host the required VM)
        return None


    # Returns a list of Clusters that fit the given VM/Job requirements
    # Parameters: (as for get_resource methods)
    # Return: a list of Cluster objects representing clusters that meet given
    #         requirements for network, cpu, memory, and storage
    def get_fitting_resources(self, network, cpuarch, memory, cpucores, storage):
        if len(self.resources) == 0:
            log.debug("Pool is empty... Cannot return list of fitting resources")
            return []

        fitting_clusters = []
        for cluster in self.resources:
            # If the cluster has no open VM slots
            if (cluster.vm_slots <= 0):
                continue
            # If the cluster does not have the required CPU architecture
            if (cpuarch not in cluster.cpu_archs):
                continue
            # If required network is NOT in cluster's network associations
            if (network not in cluster.network_pools):
                continue
            # If the cluster has no sufficient memory entries for the VM
            if (cluster.find_mementry(memory) < 0):
                continue
            # If the cluster does not have sufficient CPU cores
            if (cpucores > cluster.cpu_cores):
                continue
            # If the cluster does not have sufficient storage capacity
            if (storage > cluster.storageGB):
                continue
            # Add cluster to the list to be returned (meets all job reqs)
            fitting_clusters.append(cluster)

        # Return the list clusters that fit given requirements
        log.info("List of fitting clusters: ")
        self.log_list(fitting_clusters)
        return fitting_clusters


    # Returns a resource that fits given requirements and fits some balance
    # criteria between clusters (for example, lowest current load or most free
    # resources of the fitting clusters).
    # Returns the first find as the primary balanced cluster choice, and returns
    # a secondary fitting cluster if available (otherwise, None is returned in
    # place of a secondary cluster).
    # Built to support "Cluster-Balanced Fit Scheduling"
    # Note: Currently, we are considering the "most balanced" cluster to be that
    # with the fewest running VMs on it. This is to minimize and balance network
    # traffic to clusters, among other reasons.
    # Other possible metrics are:
    #   - Most amount of free space for VMs (vm slots, memory, cpu cores..);
    #   - etc.
    # Parameters:
    #    network  - the network assoication required by the VM
    #    cpuarch  - the cpu architecture that the VM must run on
    #    memory   - the amount of memory (RAM) the VM requires
    #    cpucores  - the number of cores that a VM requires (dedicated? or general?)
    #    storage   - the amount of scratch space the VM requires
    # Return: returns a tuple of cluster objects. The first, or primary cluster, is the
    #         most balanced fit. The second, or secondary, is an alternative fitting
    #         cluster.
    #         Normal return, (Primary_Cluster, Secondary_Cluster)
    #         If no secondary cluster is found, (Cluster, None) is returned.
    #         If no fitting clusters are found, (None, None) is returned.
    def get_resourceBF(self, network, cpuarch, memory, cpucores, storage):

        # Get a list of fitting clusters
        fitting_clusters = self.get_fitting_resources(network, cpuarch, memory, cpucores, storage)

        # If list is empty (no resources fit), return None
        if len(fitting_clusters) == 0:
            log.debug("No clusters fit requirements. Fitting resources list is empty.")
            return (None, None)
        
        # If the list has only 1 item, return immediately
        if len(fitting_clusters) == 1:
            log.debug("Only one cluster fits parameters. Returning that cluster.")
            return (fitting_clusters[0], None)

        # Set the most-balanced and next-most-balanced initial values
        # Note: mostbal_cluster stands for "most balanced cluster"
        # Note: nextbal_cluster stands for "next most balanced cluster"
        cluster1 = fitting_clusters.pop()
        cluster2 = fitting_clusters.pop()
        
        if (cluster1.num_vms() < cluster2.num_vms()):
            mostbal_cluster = cluster1
            nextbal_cluster = cluster2
        else:
            mostbal_cluster = cluster2
            nextbal_cluster = cluster1
        
        mostbal_vms = mostbal_cluster.num_vms()
        nextbal_vms = nextbal_cluster.num_vms()

        # Iterate through fitting clusters to check for most and next balanced clusters. (LINEAR search)
        for cluster in fitting_clusters:
            # If considered cluster has fewer running VMs, set it as the most balanced cluster
            if (cluster.num_vms() < mostbal_vms):
                mostbal_cluster = cluster
                mostbal_vms = cluster.num_vms()
            elif (cluster.num_vms() < nextbal_vms):
                nextbal_cluster = cluster
                nextbal_vms = cluster.unm_vms()

        # Return the most balanced cluster after considering all fitting clusters.
        return (mostbal_cluster, nextbal_cluster)

