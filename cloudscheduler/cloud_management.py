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
from suds.client import Client
import cloudscheduler.config as config
from urllib2 import URLError
from decimal import *

from cloudscheduler.utilities import determine_path
from cloudscheduler.utilities import get_or_none

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
        log.debug("New ResourcePool " + name + " created")
        self.name = name
        _collector_wsdl = "file://" + determine_path() \
                          + "/wsdl/condorCollector.wsdl"
        self.condor_collector = Client(_collector_wsdl, cache=None, location=config.condor_collector_url)

    # Read in defined clouds from cloud definition file
    def setup(self, config_file):

        log.info("Reading cloud resource configuration file %s" % config_file)
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

            cloud_type = get_or_none(cloud_config, cluster, "cloud_type")

            # Create a new cluster according to cloud_type
            if cloud_type == "Nimbus":
                new_cluster = cluster_tools.NimbusCluster(name = cluster,
                               host = get_or_none(cloud_config, cluster, "host"),
                               cloud_type = get_or_none(cloud_config, cluster, "cloud_type"),
                               memory = map(int, get_or_none(cloud_config, cluster, "memory").split(",")),
                               cpu_archs = get_or_none(cloud_config, cluster, "cpu_archs").split(","),
                               networks = get_or_none(cloud_config, cluster, "networks").split(","),
                               vm_slots = int(get_or_none(cloud_config, cluster, "vm_slots")),
                               cpu_cores = int(get_or_none(cloud_config, cluster, "cpu_cores")),
                               storage = int(get_or_none(cloud_config, cluster, "storage")),
                               )

            elif cloud_type == "AmazonEC2" or cloud_type == "Eucalyptus":
                new_cluster = cluster_tools.EC2Cluster(name = cluster,
                               host = get_or_none(cloud_config, cluster, "host"),
                               cloud_type = get_or_none(cloud_config, cluster, "cloud_type"),
                               memory = map(int, get_or_none(cloud_config, cluster, "memory").split(",")),
                               cpu_archs = get_or_none(cloud_config, cluster, "cpu_archs").split(","),
                               networks = get_or_none(cloud_config, cluster, "networks").split(","),
                               vm_slots = int(get_or_none(cloud_config, cluster, "vm_slots")),
                               cpu_cores = int(get_or_none(cloud_config, cluster, "cpu_cores")),
                               storage = int(get_or_none(cloud_config, cluster, "storage")),
                               access_key_id = get_or_none(cloud_config, cluster, "access_key_id"),
                               secret_access_key = get_or_none(cloud_config, cluster, "secret_access_key"),
                               security_group = get_or_none(cloud_config, cluster, "security_group"),
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
        log.debug("List of fitting clusters: ")
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

    # Check that a cluster will be able to meet the static requirements.
    # Parameters:
    #    network  - the network assoication required by the VM
    #    cpuarch  - the cpu architecture that the VM must run on
    # Return: True if cluster is found that fits VM requirments
    #         Otherwise, returns False
    def resourcePF(self, network, cpuarch):
        potential_fit = False

        for cluster in self.resources:
            # If the cluster does not have the required CPU architecture
            if not (cpuarch in cluster.cpu_archs):
                continue
            # If required network is NOT in cluster's network associations
            if not (network in cluster.network_pools):
                continue
            # Cluster meets network and cpu reqs
            potential_fit = True
            break

        # If no clusters are found (no clusters can host the required VM)
        return potential_fit


    # Return cluster that matches cluster_name
    def get_cluster(self, cluster_name):
        for cluster in self.resources:
            if cluster.name == cluster_name:
                return cluster
        return None

    # Find cluster that contains vm
    def get_cluster_with_vm(self, vm):
        cluster = None
        for c in self.resources:
            if vm in c.vms:
                cluster = c
        return cluster

    # Convert the Condor class ad struct into a python dict
    # Note this is done 'stupidly' without checking data types
    def convert_classad_dict(self, ad):
        native = {}
        attrs = ad[0]
        for attr in attrs:
            if attr.name and attr.value:
                native[attr.name] = attr.value
        return native

    # Takes a list of Condor class ads to convert
    def convert_classad_list(self, ad):
        native_list = []
        items = ad[0]
        for item in items:
            native_list.append(self.convert_classad_dict(item))
        return native_list

    # SOAP Query to the condor collector
    # Returns a list of dictionaries with information about the machines
    # registered with condor.
    def resource_querySOAP(self):
        log.debug("Querying condor startd with SOAP API")
        try:
            machines = self.condor_collector.service.queryStartdAds()
            if len(machines) != 0:
                machineList = self.convert_classad_list(machines)
            else:
                machineList = None
            return machineList

        except URLError, e:
            log.error("There was a problem connecting to the "
                      "Condor scheduler web service (%s) for the following "
                      "reason: %s"
                      % (config.condor_collector_url, e.reason[1]))
        except:
            log.error("There was a problem connecting to the "
                      "Condor scheduler web service (%s)"
                      % (config.condor_collector_url))
            raise
            sys.exit(1)

    # Get a Dictionary of required VM Types with how many of that type running
    # Uses the dict-list structure returned by SOAP query
    def get_vmtypes_count(self, machineList):
        count = {}
        for vm in machineList:
            if vm.has_key('VMType'):
                if vm['VMType'] not in count:
                    count[vm['VMType']] = 1
                else:
                    count[vm['VMType']] += 1
        return count

    # Determines if the key value pairs in in criteria are in the dictionary
    def match_criteria(self, base, criteria):
        return criteria == dict(set(base.items()).intersection(set(criteria.items())))
    # Find all the matching entries for given criteria
    def find_in_where(self, machineList, criteria):
        matches = []
        for machine in machineList:
            if self.match_criteria(machine, criteria):
                matches.append(machine)
        return matches

    # Get a dictionary of types of VMs the scheduler is currently tracking
    def get_vmtypes_count_internal(self):
       types = {}
       for cluster in self.resources:
           for vm in cluster.vms:
               if vm.vmtype in types:
                   types[vm.vmtype] += 1
               else:
                   types[vm.vmtype] = 1
       return types

    # Count of VMs in the system
    def vm_count(self):
        count = 0
        for cluster in self.resources:
            count = count + len(cluster.vms)
        return count

    # VM Type Distribution
    def vmtype_distribution(self):
        types = self.get_vmtypes_count_internal()
        count = Decimal(self.vm_count())
        if count == 0:
            return {}
        count = 1 / count
        for type in types.keys():
            types[type] *= count
        return types

    # Take the current and previous machineLists
    # Figure out which machines have changed jobs
    # return list of machine names that have
    def machine_jobs_changed(self, current, previous):
        auxCurrent = dict((d['Name'], d['GlobalJobId']) for d in current if 'GlobalJobId' in d.keys())
        auxPrevious = dict((d['Name'], d['GlobalJobId']) for d in previous if 'GlobalJobId' in d.keys())
        changed = [k for k,v in auxPrevious.items() if k in auxCurrent and auxCurrent[k] != v]
        for n in range(0, len(changed)):
            changed[n] = changed[n].split('.')[0]
        return changed

