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
from __future__ import with_statement

import os
import re
import sys
import json
import time
import shlex
import string
import logging
import threading
import subprocess
import ConfigParser

from suds.client import Client
from urllib2 import URLError
from decimal import *
from lxml import etree
from StringIO import StringIO

try:
    import cPickle as pickle
except:
    import pickle

import cluster_tools
import cloudscheduler.config as config

from cloudscheduler.utilities import determine_path
from cloudscheduler.utilities import get_or_none
from cloudscheduler.utilities import ErrTrackQueue
from cloudscheduler.utilities import splitnstrip
import cloudscheduler.utilities as utilities

##
## GLOBALS
##
log = None
log = logging.getLogger("cloudscheduler")

##
## CLASSES
##


class ResourcePool:
    """Stores and organises a list of Cluster resources."""
    ## Instance variables
    resources = []
    machine_list = []
    retired_resources = []
    config_file = ""

    ## Instance methods

    def __init__(self, config_file, name="Resources", condor_query_type=""):
        """ Constructor.
        
        Keywords:
            name   - The name of the ResourcePool being created
            condor_query_type - type of query to do on condor, either local or soap
        """
        global log
        log = logging.getLogger("cloudscheduler")

        log.debug("New ResourcePool " + name + " created")
        self.name = name

        _collector_wsdl = "file://" + determine_path() \
                          + "/wsdl/condorCollector.wsdl"
        self.condor_collector = Client(_collector_wsdl, cache=None, location=config.condor_collector_url)
        self.condor_collector_as_xml = Client(_collector_wsdl, cache=None,
                                              location=config.condor_collector_url, retxml=True)

        self.config_file = os.path.expanduser(config_file)
        self.ban_lock = threading.Lock()
        self.banned_job_resource = {}
        self.failures = {}
        self.setup_lock = threading.Lock()
        self.setup_queued = False

        if not condor_query_type:
            condor_query_type = config.condor_retrieval_method

        if condor_query_type.lower() == "local":
            self.resource_query = self.resource_query_local
        elif condor_query_type.lower() == "soap":
            self.resource_query = self.resource_query_SOAP
        else:
            log.error("Can't use '%s' retrieval method. Using SOAP method." % condor_query_type)
            self.resource_query = self.resource_query_SOAP
            
        if config.scheduling_metric.lower() == "slot":
            self.vmtype_distribution = self.vmtype_slot_distribution
        elif config.scheduling_metric.lower() == "memory":
            self.vmtype_distribution = self.vmtype_mem_distribution
        elif config.scheduling_metric.lower() == "memory_cpu":
            self.vmtype_distribution = self.vmtype_mem_cpu_distribution
        elif config.scheduling_metric.lower() == "memory_cpu_storage":
            self.vmtype_distribution = self.vmtype_mem_cpu_storage_distribution
        else:
            log.error("Can't use '%s' distribution method, not valid" % config.scheduling_metric)
            self.vmtype_distribution = self.vmtype_slot_distribution

        self.setup()
        if config.ban_tracking:
            self.load_banned_job_resource()
        self.load_persistence()



    def setup(self):
        """Read the cloud_resources.conf to determine the available clouds."""
        log.info("Loading cloud resource configuration file %s" % self.config_file)

        if not self.setup_lock.acquire(False):
            log.warning("Reconfig already in progress, queuing the request")
            self.setup_queued = True
            return

        new_resources = []

        try:
            cloud_config = ConfigParser.ConfigParser()
            cloud_config.read(self.config_file)
        except ConfigParser.ParsingError:
            log.exception("Cloud config problem: Couldn't " \
                  "parse your cloud config file. Check for spaces " \
                  "before or after variables.")
            sys.exit(1)

        # Read in config file, parse into Cluster objects
        for cluster in cloud_config.sections():

            new_cluster = self._cluster_from_config(cloud_config, cluster)
            if new_cluster:
                new_resources.append(new_cluster)

        # Check to see if we are removing any clusters. If so,
        # shut down all the VMs of the cluster we're removing
        original_resource_names = [cluster.name for cluster in self.resources]
        new_resource_names = [cluster.name for cluster in new_resources]

        removed_names = set(original_resource_names) - set(new_resource_names)
        added_names = set(new_resource_names) - set(original_resource_names)
        updated_names = set(original_resource_names) & set(new_resource_names)

        if removed_names:
            log.debug("Removing clusters: %s" % removed_names)
        if added_names:
            log.debug("Adding clusters: %s" % added_names)
        if updated_names:
            log.debug("Updating clusters: %s" % updated_names)

        # Set resources list to empty to make sure no VMs are started
        # while we're shuffling things around.
        old_resources = []
        for cluster in reversed(self.resources):
            with cluster.res_lock:
                cluster.vm_slots = 0
                cluster.memory = []
            old_resources.append(cluster)
            self.resources.remove(cluster)

        # Update resources
        # Do this by replacing each updated cluster object with the
        # cluster object built by reading the config file, then copying
        # over to the new object. Feel free to refactor me. I dare you.
        for updated_name in updated_names:
            for old_cluster in old_resources:
                if old_cluster.name == updated_name:

                    with old_cluster.res_lock:
                        for new_cluster in new_resources:
                            if new_cluster.name == updated_name:

                                new_cluster.vms = sorted(old_cluster.vms, key=lambda vm: vm.id)
                                new_cluster.vms = sorted(new_cluster.vms, key=lambda vm: vm.status)
                                while 1:
                                    if new_cluster.vms[0].status == "Error":
                                        new_cluster.vms.append(new_cluster.vms.pop(0))
                                    else:
                                        break
                                new_cluster.vms.reverse()
                                
                                for vm in reversed(new_cluster.vms):
                                    try:
                                        new_cluster.resource_checkout(vm)
                                    except cluster_tools.NoResourcesError, e:
                                        new_cluster.vm_destroy(vm, return_resources=False, reason="Not enough %s on %s." % (e.resource, new_cluster.name))
                                    except:
                                        new_cluster.vm_destroy(vm, return_resources=False, reason="Unexcepted error checking out resources.")
                                self.resources.append(new_cluster)

        # Add new resources
        for new_cluster_name in added_names:
            for new_cluster in new_resources:
                if new_cluster.name == new_cluster_name:
                    self.resources.append(new_cluster)

        # Remove resources
        for removed_cluster_name in removed_names:
            for cluster in reversed(old_resources):
                if cluster.name == removed_cluster_name:
                    log.info("Removing %s from available resources" % 
                                                          removed_cluster_name)
                    self.retired_resources.append(cluster)
                    for vm in cluster.vms:
                            cluster.vm_destroy(vm, return_resources=False, reason="%s has been removed from system." % cluster.name)
                    old_resources.remove(cluster)

        self.setup_lock.release()
        if self.setup_queued:
            self.setup_queued = False
            self.setup()


    @staticmethod
    def _cluster_from_config(config, cluster):
        """Create a new cluster object from a config file's specification."""
        if config.has_option(cluster, "enabled"):
            enabled = config.getboolean(cluster, "enabled")
            if not enabled:
                return None
        cloud_type = get_or_none(config, cluster, "cloud_type")
        max_vm_mem = get_or_none(config, cluster, "max_vm_mem")
        max_vm_mem = int(max_vm_mem) if max_vm_mem != None else -1
        hypervisor = get_or_none(config, cluster, "hypervisor")
        if hypervisor == None:
            hypervisor = 'xen'
        else:
            hypervisor = hypervisor.lower()
            if hypervisor != 'xen' or hypervisor != 'kvm':
                log.error("%s hypervisor not supported." % hypervisor)
                return None

        if cloud_type == "Nimbus":
            nets = splitnstrip(",", get_or_none(config, cluster, "networks"))
            if len(nets) > 1:
                # Split the vm_slots too
                slots = map(int, splitnstrip(",", get_or_none(config, cluster, "vm_slots")))
            else:
                slots = [int(get_or_none(config, cluster, "vm_slots"))]
            net_slots = {}
            for x in range(len(nets)):
                net_slots[nets[x]] = slots[x]
            total_slots = sum(slots)
            return cluster_tools.NimbusCluster(name = cluster,
                    host = get_or_none(config, cluster, "host"),
                    port = get_or_none(config, cluster, "port"),
                    cloud_type = get_or_none(config, cluster, "cloud_type"),
                    memory = map(int, splitnstrip(",", get_or_none(config, cluster, "memory"))),
                    max_vm_mem = max_vm_mem,
                    cpu_archs = splitnstrip(",", get_or_none(config, cluster, "cpu_archs")),
                    networks = nets,
                    vm_slots = total_slots,
                    cpu_cores = int(get_or_none(config, cluster, "cpu_cores")),
                    storage = int(get_or_none(config, cluster, "storage")),
                    netslots = net_slots,
                    hypervisor = hypervisor,
                    )

        elif cloud_type == "AmazonEC2" or cloud_type == "Eucalyptus" or cloud_type == "OpenStack":
            return cluster_tools.EC2Cluster(name = cluster,
                    host = get_or_none(config, cluster, "host"),
                    cloud_type = get_or_none(config, cluster, "cloud_type"),
                    memory = map(int, splitnstrip(",", get_or_none(config, cluster, "memory"))),
                    max_vm_mem = max_vm_mem if max_vm_mem != None else -1,
                    cpu_archs = splitnstrip(",", get_or_none(config, cluster, "cpu_archs")),
                    networks = splitnstrip(",", get_or_none(config, cluster, "networks")),
                    vm_slots = int(get_or_none(config, cluster, "vm_slots")),
                    cpu_cores = int(get_or_none(config, cluster, "cpu_cores")),
                    storage = int(get_or_none(config, cluster, "storage")),
                    access_key_id = get_or_none(config, cluster, "access_key_id"),
                    secret_access_key = get_or_none(config, cluster, "secret_access_key"),
                    security_group = get_or_none(config, cluster, "security_group"),
                    hypervisor = hypervisor,
                    )

        else:
            log.error("ResourcePool.setup doesn't know what to do with the"
                    + "%s cloud_type" % cloud_type)
            return None


    def add_resource(self, cluster):
        """Add a cluster resource to the pool's resource list."""
        self.resources.append(cluster)

    def log_list(self, clusters):
        """Log a list of clusters.
        Supports independently logging a list of clusters for specific ResourcePool
        functionality (such a printing intermediate working cluster lists)
        """
        for cluster in clusters:
            cluster.log()

    def log_pool(self, ):
        """Log the name and address of every cluster in the resource pool."""
        log.debug(self.get_pool_info())

    def get_pool_info(self, ):
        """Print the name and address of every cluster in the resource pool."""
        output = "Resource pool " + self.name + ":\n"
        output += "%-15s  %-10s %-15s \n" % ("NAME", "CLOUD TYPE", "NETWORK ADDRESS")
        if len(self.resources) == 0:
            output += "Pool is empty..."
        else:
            for cluster in self.resources:
                output += "%-15s  %-10s %-15s \n" % (cluster.name, cluster.cloud_type, cluster.network_address)
        return output

    def get_resource(self, ):
        """Return an arbitrary resource from the 'resources' list. Does not remove
        the returned element from the list.
        (Currently, the first cluster in the list is returned)
        """
        if len(self.resources) == 0:
            log.debug("Pool is empty... Cannot return resource.")
            return None

        return (self.resources[0])

    def get_resourceFF(self, network, cpuarch, memory, cpucores, storage):
        """Return the first resource that fits the passed in VM requirements. 
        
        Does not remove the element returned.
        Built to support "First-fit" scheduling.
        
        Keywords:
           network  - the network assoication required by the VM
           cpuarch  - the cpu architecture that the VM must run on
           memory   - the amount of memory (RAM) the VM requires
           cpucores  - the number of cores that a VM requires (dedicated? or general?)
           storage   - the amount of scratch space the VM requires
        
        Returns: returns a Cluster object if one is found that fits VM requirments
                Otherwise, returns the 'None' object
        """
        if len(self.resources) == 0:
            log.debug("Pool is empty... Cannot return FF resource")
            return None

        for cluster in self.resources:
            if not cluster.enabled:
                continue
            # If the cluster has no open VM slots
            if (cluster.vm_slots <= 0):
                continue
            # If the cluster does not have the required CPU architecture
            if not (cpuarch in cluster.cpu_archs):
                continue
            # If required network is NOT in cluster's network associations
            if not (network in cluster.network_pools):
                continue
            if cluster.__class__.__name__ == "NimbusCluster" and cluster.net_slots[network] <= 0:
                continue
            # If request exceeds the max vm memory on cluster
            if memory > cluster.max_vm_mem and cluster.max_vm_mem != -1:
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


    def get_fitting_resources(self, network, cpuarch, memory, cpucores, storage, ami, imageloc, targets=[],
                              hypervisor=['xen']):
        """get a list of Clusters that fit the given VM/Job requirements.
        
        Keywords: (as for get_resource methods)
            network  - the network assoication required by the VM
            cpuarch  - the cpu architecture that the VM must run on
            memory   - the amount of memory (RAM) the VM requires
            cpucores  - the number of cores that a VM requires (dedicated? or general?)
            storage   - the amount of scratch space the VM requires
            ami       - the ami of the vm image - used by EC2 clouds
            imageloc  - the image location url - used by Nimbus clouds
            targets   - list of target clouds 
        Return: a list of Cluster objects representing clusters that meet given
            requirements for network, cpu, memory, and storage
        """
        if len(self.resources) == 0:
            log.debug("Pool is empty... Cannot return list of fitting resources")
            return []

        fitting_clusters = []
        if len(targets) > 0:
            clusters = self.filter_resources_by_names(targets)
        else:
            clusters = self.resources
        for cluster in clusters:
            if not cluster.enabled:
                continue
            if cluster.hypervisor not in hypervisor:
                log.verbose("get_fitting_resources - Wrong hypervisor on %s" % cluster.name)
                continue
            if cluster.__class__.__name__ == "NimbusCluster":
                # If not valid image file to download
                if imageloc == "":
                    continue
                # If required network is NOT in cluster's network associations
                # if network is undefined then it means pick whatever, so we
                # just always okay it.
                if network and (network not in cluster.network_pools):
                    log.verbose("get_fitting_resources - No matching networks in %s" % cluster.name)
                    continue
                if network and network in cluster.net_slots.keys() and cluster.net_slots[network] <= 0:
                    log.verbose("get_fitting_resources - No Slots left in network %s on %s" % (network, cluster.name))
                    continue
                if imageloc in self.banned_job_resource.keys():
                    if cluster.name in self.banned_job_resource[imageloc]:
                        continue
            elif cluster.__class__.__name__ == "EC2Cluster":
                # If no valid ami to boot from
                if ami == "":
                    continue
                # If ami banned from cluster
                if ami in self.banned_job_resource.keys():
                    if cluster.name in self.banned_job_resource[ami]:
                        continue
            # If the cluster has no open VM slots
            if (cluster.vm_slots <= 0):
                log.verbose("get_fitting_resources - No free slots in %s" % cluster.name)
                continue
            # If the cluster does not have the required CPU architecture
            if (cpuarch not in cluster.cpu_archs):
                log.verbose("get_fitting_resources - No matching CPU archs in %s" % cluster.name)
                continue
            # If request exceeds the max vm memory on cluster
            if memory > cluster.max_vm_mem and cluster.max_vm_mem != -1:
                continue
            # If the cluster has no sufficient memory entries for the VM
            if (cluster.find_mementry(memory) < 0):
                log.verbose("get_fitting_resources - No available memory entry in %s" % cluster.name)
                continue
            # If the cluster does not have sufficient CPU cores
            if (cpucores > cluster.cpu_cores):
                log.verbose("get_fitting_resources - Not enough CPU Cores in %s" % cluster.name)
                continue
            # If the cluster does not have sufficient storage capacity
            if (storage > cluster.storageGB):
                log.verbose("get_fitting_resources - Not enough storage in %s" % cluster.name)
                continue
            # Add cluster to the list to be returned (meets all job reqs)
            fitting_clusters.append(cluster)

        # Return the list clusters that fit given requirements
        if fitting_clusters:
            log.debug("List of fitting clusters: ")
            self.log_list(fitting_clusters)
        return fitting_clusters


    def get_resourceBF(self, network, cpuarch, memory, cpucores, storage, ami, imageloc, targets=[], hypervisor=['xen']):
        """
        Returns a resource that fits given requirements and fits some balance
        criteria between clusters (for example, lowest current load or most free
        resources of the fitting clusters).
        Returns the first find as the primary balanced cluster choice, and returns
        a secondary fitting cluster if available (otherwise, None is returned in
        place of a secondary cluster).
        Built to support "Cluster-Balanced Fit Scheduling"
        Note: Currently, we are considering the "most balanced" cluster to be that
        with the fewest running VMs on it. This is to minimize and balance network
        traffic to clusters, among other reasons.
        Other possible metrics are:
          - Most amount of free space for VMs (vm slots, memory, cpu cores..);
          - etc.
        Keywords:
           network  - the network assoication required by the VM
           cpuarch  - the cpu architecture that the VM must run on
           memory   - the amount of memory (RAM) the VM requires
           cpucores  - the number of cores that a VM requires (dedicated? or general?)
           storage   - the amount of scratch space the VM requires
           ami       - image ami - used by EC2 clouds
           imageloc  - image url - used by Nimbus clouds
           targets   - list of target clouds
        Return: returns a tuple of cluster objects. The first, or primary cluster, is the
                most balanced fit. The second, or secondary, is an alternative fitting
                cluster.
                Normal return, (Primary_Cluster, Secondary_Cluster)
                If no secondary cluster is found, (Cluster, None) is returned.
                If no fitting clusters are found, (None, None) is returned.

        """
        # Get a list of fitting clusters
        fitting_clusters = self.get_fitting_resources(network, cpuarch, memory, cpucores, storage, ami, imageloc, targets, hypervisor)

        # If list is empty (no resources fit), return None
        if len(fitting_clusters) == 0:
            log.verbose("No clusters fit requirements. Fitting resources list is empty.")
            return (None, None)

        # If the list has only 1 item, return immediately
        if len(fitting_clusters) == 1:
            log.verbose("Only one cluster fits parameters. Returning that cluster.")
            return (fitting_clusters[0], None)
        #log.verbose("%i clusters fit parameters, determining best 2." % len(fitting_clusters))

        # Set the most-balanced and next-most-balanced initial values
        # Note: mostbal_cluster stands for "most balanced cluster"
        # Note: nextbal_cluster stands for "next most balanced cluster"
        cluster1 = fitting_clusters.pop()
        cluster2 = fitting_clusters.pop()

        if (cluster1.slot_fill_ratio() < cluster2.slot_fill_ratio()):
            mostbal_cluster = cluster1
            nextbal_cluster = cluster2
        else:
            mostbal_cluster = cluster2
            nextbal_cluster = cluster1

        mostbal_vms = mostbal_cluster.slot_fill_ratio()
        nextbal_vms = nextbal_cluster.slot_fill_ratio()

        # Iterate through fitting clusters to check for most and next balanced clusters. (LINEAR search)
        for cluster in fitting_clusters:
            # If considered cluster has fewer running VMs, set it as the most balanced cluster
            if (cluster.slot_fill_ratio() < mostbal_vms):
                mostbal_cluster = cluster
                mostbal_vms = cluster.slot_fill_ratio()
            elif (cluster.slot_fill_ratio() < nextbal_vms):
                nextbal_cluster = cluster
                nextbal_vms = cluster.slot_fill_ratio()

        # Return the most balanced cluster after considering all fitting clusters.
        return (mostbal_cluster, nextbal_cluster)

    def resourcePF(self, network, cpuarch, memory=0, disk=0, hypervisor=['xen']):
        """
        Check that a cluster will be able to meet the static requirements.
        Keywords:
           network  - the network assoication required by the VM
           cpuarch  - the cpu architecture that the VM must run on
           memory   - minimum memory required on cloud
           disk     - minimum storage space required on cloud
           hypervisor - type of hypervisor used by cloud
        Return: True if cluster is found that fits VM requirments
                Otherwise, returns False

        """
        potential_fit = False

        for cluster in self.resources:
            if not cluster.enabled:
                continue
            if cluster.hypervisor not in hypervisor:
                continue
            # If the cluster does not have the required CPU architecture
            if not (cpuarch in cluster.cpu_archs):
                continue
            # If required network is NOT in cluster's network associations
            if network and not (network in cluster.network_pools):
                continue
            # If request exceeds the max vm memory on cluster
            if memory > cluster.max_vm_mem and cluster.max_vm_mem != -1:
                continue
            if not cluster.find_potential_mementry(memory):
                continue
            # Cluster meets network and cpu reqs and may have enough memory
            potential_fit = True
            break

        # If no clusters are found (no clusters can host the required VM)
        return potential_fit

    def get_potential_fitting_resources(self, network, cpuarch, memory, disk, targets=[],
                                        hypervisor=['xen']):
        """
        Determines which clouds could start a VM with the given requirements.
        
        Keywords:
            network - the network pool
            cpuarch - CPU architecture that the VM must run on
            memory  - amount of memory VM requires to run
            disk    - amount of scratch space needed on VM
            targets - list of target clouds
        Return:
            list of clusters that fit requirements
        """
        fitting = []
        clusters = []
        if len(targets) == 0:
            clusters = self.resources
        else:
            clusters = self.filter_resources_by_names(targets)
        for cluster in clusters:
            if not cluster.enabled:
                continue
            if cluster.hypervisor not in hypervisor:
                continue
            if not (cpuarch in cluster.cpu_archs):
                continue
            # If required network is NOT in cluster's network associations
            if network and not (network in cluster.network_pools):
                continue
            # If request exceeds the max vm memory on cluster
            if memory > cluster.max_vm_mem and cluster.max_vm_mem != -1:
                continue
            if not cluster.find_potential_mementry(memory):
                continue
            if disk > cluster.max_storageGB:
                continue
            fitting.append(cluster)
        return fitting

    def filter_resources_by_names(self, names):
        """Return list of clusters that match names."""
        clusters = []
        for name in names:
            cluster = self.get_cluster(name)
            if cluster != None:
                clusters.append(cluster)
            else:
                log.debug("No Cluster with name %s in system" % name)
        return clusters

    def get_cluster(self, cluster_name):
        """Return cluster that matches cluster_name."""
        for cluster in self.resources:
            if cluster.name == cluster_name:
                return cluster
        return None

    def get_cluster_with_vm(self, vm):
        """Find cluster that contains vm."""
        cluster = None
        for c in self.resources:
            if vm in c.vms:
                cluster = c
        return cluster

    def convert_classad_dict(self, ad):
        """Convert the Condor class ad struct into a python dict.

        Note this is done 'stupidly' without checking data types
        """
        native = {}
        attrs = ad[0]
        for attr in attrs:
            if attr.name and attr.value:
                native[attr.name] = attr.value
        return native

    def convert_classad_list(self, ad):
        """Takes a list of Condor class ads to convert."""
        native_list = []
        items = ad[0]
        for item in items:
            native_list.append(self.convert_classad_dict(item))
        return native_list

    def resource_query_local(self):
        """
        resource_query_local -- does a Query to the condor collector

        Returns a list of dictionaries with information about the machines
        registered with condor.
        """
        log.debug("Querying Condor Collector with %s" % config.condor_status_command)

        machine_list = []
        try:
            condor_status = shlex.split(config.condor_status_command)
            sp = subprocess.Popen(condor_status, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (condor_out, condor_err) = sp.communicate(input=None)
        except:
            log.exception("Problem running %s, unexpected error" % string.join(condor_status, " "))
            return []


        machine_list = self._condor_status_to_machine_list(condor_out)

        return machine_list


    def resource_query_SOAP(self):
        """
        resource_query_SOAP -- does a SOAP Query to the condor collector

        Returns a list of dictionaries with information about the machines
        registered with condor.
        """
        log.debug("Querying condor startd with SOAP API")
        try:
            machines_xml = self.condor_collector_as_xml.service.queryStartdAds()
            machine_list = self._condor_machine_xml_to_machine_list(machines_xml)

            return machine_list

        except URLError, e:
            log.exception("There was a problem connecting to the "
                      "Condor scheduler web service (%s) for the following "
                      "reason:"
                      % config.condor_collector_url)
            return []
        except:
            log.exception("There was a problem connecting to the "
                      "Condor scheduler web service (%s)"
                      % (config.condor_collector_url))
            return []


    @staticmethod
    def _condor_status_to_machine_list(condor_status_output):
        """
        _condor_status_to_machine_list - Converts the output of
               condor_status -l to a list of dictionaries with the attributes
               from the Condor machine ad.

               returns [] is there are no machines
        """

        machines = []

        # Each classad is seperated by '\n\n'
        raw_machine_classads = condor_status_output.split("\n\n")
        # Empty condor pools give us an empty string or stray \n in our list
        raw_machine_classads = filter(lambda x: x != "" and x != "\n", raw_machine_classads)

        for raw_classad in raw_machine_classads:
            classad = {}
            classad_lines = raw_classad.splitlines()
            for classad_line in classad_lines:
                classad_line = classad_line.strip()
                (classad_key, classad_value) = classad_line.split(" = ", 1)
                classad_value = classad_value.strip('"')
                classad[classad_key] = classad_value

            machines.append(classad)

        return machines


    @staticmethod
    def _condor_machine_xml_to_machine_list(condor_xml):
        """
        _condor_machine_xml_to_machine_list - Converts Condor SOAP XML from Condor
                to a list of dictionarties with the attributes from the Condor 
                machine ad.

                returns [] if there are no machines
        """
        def _item_attribute(xml, element):
            try:
                return xml.xpath(".//%s" % element)[0].text
            except:
                return ""

        machines = []

        context = etree.iterparse(StringIO(condor_xml))
        for action, elem in context:
            if elem.tag == "item" and elem.getparent().tag == "result":
                xml_machine = elem
                machine = {}
                for item in xml_machine.iter("item"):
                    name = _item_attribute(item, "name")
                    value = _item_attribute(item, "value")
                    machine[name] = value

                machines.append(machine)
                elem.clear()

        return machines


    def get_vmtypes_count(self, machineList):
        """Get a Dictionary of required VM Types with how many of that type running.
        
        Uses the dict-list structure returned by SOAP/local query
        """
        count = {}
        for vm in machineList:
            if vm.has_key('VMType'):
                if vm['VMType'] not in count:
                    count[vm['VMType']] = 1
                else:
                    count[vm['VMType']] += 1
        return count

    def get_uservmtypes_count(self, machineList):
        """Get a dictionary of required VM usertypes currently running.
        
        Keywords:
            machineList - the parsed struct returned from condor of execute nodes
        """
        count = {}
        for vm in machineList:
            if vm.has_key('VMType') and vm.has_key('Start'):
                userexp = re.search('(?<=Owner == ")\w+', vm['Start'])
                if userexp:
                    user = userexp.group(0)
                    vmusertype = ':'.join([user, vm['VMType']])
                    if vmusertype not in count:
                        count[vmusertype] = 1
                    else:
                        count[vmusertype] += 1
            elif vm.has_key('VMType') and vm.has_key('RemoteOwner'):
                try:
                    user = vm['RemoteOwner'].split('@')[0]
                    vmusertype = ':'.join([user, vm['VMType']])
                    if vmusertype not in count:
                        count[vmusertype] = 1
                    else:
                        count[vmusertype] += 1
                except:
                    log.error("Failed to parse out remote owner")
            else:
                log.warning("VM Missing expected Start = ( Owner=='user') and no RemoteOwner set - are the condor init scripts on the VM up-to-date?")
                if vm.has_key('Start'):
                    log.warning("VM Start attrib = %s" % vm['Start'])
                else:
                    log.warning("VM Missing a Start attrib.")
                if not vm.has_key('VMType'):
                    log.warning("This VM has no VMType key, It should not be used with cloudscheduler.")
        log.debug("VMs in machinelist: %s" % str(count))
        return count

    def match_criteria(self, base, criteria):
        """Determines if the key value pairs in in criteria are in the dictionary."""
        return criteria == dict(set(base.items()).intersection(set(criteria.items())))

    def find_in_where(self, machineList, criteria):
        """Find all the matching entries for given criteria."""
        matches = []
        for machine in machineList:
            if self.match_criteria(machine, criteria):
                matches.append(machine)
        return matches

    #Creating a usertype version of this function was skipped
    #def get_vmtypes_count_internal(self):
        #"""Get a dictionary of types of VMs the scheduler is currently tracking."""
        #types = {}
        #for cluster in self.resources:
            #for vm in cluster.vms:
                #if vm.vmtype in types:
                    #types[vm.vmtype] += 1
                #else:
                    #types[vm.vmtype] = 1
        #return types


    def get_vmtypes_count_internal(self):
        """Get a dictionary of uservmtypes of VMs the scheduler is currently tracking."""
        types = {}
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.uservmtype in types:
                    types[vm.uservmtype] += 1
                else:
                    types[vm.uservmtype] = 1
        return types

    def vm_count(self):
        """Count of VMs in the system."""
        count = 0
        for cluster in self.resources:
            count = count + len(cluster.vms)
        return count

    def vmtype_slot_distribution(self, types=None):
        """VM Type Distribution."""
        if types is None:
            types = self.get_vmtypes_count_internal()
        count = Decimal(self.vm_count())
        if count == 0:
            return {}
        count = 1 / count
        for vmtype in types.keys():
            types[vmtype] *= count
        return types

    def vmtype_mem_distribution(self, vmcount=None):
        """VM Type Memory Distribution."""
        if vmcount:
            usage = self.vmtype_resource_usage_sim(vmcount)
        else:
            usage = self.vmtype_resource_usage()
        types = {}
        mem_total = 0
        for vmtype in usage:
            types[vmtype] = usage[vmtype][0]
            mem_total += usage[vmtype][0]
        del usage
        if mem_total == 0:
            return {}
        mem_total = 1 / Decimal(mem_total)
        for vmtype in types.keys():
            types[vmtype] *= mem_total
        return types

    def vmtype_mem_cpu_distribution(self, vmcount=None):
        """VM Type Memory & CPU Distribution."""
        if vmcount:
            usage = self.vmtype_resource_usage_sim(vmcount)
        else:
            usage = self.vmtype_resource_usage()
        types = {}
        mem_cpu_total = 0
        for vmtype in usage:
            mem_cpu_area = usage[vmtype][0] * usage[vmtype][1]
            types[vmtype] = mem_cpu_area
            mem_cpu_total += mem_cpu_area
        del usage
        if mem_cpu_total == 0:
            return {}
        mem_cpu_total = 1 / Decimal(mem_cpu_total)
        for vmtype in types.keys():
            types[vmtype] *= mem_cpu_total
        return types

    def vmtype_mem_cpu_storage_distribution(self, vmcount=None):
        """VM Type Memory & CPU & Storage Distribution."""
        if vmcount:
            usage = self.vmtype_resource_usage_sim(vmcount)
        else:
            usage = self.vmtype_resource_usage()
        types = {}
        vol_total = 0
        weight_all = config.cpu_distribution_weight * config.memory_distribution_weight * config.storage_distribution_weight
        weight_cm = config.cpu_distribution_weight * config.memory_distribution_weight
        for vmtype in usage:
            vol = 0
            if usage[vmtype][2] != 0:
                vol = usage[vmtype][0] * usage[vmtype][1] * usage[vmtype][2] * weight_all
                types[vmtype] = Decimal(str(vol))
            else:
                vol = usage[vmtype][0] * usage[vmtype][1] * weight_cm
                types[vmtype] = Decimal(str(vol))
            vol_total += vol
        del usage
        if vol_total == 0:
            return {}
        if vol_total != 0:
            mem_cpu_storage_total = 1 / Decimal(str(vol_total))
        for vmtype in types.keys():
            types[vmtype] *= mem_cpu_storage_total
        return types

    # Skipped creating an alternate usertypes version of this function
    # VM Type resource usage
    # Counts up how much/many of each resource (RAM, Cores, Storage)
    # are being used by each type of VM
    #def vmtype_resource_usage(self):
        #types = {}
        #for cluster in self.resources:
            #for vm in cluster.vms:
                #if vm.vmtype in types.keys():
                    #types[vm.vmtype].append([vm.memory, vm.cpucores, vm.storage])
                #else:
                    #types[vm.vmtype] = []
                    #types[vm.vmtype].append([vm.memory, vm.cpucores, vm.storage])
        #results = {}
        #for vmtype in types.keys():
            #results[vmtype] = [sum(values) for values in zip(*types[vmtype])]
        #return results

    def vmtype_resource_usage(self):
        """VM Type resource usage w/ uservmtype
        Counts up how much/many of each resource (RAM, Cores, Storage)
        are being used by each type of VM
        """
        types = {}
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.uservmtype in types.keys():
                    types[vm.uservmtype].append([vm.memory, vm.cpucores, vm.storage])
                else:
                    types[vm.uservmtype] = []
                    types[vm.uservmtype].append([vm.memory, vm.cpucores, vm.storage])
        results = {}
        for vmtype in types.keys():
            results[vmtype] = [sum(values) for values in zip(*types[vmtype])]
        return results

    def vmtype_resource_usage_sim(self, vmcount):
        """Count the resources used by each type of VM through a count of VMs instead of iterating over all VMs.
        Will not be able to handle cases where VMs of the same type but different resource usages exist."""
        # Locate a VM for each type in vmcount
        types = {}
        for vmusertype in vmcount.keys():
            foundIt = False
            for cluster in self.resources:
                for vm in cluster.vms:
                    if vm.uservmtype == vmusertype:
                        foundIt = True
                        types[vmusertype] = vm
                        break
                if foundIt:
                    break
            if not foundIt:
                log.warning("Unable to find VM with type %s" % vmusertype)
        results = {}
        # Compute the resource usage based on given counts instead of checking every VM.
        for vmusertype in types.keys():
            results[vmusertype] = [vmcount[vmusertype]*types[vmusertype].memory, vmcount[vmusertype]*types[vmusertype].cpucores, vmcount[vmusertype]*types[vmusertype].storage]
        return results

    #def vm_slots_used(self):
        #types = {}
        #for cluster in self.resources:
            #for vm in cluster.vms:
                #if not types.has_key(vm.vmtype):
                    #types[vm.vmtype] = []
                #if hasattr(vm, "job_per_core") and vm.job_per_core:
                    #for core in range(vm.cpucores):
                        #types[vm.vmtype].append({'memory': vm.memory, 'cores': 1, 'storage': vm.storage})
                #else:
                    #types[vm.vmtype].append({'memory': vm.memory, 'cores': vm.cpucores, 'storage': vm.storage})
        #return types
    
    def vm_slots_total(self):
        """Provides a count of the vm slots across all clusters in the system."""
        count = 0
        for cluster in self.resources:
            count += cluster.max_slots
        return count
            
    def vm_slots_available(self):
        """Provides a count of all available vm slots across all clusters in the system."""
        count = 0
        for cluster in self.resources:
            count += cluster.vm_slots
        return count

    def vm_slots_used(self):
        """Figure out the actual number of 'slots' being used when some VMs are using multi-job settings."""
        types = {}
        for cluster in self.resources:
            for vm in cluster.vms:
                if not types.has_key(vm.uservmtype):
                    types[vm.uservmtype] = []
                if hasattr(vm, "job_per_core") and vm.job_per_core:
                    for core in range(vm.cpucores):
                        types[vm.uservmtype].append({'memory': vm.memory, 'cores': 1, 'storage': vm.storage})
                else:
                    types[vm.uservmtype].append({'memory': vm.memory, 'cores': vm.cpucores, 'storage': vm.storage})
        return types

    def machine_jobs_changed(self, current, previous):
        """Take the current and previous machineLists
        Figure out which machines have changed jobs
        return list of machine names that have
        """
        auxCurrent = dict((d['Name'], d['GlobalJobId']) for d in current if 'GlobalJobId' in d.keys())
        auxPrevious = dict((d['Name'], d['GlobalJobId']) for d in previous if 'GlobalJobId' in d.keys())
        changed = [k for k,v in auxPrevious.items() if k in auxCurrent and auxCurrent[k] != v]
        del auxCurrent
        del auxPrevious
        for n in range(0, len(changed)):
            changed[n] = changed[n].split('.')[0]
        return changed

    def save_persistence(self):
        """
        save_persistence - pickle the resources list to the persistence file
        """
        with self.setup_lock:
            try:
                persistence_file = open(config.persistence_file, "wb")
                pickle.dump(self.resources, persistence_file)
                persistence_file.close()
            except IOError, e:
    
                log.error("Couldn't write persistence file to %s! \"%s\"" % 
                          (config.persistence_file, e.strerror))
            except:
                log.exception("Unknown problem saving persistence file!")

    def load_persistence(self):
        """
        load_persistence - if a pickled persistence file exists, load it and 
                           check to see if the resources described in it are
                           valid. If so, add them to the list of resources.
        """

        try:
            log.info("Loading persistence file from last run.")
            persistence_file = open(config.persistence_file, "rb")
        except IOError, e:
            log.debug("No persistence file to load. Exited normally last time.")
            return
        except:
            log.exception("Unknown problem opening persistence file!")
            return

        try:
            old_resources = pickle.load(persistence_file)
        except:
            log.exception("Unknown problem opening persistence file!")
            return
        persistence_file.close()

        for old_cluster in old_resources:
            old_cluster.setup_logging()

            for vm in old_cluster.vms:

                log.debug("Found VM %s" % vm.id)
                vm_status = old_cluster.vm_poll(vm)
                if vm_status == "Error":
                    old_cluster.vm_destroy(vm, reason="Persisted VM in Error state.")
                elif vm_status == "Destroyed":
                    log.info("VM %s on %s no longer exists. Ignoring it." % (vm.id, old_cluster.name))
                else:
                    new_cluster = self.get_cluster(old_cluster.name)

                    if new_cluster:
                        try:
                            new_cluster.resource_checkout(vm)
                            new_cluster.vms.append(vm)
                            log.info("Persisted VM %s on %s." % (vm.id, new_cluster.name))
                        except cluster_tools.NoResourcesError, e:
                            new_cluster.vm_destroy(vm, return_resources=False, reason="Not enough %s left on %s" %(e.resource, new_cluster.name))
                        except:
                            new_cluster.vm_destroy(vm, return_resources=False, reason="Unexpected error checking out resources.")
                    else:
                        log.info("%s doesn't seem to exist, so destroying vm %s." %
                                 (old_cluster.name, vm.id))
                        old_cluster.vm_destroy(vm, reason="cloud %s no longer exists." % old_cluster.name)


    def track_failures(self, job, resources,  value):
        """Error Tracking to be used to ban / filter resources."""
        for cluster in resources:
            if cluster.__class__.__name__ == 'NimbusCluster':
                if job.req_imageloc in self.failures.keys():
                    foundIt = False
                    for resource in self.failures[job.req_imageloc]:
                        if resource.name == cluster.name:
                            resource.append(value)
                            foundIt = True
                        if foundIt:
                            break
                        else:
                            queue = ErrTrackQueue(cluster.name)
                            queue.append(value)
                            self.failures[job.req_imageloc].append(queue)
                else:
                    self.failures[job.req_imageloc] = []
                    queue = ErrTrackQueue(cluster.name)
                    queue.append(value)
                    self.failures[job.req_imageloc].append(queue)
            elif cluster.__class__.__name__ == 'EC2Cluster':
                if job.req_ami in self.failures.keys():
                    foundIt = False
                    for resource in self.failures[job.req_ami]:
                        if resource.name == cluster.name:
                            resource.append(value)
                            foundIt = True
                        if foundIt:
                            break
                        else:
                            queue = ErrTrackQueue(cluster.name)
                            queue.append(value)
                            self.failures[job.req_ami].append(queue)
                else:
                    self.failures[job.req_ami] = []
                    queue = ErrTrackQueue(cluster.name)
                    queue.append(value)
                    self.failures[job.req_ami].append(queue)

    def check_failures(self):
        """Check if failures have crossed the threshold and ban job from resources."""
        with self.ban_lock:
            banned_changed = False
            for img in self.failures.keys():
                for cq in self.failures[img]:
                    if cq.min_use() and cq.dist_false() == config.ban_failrate_threshold:
                        # add this img / cluster entry to banned jobs
                        if img in self.banned_job_resource.keys():
                            if cq.name not in self.banned_job_resource[img]:
                                self.banned_job_resource[img].append(cq.name)
                                banned_changed = True
                        else:
                            self.banned_job_resource[img] = []
                            self.banned_job_resource[img].append(cq.name)
                            banned_changed = True
            if banned_changed:
                self.save_banned_job_resource()
                log.debug("Updating Banned job file")

    def save_banned_job_resource(self):
        """
        save_banned_job_resource - pickle the banned jobs list to file """
        try:
            ban_file = open(config.ban_file, "w")
            ban_file.write(json.dumps(self.banned_job_resource, encoding='ascii'))
            ban_file.close()
        except IOError, e:

            log.error("Couldn't write ban file to %s! \"%s\"" % 
                      (config.ban_file, e.strerror))
        except:
            log.exception("Unknown problem saving ban file!")

    def load_banned_job_resource(self):
        """
        load_banned_job_resource - reload the file to update which images
                    have been banned from clusters.
        """
        with self.ban_lock:
            no_bans = False
            ban_file = None
            try:
                log.info("Loading ban file.")
                ban_file = open(config.ban_file, "r")
            except IOError, e:
                log.debug("No ban file to load. No images banned.")
                no_bans = True
            except:
                log.exception("Unknown problem opening ban file!")
                return
            updated_ban = {}
            try:
                if not no_bans:
                    updated_ban = json.loads(ban_file.read(), encoding='ascii')
                    ban_file.close()
            except:
                log.exception("Unknown problem opening ban file!")
                return
            # Need to go through the failures and 'reset' any of the 
            # bans that have been removed
            if len(updated_ban) == 0:
                for img in self.banned_job_resource.keys():
                    for res in self.banned_job_resource[img]:
                        foundit = False
                        for cl in self.failures[img]:
                            if cl.name == res:
                                foundit = True
                                cl.clear()
                            if foundit:
                                break
            else:
                for img in updated_ban.keys():
                    if img in self.banned_job_resource.keys():
                        diff = set(self.banned_job_resource[img]) - set(updated_ban[img])
                        for res in diff:
                            foundit = False
                            for cl in self.failures[img]:
                                if cl.name == res:
                                    foundit = True
                                    cl.clear()
                                if foundit:
                                    break
            self.banned_job_resource = updated_ban

    def do_condor_off(self, machine_name, machine_addr):
        """Perform a condor_off on an execute node.

        Executes multiple commands to condor in order to peacefully stop the start deamon
        on a VM so that it will finish its current job but accept no new jobs.
        
        Keywords:
            machine_name - the condor machine name to condor_off
            machine_addr - the condor machine addr to condor_off
        Return:
            a 3 tuple of the returncodes from the 2 commands used and a return code
        """
        cmd = '%s -peaceful -name "%s" -subsystem startd' % (config.condor_off_command, machine_name)
        cmd2 = '%s -peaceful -addr "%s" -startd' % (config.condor_off_command, machine_addr)
        cmd3 = '%s -peaceful -addr "%s" -master' % (config.condor_off_command, machine_addr)
        args = []
        args2 = []
        args3 = []
        if config.cloudscheduler_ssh_key:
            args.append(config.ssh_path)
            args.append('-i')
            args.append(config.cloudscheduler_ssh_key)
            central_address = re.search('(?<=http://)(.*):', config.condor_webservice_url).group(1)
            args.append(central_address)
            args.append(cmd)
            
            args2.append(config.ssh_path)
            args2.append('-i')
            args2.append(config.cloudscheduler_ssh_key)
            args2.append(central_address)
            args2.append(cmd2)
            
            args3.append(config.ssh_path)
            args3.append('-i')
            args3.append(config.cloudscheduler_ssh_key)
            args3.append(central_address)
            args3.append(cmd3)
        else:
            args.append(config.condor_off_command)
            args.append('-peaceful')
            args.append('-name')
            args.append(machine_name)
            args.append('-subsystem')
            args.append('startd')
            
            args2.append(config.condor_off_command)
            args2.append('-peaceful')
            args2.append('-addr')
            args2.append(machine_addr)
            args2.append('-subsystem')
            args2.append('startd')
            
            args3.append(config.condor_off_command)
            args3.append('-peaceful')
            args3.append('-addr')
            args3.append(machine_addr)
            args3.append('-subsystem')
            args3.append('master')
        # Send condor_off to startd first
        try:
            sp1 = subprocess.Popen(args2, shell=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not utilities.check_popen_timeout(sp1):
                (out, err) = sp1.communicate(input=None)
            ret1 = -1
            if out.startswith("Sent"):
                ret1 = 0
            if sp1.returncode == 0 and ret1 == 0:
                log.verbose("Successfuly sent condor_off startd to %s" % (machine_name))
            else:
                log.debug("Failed to send condor_off startd to %s" % (machine_name))
                log.debug("Reason: %s" % out) 
                log.debug("Error: %s" % err)
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (string.join(args, " "), e.errno, e.strerror))
            return (-1, -1, -1, -1)
        except:
            log.error("Problem running %s, unexpected error" % string.join(args, " "))
            return (-1, -1, -1, -1)
        # Now send the master off
        try:
            sp2 = subprocess.Popen(args3, shell=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not utilities.check_popen_timeout(sp2):
                (out, err) = sp2.communicate(input=None)
            ret2 = -1
            if out.startswith("Sent"):
                ret2 = 0
            if sp2.returncode == 0 and ret2 == 0:
                log.verbose("Successfuly sent condor_off master to %s" % (machine_name))
            else:
                log.debug("Failed to send condor_off master to %s" % (machine_name))
                log.debug("Reason: %s \n Error: %s" % (out, err))
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (string.join(args, " "), e.errno, e.strerror))
            return (-1, -1, -1, -1)
        except:
            log.error("Problem running %s, unexpected error" % string.join(args, " "))
            print args
            return (-1, -1, -1, -1)
        return (sp1.returncode, ret1, sp2.returncode, ret2)

    def do_condor_on(self, machine_name, machine_addr):
        """Attempt to turn on the start deamon of a VM.
        
        Keywords:
            machine_name - the condor machine name to try to condor_on
            machine_addr - the condor machine addr to try to condor_on
        Return:
            the return code indicating success of failure to condor_on
        """
        cmd = '%s -subsystem startd -name "%s"' % (config.condor_on_command, machine_name)
        cmd4 = '%s -subsystem master -name "%s"' % (config.condor_on_command, machine_name)
        cmd2 = '%s -addr "%s" -startd' % (config.condor_on_command, machine_addr)
        cmd3 = '%s -addr "%s" -master' % (config.condor_on_command, machine_addr)
        args = []
        args2 = []
        args3 = []
        args4 = []
        if config.cloudscheduler_ssh_key:
            args.append(config.ssh_path)
            args.append('-i')
            args.append(config.cloudscheduler_ssh_key)
            central_address = re.search('(?<=http://)(.*):', config.condor_webservice_url).group(1)
            args.append(central_address)
            args.append(cmd)
            args4.append(config.ssh_path)
            args4.append('-i')
            args4.append(config.cloudscheduler_ssh_key)
            args4.append(central_address)
            args4.append(cmd4)

            args2.append(config.ssh_path)
            args2.append('-i')
            args2.append(config.cloudscheduler_ssh_key)
            args2.append(central_address)
            args2.append(cmd2)
            
            args3.append(config.ssh_path)
            args3.append('-i')
            args3.append(config.cloudscheduler_ssh_key)
            args3.append(central_address)
            args3.append(cmd3)
        else:
            args.append(config.condor_on_command)
            args.append('-subsystem')
            args.append('startd')
            args.append('-name')
            args.append(machine_name)
            args4.append(config.condor_on_command)
            args4.append('-name')
            args4.append(machine_name)
            args4.append('-subsystem')
            args4.append('master')

            args2.append(config.condor_on_command)
            args2.append('-addr')
            args2.append(machine_addr)
            args2.append('-subsystem')
            args2.append('startd')

            args3.append(config.condor_on_command)
            args3.append('-addr')
            args3.append(machine_addr)
            args3.append('-subsystem')
            args3.append('master')
        # try to turn on master first
        try:
            sp1 = subprocess.Popen(args4, shell=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not utilities.check_popen_timeout(sp1):
                (out, err) = sp1.communicate(input=None)
            ret1 = -1
            if out.startswith("Sent"):
                ret1 = 0
            if sp1.returncode == 0 and ret1 == 0:
                log.verbose("Successfuly sent condor_on startd to %s" % (machine_name))
            else:
                log.debug("Failed to send condor_on startd to %s" % (machine_name))
                log.debug("Reason: %s" % out) 
                log.debug("Error: %s" % err)
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (string.join(args, " "), e.errno, e.strerror))
            return (-1, -1, -1, -1)
        except:
            log.error("Problem running %s, unexpected error" % string.join(args, " "))
            return (-1, -1, -1, -1)
        # Now send the startd on
        try:
            sp2 = subprocess.Popen(args, shell=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not utilities.check_popen_timeout(sp2):
                (out, err) = sp2.communicate(input=None)
            ret2 = -1
            if out.startswith("Sent"):
                ret2 = 0
            if sp2.returncode == 0 and ret2 == 0:
                log.verbose("Successfuly sent condor_on master to %s" % (machine_name))
            else:
                log.debug("Failed to send condor_on master to %s" % (machine_name))
                log.debug("Reason: %s \n Error: %s" % (out, err))
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (string.join(args, " "), e.errno, e.strerror))
            return (-1, -1, -1, -1)
        except:
            log.error("Problem running %s, unexpected error" % string.join(args, " "))
            print args
            return (-1, -1, -1, -1)
        return (sp1.returncode, ret1, sp2.returncode, ret2)
        #try:
            #sp = subprocess.Popen(args, shell=False,
                                  #stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            #if not utilities.check_popen_timeout(sp):
                #(out, err) = sp.communicate(input=None)
            #if sp.returncode == 0:
                #log.debug("Successfuly sent condor_on to %s" % (machine_name))
            #else:
                #log.debug("Failed to send condor_on to %s" % (machine_name))
                #log.debug("Reason: %s \n Error: %s" % (out, err))
            #return sp.returncode
        #except OSError, e:
            #log.error("Problem running %s, got errno %d \"%s\"" % (string.join(args, " "), e.errno, e.strerror))
            #return (-1, "")
        #except:
            #log.exception("Problem running %s, unexpected error" % string.join(args, " "))
            #return (-1, "")

    def find_vm_with_name(self, condor_name):
        """Find a VM in cloudscheduler with the given condor machine name(hostname)."""
        foundIt = False
        vm_match = None
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.condorname == condor_name:
                    foundIt = True
                    vm_match = vm
                    break
            if foundIt:
                break
        if not foundIt:
            log.debug("Could not find a VM with name: %s" % condor_name)
        return vm_match

    def find_cluster_with_vm(self, condor_name):
        """Find which cluster holds a VM with the given condor machine name(hostname)."""
        foundIt = False
        cluster_match = None
        vm_match = None
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.condorname == condor_name:
                    foundIt = True
                    cluster_match = cluster
                    vm_match = vm
                    break
            if foundIt:
                break
        return (cluster_match, vm_match)

    def find_vm_with_addr(self, condor_addr):
        """Find a VM with the given condor address."""
        foundIt = False
        vm_match = None
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.condoraddr == condor_addr:
                    foundIt = True
                    vm_match = vm
                    break
            if foundIt:
                break
        return vm_match

    def retiring_vms_of_type(self, vmtype):
        """Get a list of the VMs in the Retiring state of the given type."""
        retiring = []
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.vmtype == vmtype:
                    if vm.override_status == 'Retiring':
                        retiring.append(vm)
        return retiring

    def retiring_vms_of_usertype(self, vmtype):
        """Get a list of the VMs in the Retiring state of the given usertype."""
        retiring = []
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.uservmtype == vmtype:
                    if vm.override_status == 'Retiring':
                        retiring.append(vm)
        return retiring

    def get_starting_of_type(self, vmtype):
        """Get a list of the VMs in the Starting state of the given type."""
        starting = []
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.vmtype == vmtype:
                    if vm.status == "Starting":
                        starting.append(vm)
        return starting
    
    def get_num_starting_vms(self):
        """Count the number of starting state VMs."""
        num_starting = 0
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.status == "Starting":
                    num_starting += 1
        log.debug("There are %i Starting VMs, the max_starting_vm is %i." % (num_starting, config.max_starting_vm))
        return num_starting

    def get_starting_of_usertype(self, vmtype):
        """Get a list of the VMs in the Starting state of the given usertype."""
        starting = []
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.uservmtype == vmtype:
                    if vm.status == "Starting":
                        starting.append(vm)
        return starting

    def get_error_of_usertype(self, vmtype):
        """Get a list of the VMs in the Error state of the given usertype."""
        error = []
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.uservmtype == vmtype:
                    if vm.status == "Error":
                        error.append(vm)
        return error
    def get_all_vms(self):
        """Returns a list of all the VMs in the system."""
        all_vms = []
        for cluster in self.resources:
            for vm in cluster.vms:
                all_vms.append(vm)
        return all_vms

    def get_user_vms(self, user):
        """Returns a list of all VMs of a user."""
        user_vms = []
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.user == user:
                    user_vms.append(vm)
        return user_vms

    def get_cloud_config_output(self):
        """Build up a string of the cloudscheduler configuration values."""
        try:
            cloud_config = ConfigParser.SafeConfigParser()
            cloud_config.read(self.config_file)
        except ConfigParser.ParsingError:
            log.exception("Cloud config problem: Couldn't " \
                  "parse your cloud config file. Check for spaces " \
                  "before or after variables.")
            return None
        outputlist = []
        # Read in config file, parse into Cluster objects
        for cluster in cloud_config.sections():
            items = cloud_config.items(cluster) # list of (name, value) pairs for each option
            outputlist.append(cluster)
            outputlist.append(' ')
            for item in items:
                outputlist.append('[')
                outputlist.append(','.join(item))
                outputlist.append(']')
            outputlist.append('\n')
        return "".join(outputlist)

    def shutdown_cluster_vm(self, clustername, vmid):
        """Manually shutdown a VM, for use by cloud_admin."""
        output = ""
        cluster = self.get_cluster(clustername)
        if cluster:
            vm = cluster.get_vm(vmid)
            if vm:
                # found the vm - shutdown
                # move the vmdestroycmd thread into a better place and import so avilable here
                thread = VMDestroyCmd(cluster, vm, reason="Shutdown request from admin client.")
                thread.start()
                while thread.is_alive():
                    time.sleep(1)
                if not thread.is_alive():
                    if thread.get_result() != 0:
                        output = "Destroying VM %s failed. Leaving it for now." % thread.get_vm().id
                    else:
                        output = "VM %s has been Destroyed." % thread.get_vm().id
                    thread.join()
            else:
                output = "Could not find VM with ID: %s on Cluster: %s." % (vmid, clustername)
        else:
            output = "Could not find a Cluster with name: %s." % clustername
        return output

    def shutdown_cluster_all(self, clustername):
        """Manually shutdown all VMs on a cluster, for use by cloud_admin."""
        output = ""
        vmdesth = {}
        cluster = self.get_cluster(clustername)
        if cluster:
            for vm in cluster.vms:
                th = VMDestroyCmd(cluster, vm, reason="Shutdown request from admin client.")
                vmdesth[vm.id] = th
                th.start()
            while len(vmdesth) > 0:
                to_remove = []
                for k, thread in vmdesth.iteritems():
                    if not thread.is_alive():
                        if thread.get_result() != 0:
                            output += "Destroying VM %s failed. Leaving it for now.\n" % thread.get_vm().id
                        else:
                            output += "VM %s has been Destroyed.\n" % thread.get_vm().id
                        thread.join()
                        to_remove.append(k)
                for k in to_remove:
                    del vmdesth[k]
        else:
            output = "Could not find a Cluster with name: %s." % clustername
        return output
    
    def remove_vm_no_shutdown(self, clustername, vmid):
        """Remove a VM entry from Cloudscheduler without issuing a shutdown to the cluster, for use by cloud_admin."""
        output = ""
        cluster = self.get_cluster(clustername)
        if cluster:
            vm = cluster.get_vm(vmid)
            if vm:
                with cluster.vms_lock:
                    cluster.vms.remove(vm)
                cluster.resource_return(vm)
                output = "Removed %s's VM %s from CloudScheduler." % (clustername, vmid)
            else:
                output = "Could not find that VM ID."
        else:
            output = "Could not find Cloud %s." % clustername
        return output

    def remove_all_vmcloud_no_shutdown(self, clustername):
        """Remove all VM entries from a cluster without issuing shutdowns to the IaaS, for use by cloud_admin."""
        cluster = self.get_cluster(clustername)
        output = ""
        if cluster:
            for vm in reversed(cluster.vms):
                with cluster.vms_lock:
                    cluster.vms.remove(vm)
                cluster.resource_return(vm)
            output = "Removed all VMs from %s." % clustername
        else:
            output = "Could not find Cloud %s." % clustername
        return output

    def disable_cluster(self, clustername):
        """Toggles the enabled flag for a cluster, for use by cloud_admin."""
        cluster = self.get_cluster(clustername)
        ret = ""
        if cluster:
            cluster.enabled = False
            ret = "Cloud: %s disabled." % clustername
        else:
            ret = "Could not find cloud %s." % clustername
        return ret
    
    def enable_cluster(self, clustername):
        """Toggles the enabled flag for a cluster, for use by cloud_admin."""
        cluster = self.get_cluster(clustername)
        ret = ""
        if cluster:
            cluster.enabled = True
            ret = "Cloud: %s enabled." % clustername
        else:
            ret = "Could not find cloud %s." % clustername
        return ret


class VMDestroyCmd(threading.Thread):
    """
    VMCmd - passing shutdown and destroy requests to a separate thread 
    """

    def __init__(self, cluster, vm, reason=""):
        threading.Thread.__init__(self, name=self.__class__.__name__)
        self.cluster = cluster
        self.vm = vm
        self.result = None
        self.reason = reason
    def run(self):
        self.result = self.cluster.vm_destroy(self.vm, reason=self.reason)
        if self.result != 0:
            log.error("Failed to destroy vm %s" % self.vm.id)
    def get_result(self):
        return self.result
    def get_vm(self):
        return self.vm
