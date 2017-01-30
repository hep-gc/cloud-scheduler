#!/usr/bin/env python

from __future__ import with_statement

import os
import re
import sys
import json
import time
import copy
import shlex
import string
import logging
import tempfile
import threading
import subprocess
import ConfigParser

from decimal import *
from collections import defaultdict

try:
    import cPickle as pickle
except:
    import pickle

import cluster_tools
import ec2cluster
try:
    import stratuslabcluster
except:
    pass
try:
    import googlecluster
except Exception as e:
    pass
try:
    import openstackcluster
except:
    pass
try:
    import azurecluster
except:
    pass
try:
    import botocluster
except:
    pass

import cloudscheduler.config as config
import cloudconfig

from cloudscheduler.utilities import determine_path
from cloudscheduler.utilities import get_or_none
from cloudscheduler.utilities import ErrTrackQueue
from cloudscheduler.utilities import splitnstrip
import cloudscheduler.utilities as utilities


# GLOBALS
log = None
log = logging.getLogger("cloudscheduler")

"""Verify if stratuslab dependencies are available"""
try:
    from stratuslab.Image import Image
    stratuslab_support = True
except ImportError:
    stratuslab_support = False
    log.warning("Stratuslab dependencies are not available")

##
## CLASSES
##


class ResourcePool:    
    
    """Stores and organises a list of Cluster resources."""
    ## Instance variables
    resources = []
    machine_list = []
    prev_machine_list = []
    vm_machine_list = []
    prev_vm_machine_list = []
    master_list = []
    retired_resources = []
    config_file = ""

    ## Instance methods

    def __init__(self, config_file, name="Resources", condor_query_type="local"):
        """ Constructor.
        
        Keywords:
            name   - The name of the ResourcePool being created
            condor_query_type - type of query to do on condor - local 
        """
        global log
        log = logging.getLogger("cloudscheduler")

        log.verbose("New ResourcePool %s created" %name)
        self.name = name

        self.config_file = os.path.expanduser(config_file)
        self.ban_lock = threading.Lock()
        self.banned_job_resource = {}
        self.user_vm_limits = {}
        self.failures = {}
        self.setup_lock = threading.Lock()
        self.setup_queued = False
        self.non_cs_condor_machines = set()
        self.missing_vm_condor_machines = set()

        if not condor_query_type:
            condor_query_type = config.condor_retrieval_method

        if condor_query_type.lower() == "local":
            self.resource_query = self.resource_query_local
        else:
            log.error("Can't use '%s' retrieval method. Using local method." % condor_query_type)
            self.resource_query = self.resource_query_local
            
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
        
        if config.user_limit_file:
            self.user_vm_limits = self.load_user_limits(config.user_limit_file)
        if config.ban_tracking:
            self.load_banned_job_resource()
        if config.target_cloud_alias_file:
            self.target_cloud_aliases = self.load_cloud_aliases(config.target_cloud_alias_file)
        else:
            self.target_cloud_aliases = {}
        self.load_persistence()


    def setup(self):
        """Read the cloud_resources.conf to determine the available clouds."""
        log.debug("Loading cloud resource configuration file %s" % self.config_file)

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
            if cloudconfig.verify_sections_base(cloud_config, cluster):
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
    def _cluster_from_config(cconfig, cluster):
        """Create a new cluster object from a config file's specification."""
        enabled = False
        #if not _verify_cloud_conf_base(cconfig, cluster):
        #    return None
        cloud_type = get_or_none(cconfig, cluster, "cloud_type")
        max_vm_mem = get_or_none(cconfig, cluster, "max_vm_mem")
        try:
            max_vm_mem = int(max_vm_mem) if max_vm_mem != None else -1
        except ValueError:
            log.error("%s max_vm_mem must be a valid number." % cluster)
        max_vm_storage = get_or_none(cconfig, cluster, "max_vm_storage")
        try:
            max_vm_storage = int(max_vm_storage) if max_vm_storage != None else -1
        except ValueError:
            log.error("%s max_vm_storage must be a valid number." % cluster)
        total_cpu_cores = get_or_none(cconfig, cluster, "total_cpu_cores")
        try:
            total_cpu_cores = int(total_cpu_cores) if total_cpu_cores != None else -1
        except ValueError:
            log.error("%s total_cpu_cores must be a valid number." % cluster)
        priority = get_or_none(cconfig, cluster, "priority")
        try:
            priority = int(priority) if priority != None else 0
        except ValueError:
            log.error("%s Priority must be a valid number." % cluster)
        keep_alive = get_or_none(cconfig, cluster, "vm_keep_alive")
        try:
            keep_alive = int(keep_alive)*60 if keep_alive else 0
            if keep_alive > config.max_keepalive:
                keep_alive = config.max_keepalive
        except ValueError:
            log.error("%s KeepAlive must be a valid number." % cluster)
            keep_alive = 0
        networks = []
        if cconfig.has_option(cluster, "networks"):
            try:
                networks = splitnstrip(",", get_or_none(cconfig, cluster, "networks"))
            except:
                log.error("No networks specified for %s, will use the default" % cluster)

        if cloud_type.lower() == "amazonec2" or cloud_type.lower() == "eucalyptus" or cloud_type.lower() == "openstack":
            try:
                port = int(get_or_none(cconfig, cluster, "port"))
            except TypeError:
                port = 8773
            if cloudconfig.verify_cloud_conf_ec2(cconfig, cluster):
                return ec2cluster.EC2Cluster(name = cluster.lower(),
                    host = get_or_none(cconfig, cluster, "host"),
                    cloud_type = get_or_none(cconfig, cluster, "cloud_type"),
                    memory = int(get_or_none(cconfig, cluster, "memory")),
                    max_vm_mem = max_vm_mem if max_vm_mem != None else -1,
                    networks = networks,
                    vm_slots = int(get_or_none(cconfig, cluster, "vm_slots")),
                    cpu_cores = int(get_or_none(cconfig, cluster, "cpu_cores")),
                    storage = int(get_or_none(cconfig, cluster, "storage")),
                    access_key_id = get_or_none(cconfig, cluster, "access_key_id"),
                    secret_access_key = get_or_none(cconfig, cluster, "secret_access_key"),
                    security_group = splitnstrip(",", get_or_none(cconfig, cluster, "security_group")),
                    key_name = get_or_none(cconfig, cluster, "key_name"),
                    boot_timeout = get_or_none(cconfig, cluster, "boot_timeout"),
                    secure_connection = get_or_none(cconfig, cluster, "secure_connection"),
                    regions = map(str, splitnstrip(",", get_or_none(cconfig, cluster, "regions"))),
                    reverse_dns_lookup = get_or_none(cconfig, cluster, "reverse_dns_lookup"),
                    placement_zone = get_or_none(cconfig, cluster, "placement_zone"),
                    enabled=enabled,
                    priority = priority,
                    keep_alive=keep_alive,
                    port= port,
                    )
        elif cloud_type.lower() == "stratuslab" and stratuslab_support and cloudconfig.verify_cloud_conf_stratuslab(cconfig, cluster):
            return stratuslabcluster.StratusLabCluster(name = cluster.lower(),
                    host = get_or_none(cconfig, cluster, "host"),
                    cloud_type = get_or_none(cconfig, cluster, "cloud_type"),
                    memory = int(get_or_none(cconfig, cluster, "memory")),
                    max_vm_mem = max_vm_mem if max_vm_mem != None else -1,
                    networks = networks,
                    vm_slots = int(get_or_none(cconfig, cluster, "vm_slots")),
                    cpu_cores = int(get_or_none(cconfig, cluster, "cpu_cores")),
                    storage = int(get_or_none(cconfig, cluster, "storage")),
                    contextualization = get_or_none(cconfig, cluster, "contextualization"),
                    enabled=enabled,
                    priority = priority,
                    keep_alive=keep_alive,
                    )
        elif cloud_type.lower() == "googlecomputeengine" or cloud_type.lower() == "gce" and cloudconfig.verify_cloud_conf_gce(cconfig, cluster):
            return googlecluster.GoogleComputeEngineCluster(name = cluster.lower(),
                    cloud_type = get_or_none(cconfig, cluster, "cloud_type"),
                    memory = int(get_or_none(cconfig, cluster, "memory")),
                    max_vm_mem = max_vm_mem if max_vm_mem != None else -1,
                    networks = networks,
                    vm_slots = int(get_or_none(cconfig, cluster, "vm_slots")),
                    cpu_cores = int(get_or_none(cconfig, cluster, "cpu_cores")),
                    storage = int(get_or_none(cconfig, cluster, "storage")),
                    auth_dat_file = get_or_none(cconfig, cluster, "auth_dat_file"),
                    secret_file = get_or_none(cconfig, cluster, "secret_file"),
                    security_group = splitnstrip(",", get_or_none(cconfig, cluster, "security_group")),
                    boot_timeout = get_or_none(cconfig, cluster, "boot_timeout"),
                    project_id = get_or_none(cconfig, cluster, "project_id"),
                    enabled=enabled,
                    priority = priority,
                    total_cpu_cores = total_cpu_cores,
                    keep_alive=keep_alive,
                    )
        elif cloud_type.lower() == "openstacknative" and cloudconfig.verify_cloud_conf_openstacknative(cconfig, cluster):
            return openstackcluster.OpenStackCluster(name = cluster.lower(),
                    cloud_type = get_or_none(cconfig, cluster, "cloud_type"),
                    memory = int(get_or_none(cconfig, cluster, "memory")),
                    max_vm_mem = max_vm_mem if max_vm_mem != None else -1,
                    networks = networks,
                    vm_slots = int(get_or_none(cconfig, cluster, "vm_slots")),
                    cpu_cores = int(get_or_none(cconfig, cluster, "cpu_cores")),
                    storage = int(get_or_none(cconfig, cluster, "storage")),
                    username = get_or_none(cconfig, cluster, "username"),
                    password = get_or_none(cconfig, cluster, "password"),
                    tenant_name = get_or_none(cconfig, cluster, "tenant_name"),
                    auth_url = get_or_none(cconfig, cluster, "auth_url"),
                    security_group = splitnstrip(",", get_or_none(cconfig, cluster, "security_group")),
                    key_name = get_or_none(cconfig, cluster, "key_name"),
                    boot_timeout = get_or_none(cconfig, cluster, "boot_timeout"),
                    secure_connection = get_or_none(cconfig, cluster, "secure_connection"),
                    regions = map(str, splitnstrip(",", get_or_none(cconfig, cluster, "regions"))),
                    reverse_dns_lookup = get_or_none(cconfig, cluster, "reverse_dns_lookup"),
                    placement_zone = get_or_none(cconfig, cluster, "placement_zone"),
                    enabled=enabled,
                    priority = priority,
                    cacert = get_or_none(cconfig, cluster, "cacert"),
                    keep_alive=keep_alive,
                    )
        elif cloud_type.lower() == "azure" and cloudconfig.verify_cloud_conf_azure(cconfig, cluster):
            return azurecluster.AzureCluster(name = cluster.lower(),
                    cloud_type = get_or_none(cconfig, cluster, "cloud_type"),
                    memory = int(get_or_none(cconfig, cluster, "memory")),
                    max_vm_mem = max_vm_mem if max_vm_mem != None else -1,
                    vm_slots = int(get_or_none(cconfig, cluster, "vm_slots")),
                    cpu_cores = int(get_or_none(cconfig, cluster, "cpu_cores")),
                    storage = int(get_or_none(cconfig, cluster, "storage")),
                    username = get_or_none(cconfig, cluster, "username"),
                    password = get_or_none(cconfig, cluster, "password"),
                    tenant_name = get_or_none(cconfig, cluster, "tenant_name"),
                    boot_timeout = get_or_none(cconfig, cluster, "boot_timeout"),
                    regions = map(str, splitnstrip(",", get_or_none(cconfig, cluster, "regions"))),
                    placement_zone = get_or_none(cconfig, cluster, "placement_zone"),
                    enabled=enabled,
                    priority = priority,
                    keycert = get_or_none(cconfig, cluster, "keycert"),
                    keep_alive=keep_alive,
                    blob_url= get_or_none(cconfig, cluster, "blob_url"),
                    service_name= get_or_none(cconfig, cluster, "service_name"),)
        elif cloud_type.lower() == "opennebula":
            try:
                port = int(get_or_none(cconfig, cluster, "port"))
            except TypeError:
                port = 8773
            if cloudconfig.verify_cloud_conf_ec2(cconfig, cluster):
                return botocluster.BotoCluster(name = cluster.lower(),
                    host = get_or_none(cconfig, cluster, "host"),
                    cloud_type = get_or_none(cconfig, cluster, "cloud_type"),
                    memory = int(get_or_none(cconfig, cluster, "memory")),
                    max_vm_mem = max_vm_mem if max_vm_mem != None else -1,
                    networks = networks,
                    vm_slots = int(get_or_none(cconfig, cluster, "vm_slots")),
                    cpu_cores = int(get_or_none(cconfig, cluster, "cpu_cores")),
                    storage = int(get_or_none(cconfig, cluster, "storage")),
                    access_key_id = get_or_none(cconfig, cluster, "access_key_id"),
                    secret_access_key = get_or_none(cconfig, cluster, "secret_access_key"),
                    security_group = splitnstrip(",", get_or_none(cconfig, cluster, "security_group")),
                    key_name = get_or_none(cconfig, cluster, "key_name"),
                    boot_timeout = get_or_none(cconfig, cluster, "boot_timeout"),
                    secure_connection = get_or_none(cconfig, cluster, "secure_connection"),
                    regions = get_or_none(cconfig, cluster, "regions"),
                    reverse_dns_lookup = get_or_none(cconfig, cluster, "reverse_dns_lookup"),
                    placement_zone = get_or_none(cconfig, cluster, "placement_zone"),
                    enabled=enabled,
                    priority = priority,
                    keep_alive=keep_alive,
                    port= port,
                    )
        else:
            log.error("ResourcePool.setup encountered a problem creating entry for %s" % cluster)
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
        output = "Resource pool %s:\n" %self.name
        output += "%-15s  %-10s %-30s %-10s \n" % ("NAME", "CLOUD TYPE", "NETWORK ADDRESS", "ENABLED")
        if len(self.resources) == 0:
            output += "Pool is empty..."
        else:
            for cluster in self.resources:
                output += "%-15s  %-10s %-30s %-10s\n" % (cluster.name, cluster.cloud_type, cluster.network_address, cluster.enabled)
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

    def get_resourceFF(self, network, memory, cpucores, storage):
        """Return the first resource that fits the passed in VM requirements. 
        
        Does not remove the element returned.
        Built to support "First-fit" scheduling.
        
        Keywords:
           network  - the network assoication required by the VM
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
            # If required network is NOT in cluster's network associations
            if not (network in cluster.network_pools):
                continue
            # If request exceeds the max vm memory on cluster
            if memory > cluster.max_vm_mem and cluster.max_vm_mem != -1:
                continue
            # If the cluster has no sufficient memory entries for the VM
            # If the cluster does not have sufficient CPU cores
            if (cpucores > cluster.cpu_cores):
                continue

            # Return the cluster as an available resource (meets all job reqs)
            return cluster

        # If no clusters are found (no clusters can host the required VM)
        return None


    def get_fitting_resources(self, network, memory, cpucores, storage, ami, imageloc, targets=[],
                              blocked=[]):
        """get a list of Clusters that fit the given VM/Job requirements.
        
        Keywords: (as for get_resource methods)
            network  - the network assoication required by the VM
            memory   - the amount of memory (RAM) the VM requires
            cpucores  - the number of cores that a VM requires (dedicated? or general?)
            storage   - the amount of scratch space the VM requires
            ami       - the ami of the vm image - used by EC2 clouds
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
            log.verbose("Trying with cluster %s (Name: %s)" % (str(cluster), cluster.name))
            if not cluster.enabled:
                log.verbose("get_fitting_resources - %s is disabled - skipping" % cluster.name)
                continue
            if cluster.name in blocked:
                log.verbose("get_fitting_resources - %s is blocked." % cluster.name)
                continue
            if cluster.__class__.__name__ == "EC2Cluster":
                # If no valid ami to boot from
                if ami == "":
                    continue
                # If ami banned from cluster
                if ami in self.banned_job_resource.keys():
                    if cluster.name in self.banned_job_resource[ami]:
                        log.verbose("get_fitting_resources - %s ami banned on %s" % (ami, cluster.name))
                        continue
            
            elif cluster.__class__.__name__ == "StratusLabCluster" and stratuslab_support:
                # If not valid image file
                if imageloc == "":
                    continue
                if imageloc in self.banned_job_resource.keys():
                    if cluster.name in self.banned_job_resource[imageloc]:
                        continue
                if (not Image.isDiskId(imageloc)) and (not Image.isImageId(imageloc)):
                    continue
            
            # If the cluster has no open VM slots
            if (cluster.vm_slots <= 0):
                log.verbose("get_fitting_resources - No free slots in %s" % cluster.name)
                continue
            # If request exceeds the max vm memory on cluster
            if memory > cluster.max_vm_mem and cluster.max_vm_mem != -1:
                log.verbose("get_fitting_resources - memory request exceeds max_vm_mem on %s" % cluster.name)
                continue
            # If the cluster has no sufficient memory entries for the VM
            if (memory > cluster.memory):
                log.verbose("get_fitting_resources - Not enough Memory  in %s" % cluster.name)
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
            log.verbose("List of fitting clusters: ")
            self.log_list(fitting_clusters)
        return fitting_clusters


    def get_resourceBF(self, network, memory, cpucores, storage, ami, imageloc, targets=[], blocked=[]):
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
           memory   - the amount of memory (RAM) the VM requires
           cpucores  - the number of cores that a VM requires (dedicated? or general?)
           storage   - the amount of scratch space the VM requires
           ami       - image ami - used by EC2 clouds
           targets   - list of target clouds
        Return: returns a tuple of cluster objects. The first, or primary cluster, is the
                most balanced fit. The second, or secondary, is an alternative fitting
                cluster.
                Normal return, (Primary_Cluster, Secondary_Cluster)
                If no secondary cluster is found, (Cluster, None) is returned.
                If no fitting clusters are found, (None, None) is returned.

        """
        # Get a list of fitting clusters
        fitting_clusters = self.get_fitting_resources(network, memory, cpucores, storage, ami, imageloc, targets, blocked)

        # If list is empty (no resources fit), return None
        if len(fitting_clusters) == 0:
            log.verbose("No clusters fit requirements. Fitting resources list is empty.")
            return []

        # If the list has only 1 item, return immediately
        if len(fitting_clusters) == 1:
            log.verbose("Only one cluster fits parameters. Returning that cluster.")
            return fitting_clusters

        # sort them based on how full and return the list
        fitting_clusters.sort(key=lambda cluster: cluster.slot_fill_ratio())
        fitting_clusters.sort(key=lambda cluster: cluster.priority)
        return fitting_clusters

    def resourcePF(self, network, memory=0, disk=0):
        """
        Check that a cluster will be able to meet the static requirements.
        Keywords:
           network  - the network assoication required by the VM
           memory   - minimum memory required on cloud
           disk     - minimum storage space required on cloud
        Return: True if cluster is found that fits VM requirments
                Otherwise, returns False

        """
        potential_fit = False

        for cluster in self.resources:
            if not cluster.enabled:
                continue
            # If required network is NOT in cluster's network associations
            if network and not (network in cluster.network_pools):
                continue
            # If request exceeds the max vm memory on cluster
            if memory > cluster.max_vm_mem and cluster.max_vm_mem != -1:
                continue
            if not cluster.check_memory(memory):
                continue
            # Cluster meets network and cpu reqs and may have enough memory
            potential_fit = True
            break

        # If no clusters are found (no clusters can host the required VM)
        return potential_fit

    def get_potential_fitting_resources(self, network, memory, disk, targets=[],
                                        cpucores=-1, blocked=[]):
        """
        Determines which clouds could start a VM with the given requirements.
        
        Keywords:
            network - the network pool
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
            if cluster.name in blocked:
                continue
            # If required network is NOT in cluster's network associations
            if network and not (network in cluster.network_pools):
                continue
            # If request exceeds the max vm memory on cluster
            if memory > cluster.max_vm_mem and cluster.max_vm_mem != -1:
                continue
            if not cluster.check_memory(memory):
                continue
            if disk > cluster.max_storageGB:
                continue

            fitting.append(cluster)
        return fitting

    def filter_resources_by_names(self, names):
        """Return list of clusters that match names."""
        expanded_names = self.resolve_target_cloud_alias(names)
        clusters = []
        for name in expanded_names:
            cluster = self.get_cluster(name.lower())
            if cluster != None:
                clusters.append(cluster)
            else:
                log.debug("No Cluster with name %s in system" % name)
        return clusters

    def get_cluster(self, cluster_name, retired=False):
        """Return cluster that matches cluster_name."""
        if retired:
            res_list = self.retired_resources
        else:
            res_list = self.resources
        for cluster in res_list:
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
        log.verbose("Querying Condor Collector with %s" % config.condor_status_command)
        condor_status=condor_out=condor_err=""
        try:
            condor_status = shlex.split(config.condor_status_command)
            sp = subprocess.Popen(condor_status, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (condor_out, condor_err) = sp.communicate(input=None)
        except OSError:
            log.error("OSError occured while doing condor_status - will try again next cycle.")
            return []
        except:
            log.exception("Problem running %s, unexpected error: %s" % (string.join(condor_status, " "), condor_err))
            return []

        return self._condor_status_to_machine_list(condor_out)

    def master_resource_query_local(self):
        """
        master_resource_query_local -- does a Query to the condor collector about master daemons

        Returns a list of dictionaries with information about the machines masters
        registered with condor.
        """
        log.verbose("Querying Condor Collector with %s" % config.condor_status_master_command)
        condor_status=condor_out=condor_err=""
        try:
            condor_status = shlex.split(config.condor_status_master_command)
            sp = subprocess.Popen(condor_status, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (condor_out, condor_err) = sp.communicate(input=None)
        except OSError:
            log.error("OSError occured while doing condor_status -master - will try again next cycle.")
            return []
        except:
            log.exception("Problem running %s, unexpected error: %s" % (string.join(condor_status, " "), condor_err))
            return []

        return self._condor_status_to_machine_list(condor_out)

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

    def get_vmtypes_count(self, machineList):
        """Get a Dictionary of required VM Types with how many of that type running.
        
        Uses the dict-list structure returned by local query
        """
        count = defaultdict(int)
        for vm in machineList:
            count[vm.vmtype] += 1
        return count

    def get_uservmtypes_count(self, machineList):
        """Get a dictionary of required VM usertypes currently running.
        
        Keywords:
            machineList - the parsed struct returned from condor of execute nodes
        """
        count = defaultdict(int)
        for vm in machineList:
            if vm.slot_type == "Partitionable" and vm.total_slots != "1":
                continue
            if vm.remote_owner:
                try:
                    user = vm.remote_owner.split('@')[0]
                    vmusertype = ':'.join([user, vm.vmtype])
                    count[vmusertype] += 1
                except:
                    log.error("Failed to parse out remote owner on %s" % vm.machine_name)
            elif vm.start_req:
                userexp = re.search('(?<=Owner == ")\w+', vm.start_req)
                if userexp:
                    user = userexp.group(0)
                    vmusertype = ':'.join([user, vm.vmtype])
                    count[vmusertype] += 1
            elif vm.activity == 'Retiring':
                pass
            else:
                log.warning("VM Missing expected Start = ( Owner=='user') and no RemoteOwner set on %s - are the condor init scripts on the VM up-to-date?" % vm.machine_name)
                if vm.start_req:
                    log.warning("VM Start attrib = %s on %s" % (vm.start_req, vm.machine_name))
                elif vm.activity == 'Retiring':
                    pass
                else:
                    log.warning("VM Missing a Start attrib on %s." % vm.machine_name)
                if not vm.vmtype:
                    log.warning("This VM %s has no VMType key, It should not be used with cloudscheduler." % vm.machine_name)
        log.verbose("VMs in machinelist: %s" % str(count))
        return count

    def match_criteria(self, base, criteria):
        """Determines if the key value pairs in in criteria are in the dictionary."""
        return criteria == dict(set(base.items()).intersection(set(criteria.items())))

    def find_in_where(self, machineList, criteria):
        """Find all the matching entries for given criteria."""
        matches = []
        for machine in machineList:
            if self.match_criteria(machine.__dict__, criteria):
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
        types = defaultdict(int)
        for cluster in self.resources:
            for vm in cluster.vms:
                types[vm.uservmtype] += 1
        return types

    def get_vmtypes_count_cpu_slots(self):
        """Get a dictionary of uservmtypes of VMs the scheduler is currently tracking."""
        types = defaultdict(int)
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.job_per_core:
                    types[vm.uservmtype] += vm.cpucores
                else:
                    types[vm.uservmtype] += 1
        return types

    def get_vm_count_user(self, user):
        """Get a count of the number of VMs for specified user."""
        count = 0
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.user == user:
                    count += 1
        return count

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


    def vmtype_resource_usage(self):
        """VM Type resource usage w/ uservmtype
        Counts up how much/many of each resource (RAM, Cores, Storage)
        are being used by each type of VM
        """
        types = defaultdict(list)
        for cluster in self.resources:
            for vm in cluster.vms:
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
        types = defaultdict(list)
        for cluster in self.resources:
            for vm in cluster.vms:
                if hasattr(vm, "job_per_core") and vm.job_per_core:
                    for _ in range(vm.cpucores):
                        types[vm.uservmtype].append({'memory': vm.memory, 'cores': 1, 'storage': vm.storage})
                else:
                    types[vm.uservmtype].append({'memory': vm.memory, 'cores': vm.cpucores, 'storage': vm.storage})
        return types

    def machine_jobs_changed(self, current, previous):
        """Take the current and previous machineLists
        Figure out which machines have changed jobs
        return list of machine names that have
        """
        auxCurrent = dict((d.name, d.global_job_id) for d in current)
        auxPrevious = dict((d.name, d.global_job_id) for d in previous)
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
            log.exception("Unknown problem unpickling persistence file!")
            try:
                pbak = open('/tmp/cloudscheduler.persistence.bak', 'wb')
                persistence_file = open(config.persistence_file, "rb")
                pcontents = persistence_file.read()
                pbak.write(pcontents)
            except Exception as e:
                log.error("Problem trying to create backup pickle: %s" % e)
            return
        persistence_file.close()
        
        empty_vm = cluster_tools.VM()
        empty_vm_key_set = set(empty_vm.__dict__)
        
        for old_cluster in old_resources:
            old_cluster.setup_logging()
            new_cluster = self.get_cluster(old_cluster.name)
            if new_cluster:
                new_cluster.enabled = old_cluster.enabled
                if new_cluster.__class__.__name__ == 'AzureCluster':
                    new_cluster.count = old_cluster.count

            for vm in old_cluster.vms:
                log.debug("Found VM %s on %s" % (vm.id, old_cluster.name))
                if new_cluster:
                    try:
                        new_cluster.resource_checkout(vm)
                        try:
                            # Add any new attributes to VM object loaded from pickle
                            vm_key_set = set(vm.__dict__)
                            key_diff = empty_vm_key_set - vm_key_set
                            while len(key_diff) > 0:
                                vm.__dict__[key_diff.pop()] = None
                        except Exception as e:
                            log.exception("Exception appending new keys %s" % e)
                        new_cluster.vms.append(vm)
                        log.info("Persisted VM %s on %s." % (vm.id, new_cluster.name))
                    except cluster_tools.NoResourcesError, e:
                        if config.retire_reallocate:
                            if old_cluster not in self.retired_resources:
                                old_cluster_copy = copy.deepcopy(old_cluster)
                                old_cluster_copy.vms = []
                                old_cluster_copy.vms.append(vm)
                                self.retired_resources.append(old_cluster_copy)
                            else:
                                old_copy = self.retired_resources.get_cluster(old_cluster.name)
                                old_copy.vms.append(vm)
                            vm.return_resources = False
                            self.force_retire_vm(vm)
                        else:
                            new_cluster.vm_destroy(vm, return_resources=False, reason="Not enough %s left on %s" %(e.resource, new_cluster.name))
                    except:
                        if config.retire_reallocate:
                            self.force_retire_vm(vm)
                        else:
                            new_cluster.vm_destroy(vm, return_resources=False, reason="Unexpected error checking out resources.")
                else:
                    log.info("%s doesn't seem to exist, so destroying vm %s." %
                             (old_cluster.name, vm.id))
                    old_cluster.vm_destroy(vm, reason="cloud %s no longer exists." % old_cluster.name)

    def track_failures(self, job, resources,  value):
        """Error Tracking to be used to ban / filter resources."""
        for cluster in resources:
            if cluster.__class__.__name__ == 'StratusLabCluster' and stratuslab_support:
                # If not valid image file to download
                if job.req_imageloc == "":
                    continue
                if (not Image.isDiskId(job.req_imageloc)) and (not Image.isImageId(job.req_imageloc)):
                    continue
                for resource in self.failures[job.req_imageloc]:
                    if resource.name == cluster.name:
                        resource.append(value)
            else:
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
                log.verbose("Updating Banned job file")

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

    def load_user_limits(self, path=None):
            limit_file = None
            try:
                log.info("Loading user VM Limits file.")
                limit_file = open(path, "r")
            except IOError, e:
                log.debug("No user vm limit file to load. No Limits set.")
                return {}
            except:
                log.exception("Unknown problem opening user limit file!")
                return {}
            user_limits = {}
            try:
                user_limits = json.loads(limit_file.read(), encoding='ascii')
                limit_file.close()
                log.debug("User limit file loaded.")
            except:
                log.exception("Unknown problem opening user limit file!")
                return {}
            return user_limits

    def load_cloud_aliases(self, path=None):
            alias_file = None
            try:
                log.info("Loading Cloud Alias file.")
                alias_file = open(path, "r")
            except IOError, e:
                log.debug("No Cloud Alias file to load. No alias' set.")
                return {}
            except:
                log.exception("Unknown problem opening cloud alias file!")
                return {}
            cloud_alias = {}
            try:
                cloud_alias = json.loads(alias_file.read(), encoding='ascii')
                alias_file.close()
                log.debug("Cloud Alias file loaded.")
            except:
                log.exception("Unknown problem parsing cloud alias file!")
                return {}
            return cloud_alias

    def do_condor_off(self, machine_name, machine_addr, master_addr):
        """Perform a condor_off on an execute node.

        Executes multiple commands to condor in order to peacefully stop the start deamon
        on a VM so that it will finish its current job but accept no new jobs.
        
        Keywords:
            machine_name - the condor machine name to condor_off
            machine_addr - the condor machine addr to condor_off
        Return:
            a 3 tuple of the returncodes from the 2 commands used and a return code
        """
        log.debug("cloud_management.py::do_condor_off: %s, addr: %s, master_addr: %s"%(machine_name,machine_addr,master_addr))
        #cmd = '%s -peaceful -name "%s" -subsystem startd' % (config.condor_off_command, machine_name)
        cmd2 = '%s -peaceful -addr "%s" -subsystem startd' % (config.condor_off_command, machine_addr)
        cmd3 = '%s -peaceful -addr "%s" -subsystem master' % (config.condor_off_command, master_addr)
        #args = []
        args2 = []
        args3 = []
        if machine_name == None:
            machine_name = 'NoneType'
        if machine_addr == None:
            machine_addr = 'NoneType'
            log.debug("Start Addr is None for Machine: %s cannot do condor_off." % machine_name)
            return (-1,-1,-1,-1)
        if master_addr == None:
            master_addr = 'NoneType'
            log.debug("Master Addr is None for Machine: %s cannot do condor_off." % machine_name)
            return (-1,-1,-1,-1)
        if config.cloudscheduler_ssh_key:
            #args.append(config.ssh_path)
            #args.append('-i')
            #args.append(config.cloudscheduler_ssh_key)
            central_address = re.search('(?<=http://)(.*):', config.condor_webservice_url).group(1)
            #args.append(central_address)
            #args.append(cmd)
            
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
            #args.append(config.condor_off_command)
            #args.append('-peaceful')
            #args.append('-name')
            #args.append(machine_name)
            #args.append('-subsystem')
            #args.append('startd')
            
            args2.append(config.condor_off_command)
            args2.append('-peaceful')
            args2.append('-addr')
            args2.append(machine_addr)
            args2.append('-subsystem')
            args2.append('startd')
            
            args3.append(config.condor_off_command)
            args3.append('-peaceful')
            args3.append('-addr')
            args3.append(master_addr)
            args3.append('-subsystem')
            args3.append('master')
        # Send condor_off to startd first
        try:
            log.debug(" ".join(args2))
            sp1 = subprocess.Popen(args2, shell=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not utilities.check_popen_timeout(sp1):
                (out, err) = sp1.communicate(input=None)
            ret1 = -1
            if out.startswith("Sent"):
                ret1 = 0
            if sp1.returncode == 0 and ret1 == 0:
                log.debug("Successfuly sent condor_off startd to %s" % (machine_name))
            else:
                log.debug("Failed to send condor_off startd to %s: Reason: %s. Err: %s" % (machine_name, out, err))
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (' '.join(args2), e.errno, e.strerror))
            return (-1, -1, -1, -1)
        except:
            log.error("Problem running %s, unexpected error" % ' '.join(args2))
            return (-1, -1, -1, -1)
        # Now send the master off
        try:
            log.debug(" ".join(args3))
            sp2 = subprocess.Popen(args3, shell=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not utilities.check_popen_timeout(sp2):
                (out, err) = sp2.communicate(input=None)
            ret2 = -1
            if out.startswith("Sent"):
                ret2 = 0
            if sp2.returncode == 0 and ret2 == 0:
                log.debug("Successfuly sent condor_off master to %s" % (machine_name))
            else:
                log.debug("Failed to send condor_off master to %s : Reason: %s : Error: %s" % (machine_name, out, err))
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (' '.join(args3), e.errno, e.strerror))
            return (-1, -1, -1, -1)
        except:
            log.error("Problem running %s, unexpected error" % ' '.join(args3))
            return (-1, -1, -1, -1)
        return (sp1.returncode, ret1, sp2.returncode, ret2)

    def do_condor_advertise_master(self, target_file):
        """Perform a condor_advertise INVALIDATE_MASTER_ADS on condor pool.

        Attempts to remove a bad classad from the condor pool to improve job scheduling

        Keywords:
            target_file - path of the file containing the correct format for the classads to invalidate
        Return:
            a tuple of the returncodes from the command used and a return code
        """
        log.debug("cloud_management.py::do_advertise_master - target_file: %s" % (target_file))

        cmd = '%s INVALIDATE_MASTER_ADS "%s"' % (config.condor_advertise_command, target_file)
        args = []

        if target_file == None:
            log.error("No target_file specified, cannot perform condor_advertise INVALIDATE_MASTER_ADS")
            return (-1,-1)
        if config.cloudscheduler_ssh_key:
            central_address = re.search('(?<=http://)(.*):', config.condor_webservice_url).group(1)
            args.append(config.ssh_path)
            args.append('-i')
            args.append(config.cloudscheduler_ssh_key)
            args.append(central_address)
            args.append(cmd)
        else:
            args.append(config.condor_advertise_command)
            args.append('INVALIDATE_MASTER_ADS')
            args.append(target_file)
        try:
            log.debug(" ".join(args))
            sp1 = subprocess.Popen(args, shell=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not utilities.check_popen_timeout(sp1):
                (out, err) = sp1.communicate(input=None)
            ret1 = -1
            if out.startswith("Sent"):
                ret1 = 0
            if sp1.returncode == 0:
                log.verbose("Successfuly sent condor_advertise invalidate_master_ads %s" % (target_file))
            else:
                log.debug("Failed to send condor_advertise invalidate_master_ads %s: Reason: %s. Err: %s" % (target_file, out, err))
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (' '.join(args), e.errno, e.strerror))
            return (-1, -1)
        except:
            log.error("Problem running %s, unexpected error" % ' '.join(args))
            return (-1, -1)

        return (sp1.returncode, ret1)

    def do_condor_advertise_startd(self, target_file):
        """Perform a condor_advertise INVALIDATE_STARTD_ADS on condor pool.

        Attempts to remove a bad classad from the condor pool to improve job scheduling

        Keywords:
            target_file - path of the file containing the correct format for the classads to invalidate
        Return:
            a tuple of the returncodes from the command used and a return code
        """
        log.debug("cloud_management.py::do_advertise_startd - target_file: %s" % (target_file))

        cmd = '%s INVALIDATE_STARTD_ADS "%s"' % (config.condor_advertise_command, target_file)
        args = []

        if target_file == None:
            log.error("No target_file specified, cannot perform condor_advertise INVALIDATE_STARTD_ADS")
            return (-1,-1)
        if config.cloudscheduler_ssh_key:
            central_address = re.search('(?<=http://)(.*):', config.condor_webservice_url).group(1)
            args.append(config.ssh_path)
            args.append('-i')
            args.append(config.cloudscheduler_ssh_key)
            args.append(central_address)
            args.append(cmd)
        else:
            args.append(config.condor_advertise_command)
            args.append('INVALIDATE_STARTD_ADS')
            args.append(target_file)
        try:
            log.debug(" ".join(args))
            sp1 = subprocess.Popen(args, shell=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not utilities.check_popen_timeout(sp1):
                (out, err) = sp1.communicate(input=None)
            ret1 = -1
            if out.startswith("Sent"):
                ret1 = 0
            if sp1.returncode == 0:
                log.verbose("Successfuly sent condor_advertise invalidate_startd_ads %s" % (target_file))
            else:
                log.debug("Failed to send condor_advertise invalidate_startd_ads %s: Reason: %s. Err: %s" % (target_file, out, err))
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (' '.join(args), e.errno, e.strerror))
            return (-1, -1)
        except:
            log.error("Problem running %s, unexpected error" % ' '.join(args))
            return (-1, -1)

        return (sp1.returncode, ret1)

    def create_condor_advertise_target_file(self, names=[]):
        """Creates a file with the correct format for condor_advertise to remove classads
        File will contain:
        MyType="Query"
        TargetType="Machine"
        Requirements=Name==<name of classad>

        multiple can be specified by separating with a line

        :return: path of the file created.
        """
        output = []
        for name in names:
            contents = """MyType="Query"\nTargetType="Machine"\nRequirements=Name=="%s"\n""" % name
            output.append(contents)
        strout = '\n'.join(output)
        (fd, filepath) = tempfile.mkstemp(suffix='.cs', text=True)
        try:
            os.write(fd, strout)
            os.close(fd)
        except Exception as e:
            log.error("Problem writing to temp file: %s Exception: %s" % (filepath, e))
        return filepath

    def find_vm_with_name(self, condor_name):
        """Find a VM in cloudscheduler with the given condor machine name(hostname)."""
        foundIt = False
        vm_match = None
        if len(condor_name.split('@')) > 1:
            condor_name = condor_name.split('@')[1]
        for cluster in self.resources:
            for vm in cluster.vms:
                if utilities.match_host_with_condor_host(vm.hostname, condor_name) or utilities.match_host_with_condor_host(vm.alt_hostname, condor_name) or \
                  utilities.match_host_with_condor_host(vm.condormasteraddr, condor_name) or utilities.match_host_with_condor_host(vm.condorname, condor_name):
                    foundIt = True
                    vm_match = vm
                    break
            if foundIt:
                break
        if not foundIt:
            log.verbose("Could not find a VM with name: %s, checking retired_resources." % condor_name)
            for cluster in self.retired_resources:
                for vm in cluster.vms:
                    if utilities.match_host_with_condor_host(vm.condorname, condor_name) or utilities.match_host_with_condor_host(vm.hostname, condor_name) or \
                      utilities.match_host_with_condor_host(vm.condormasteraddr, condor_name) or utilities.match_host_with_condor_host(vm.alt_hostname, condor_name):
                        foundIt = True
                        vm_match = vm
                        break
                if foundIt:
                    break
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
        if not foundIt:
            for cluster in self.retired_resources:
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
                    if vm.status == "Starting" or vm.status == "Unpropagated":
                        starting.append(vm)
        return starting
    
    def get_num_starting_vms(self):
        """Count the number of starting state VMs."""
        num_starting = 0
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.status == "Starting" or vm.status == "Unpropagated":
                    num_starting += 1
        log.verbose("There are %i Starting VMs, the max_starting_vm is %i." % (num_starting, config.max_starting_vm))
        return num_starting

    def get_starting_of_usertype(self, vmtype):
        """Get a list of the VMs in the Starting state of the given usertype."""
        starting = []
        for cluster in self.resources:
            for vm in cluster.vms:
                if vm.uservmtype == vmtype:
                    if vm.status == "Starting" or vm.status == "Unpropagated":
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
                if item[0] in ['password', 'access_key_id', 'secret_access_key', 'username']:
                    continue
                outputlist.append('[')
                outputlist.append(','.join(item))
                outputlist.append(']')
            outputlist.append('\n')
        return "".join(outputlist)

    def shutdown_cluster_number(self, cloudname, number):
        try:
            number = int(number)
        except:
            return "Unable to shutdown %s VMs, use a number." % number
        
        output = ""
        cluster = self.get_cluster(cloudname.lower())
        desthreads = []
        if cluster:
            if number > len(cluster.vms):
                number = len(cluster.vms)
            for x in range(0, number):
                thread = VMDestroyCmd(cluster, cluster.vms[x], reason="Shutdown request from admin client.")
                desthreads.append(thread)
            for thread in desthreads:
                thread.start()
            while len(desthreads) > 0:
                while desthreads[-1].is_alive():
                    time.sleep(1)
                if not desthreads[-1].is_alive():
                    if desthreads[-1].get_result() != 0:
                        output += "Destroying VM %s failed. Leaving it for now.\n" % desthreads[-1].get_vm().id
                    else:
                        output += "VM %s has been Destroyed.\n" % thread.get_vm().id
                    desthreads[-1].join()
                    desthreads.pop()
        else:
            output = "Could not find cloud named: %s" % cloudname
        return output

    def shutdown_cluster_vm(self, clustername, vmid):
        """Manually shutdown a VM, for use by cloud_admin."""
        output = ""
        cluster = self.get_cluster(clustername.lower())
        cluster_retired = self.get_cluster(clustername.lower(), True)
        if cluster:
            vm = cluster.get_vm(vmid)
            if cluster_retired:
                vm_retired = cluster_retired.get_vm(vmid)
            if vm:
                # found the vm - shutdown
                # move the vmdestroycmd thread into a better place and import so avilable here
                self._shutdown_admin(cluster, vm)
            elif cluster_retired and vm_retired:
                self._shutdown_admin(cluster_retired, vm_retired)
            else:
                output = "Could not find VM with ID: %s on Cluster: %s." % (vmid, clustername)
        else:
            output = "Could not find a Cluster with name: %s." % clustername
        return output

    @staticmethod
    def _shutdown_admin(cluster, vm):
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


    def shutdown_cluster_all(self, clustername):
        """Manually shutdown all VMs on a cluster, for use by cloud_admin."""
        output = ""
        vmdesth = {}
        cluster = self.get_cluster(clustername.lower())
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
        cluster = self.get_cluster(clustername.lower())
        cluster_retired = self.get_cluster(clustername.lower(), True)
        if cluster:
            vm = cluster.get_vm(vmid)
            if cluster_retired:
                vm_retired = cluster_retired.get_vm(vmid)
            if vm:
                with cluster.vms_lock:
                    cluster.vms.remove(vm)
                    log.debug("VM: %s, on %s removed from list." % (vm.id, vm.clusteraddr))
                cluster.resource_return(vm)
                output = "Removed %s's VM %s from CloudScheduler." % (clustername, vmid)
                log.debug(output)
            elif cluster_retired and vm_retired:
                with cluster_retired.vms_lock:
                    cluster_retired.vms.remove(vm)
                    log.debug("VM: %s, on %s removed from list." % (vm.id, vm.clusteraddr))
                output = "Removed %s's VM %s from CloudScheduler retired resources." % (clustername, vmid)
                log.debug(output)
            else:
                output = "Could not find VM ID: %s on Cloud: %s" % (vmid, clustername)
        else:
            output = "Could not find Cloud %s." % clustername
        return output

    def remove_all_vmcloud_no_shutdown(self, clustername):
        """Remove all VM entries from a cluster without issuing shutdowns to the IaaS, for use by cloud_admin."""
        cluster = self.get_cluster(clustername.lower())
        output = ""
        if cluster:
            for vm in reversed(cluster.vms):
                with cluster.vms_lock:
                    cluster.vms.remove(vm)
                    log.debug("VM: %s, on %s removed from list." % (vm.id, vm.clusteraddr))
                cluster.resource_return(vm)
            output = "Removed all VMs from %s." % clustername
            log.debug(output)
        else:
            output = "Could not find Cloud %s." % clustername
        return output
    
    def force_retire_vm(self, vm):
        ret = False
        if vm:
            (_, ret2, _, ret22) = self.do_condor_off(vm.condorname, vm.condoraddr, vm.condormasteraddr)
            if ret2 == 0 and ret22 == 0:
                vm.force_retire = True
                vm.override_status = 'Retiring'
                ret = True
        return ret

    def force_retire_cluster_vm(self, clustername, vmid):
        output = ""
        cluster = self.get_cluster(clustername)
        cluster_retired = self.get_cluster(clustername, True)
        if cluster:
            vm = cluster.get_vm(vmid)
            if vm:
                if self.force_retire_vm(vm):
                    output = "Retired VM %s on %s." % (vmid, clustername)
                    log.debug(output)
                else:
                    output = "Unable to retire VM."
            else:
                if cluster_retired:
                    vm = cluster_retired.get_vm(vmid)
                    if vm:
                        if self.force_retire_vm(vm):
                            output = "Retired VM %s on %s." % (vmid, clustername)
                            log.debug(output)
                        else:
                            output = "Unable to retire VM."
                    else:
                        output = "Could not find VM ID %s." % vmid
        else:
            output = "Could not find Cloud %s." % clustername
        return output
    
    def force_retire_cluster_all(self, cloudname):
        cluster = self.get_cluster(cloudname.lower())
        output = ""
        if cluster:
            for vm in cluster.vms:
                if self.force_retire_vm(vm):
                    pass
                else:
                    output += "Unable to retire VM %s\n" % vm.id
            output = "Retired all VMs in %s." % cloudname
            log.debug(output)
        else:
            output = "Cloud not find Cloud %s." % cloudname
        return output

    def force_retire_cluster_number(self, cloudname, number):
        cluster = self.get_cluster(cloudname.lower())
        output = ""
        try:
            number = int(number)
        except:
            return "Unable to retire %s VMs, use a number" % number
        if cluster:
            if number > len(cluster.vms):
                number = len(cluster.vms)
            for vm in cluster.vms[:number]:
                if self.force_retire_vm(vm):
                    pass
                else:
                    output += "Unable to retire VM %s\n" % vm.id
            output = "Retired %s VM(s) in %s." % (str(number), cloudname)
            log.debug(output)
        else:
            output = "Cloud not find Cloud %s." % cloudname
        return output

    def disable_cluster(self, clustername):
        """Toggles the enabled flag for a cluster, for use by cloud_admin."""
        cluster = self.get_cluster(clustername.lower())
        ret = ""
        if cluster:
            cluster.enabled = False
            ret = "Cloud: %s disabled." % clustername
            log.debug(ret)
        else:
            ret = "Could not find cloud %s." % clustername
        return ret
    
    def enable_cluster(self, clustername):
        """Toggles the enabled flag for a cluster, for use by cloud_admin."""
        cluster = self.get_cluster(clustername.lower())
        ret = ""
        if cluster:
            cluster.enabled = True
            ret = "Cloud: %s enabled." % clustername
            log.debug(ret)
        else:
            ret = "Could not find cloud %s." % clustername
        return ret

    def reset_override_state(self, clustername, vmid):
        output = ""
        cluster = self.get_cluster(clustername.lower())
        if cluster:
            vm = cluster.get_vm(vmid)
            if vm:
                vm.override_status = None
                vm.force_retire = False
                output = "Reset state of %s on %s" % (clustername, vmid)
                log.debug(output)
            else:
                output = "Could not find VM ID %s." % vmid
        else:
            output = "Could not find Cloud %s." % clustername
        return output
    
    def fetch_missing_vm_list(self):
        """Report missing_vm_condor_machines list to cloud_admin."""
        log.debug("Fetching list of Condor Entries with no match in CS.")
        output = "List of Condor Entries with no match in Cloud Scheduler(host:startd):\n"
        for machine in self.missing_vm_condor_machines:
            output += ' : '.join([machine.machine_name, machine.address_startd])
            output += '\n'
        return output

    def user_at_limit(self, user):
        """Check if a user has met their throttled limit."""
        count = self.get_vm_count_user(user)
        limit = False
        if user in self.user_vm_limits:
            if not (count < self.user_vm_limits[user]):
                limit = True
        return limit

    def uservmtype_at_limit(self, uservmtype, limit):
        """Check if a vmusertype has met it's limit."""
        atLimit = False
        counts = self.get_vmtypes_count_internal()
        if limit != -1 and uservmtype in counts.keys() and not (counts[uservmtype] < limit):
            atLimit = True
        return atLimit

    def machinelist_to_vmmachinelist(self, machinelist, master_machinelist):
        vm_machine_list = []
        master_machine_ips = {}
        for master in master_machinelist:
            try:
                master_machine_ips[master['Machine']] = master['MasterIpAddr']
            except:
                log.warning('could not read master ip addr')
        for machine in machinelist:
            try:
                name = machine_name = job_id = global_job_id = address_startd = \
                     address_master = state = activity = vmtype = start_req = \
                     remote_owner = slot_type = total_slots = ""
                current_time = entered_state_time = -1
                if machine.has_key('Name'):
                    name = machine['Name']
                if machine.has_key('Machine'):
                    machine_name = machine['Machine']
                if machine.has_key('JobId'):
                    job_id = machine['JobId']
                if machine.has_key('GlobalJobId'):
                    global_job_id = machine['GlobalJobId']
                if machine.has_key('MyAddress'):
                    address_startd = machine['MyAddress']
                if machine.has_key('State'):
                    state = machine['State']
                if machine.has_key('Activity'):
                    activity = machine['Activity']
                if machine.has_key('VMType'):
                    vmtype = machine['VMType']
                if machine.has_key('MyCurrentTime'):
                    current_time = machine['MyCurrentTime']
                if machine.has_key('EnteredCurrentState'):
                    entered_state_time = machine['EnteredCurrentState']
                if machine.has_key('Start'):
                    start_req = machine['Start']
                if machine.has_key('RemoteOwner'):
                    remote_owner = machine['RemoteOwner']
                if master_machine_ips.has_key(machine['Machine']):
                    address_master = master_machine_ips[machine['Machine']]
                if machine.has_key('SlotType'):
                    slot_type = machine['SlotType']
                if machine.has_key('TotalSlots'):
                    total_slots = machine['TotalSlots']
                vmmachine = VMMachine(name=name, machine_name=machine_name, job_id=job_id, global_job_id=global_job_id,
                 address_startd=address_startd, address_master=address_master, state=state, activity=activity,
                 vmtype=vmtype, current_time=current_time, entered_state_time=entered_state_time,
                 start_req=start_req, remote_owner=remote_owner, slot_type=slot_type, total_slots=total_slots)
                vm_machine_list.append(vmmachine)
            except:
                log.warning("Failed to create VMMachine Obj")
        return vm_machine_list

    def resolve_target_cloud_alias(self, targets):
        expanded_targets = []
        for cloud in targets:
            if cloud in self.target_cloud_aliases.keys():
                expanded_targets.extend(self.target_cloud_aliases[cloud])
            else:
                expanded_targets.append(cloud)
        trimmed_targets = list(set(expanded_targets))
        return trimmed_targets
    
    def resolve_vmami_cloud_alias(self, vmamis=None):
        expanded_amis = {}
        for k, v in vmamis.iteritems():
            if k in self.target_cloud_aliases.keys():
                exp = self.target_cloud_aliases[k]
                for cloud in exp:
                    expanded_amis[cloud.lower()] = v
            else:
                expanded_amis[k.lower()] = v
        return expanded_amis

    def resolve_vminstancetype_cloud_alias(self, vminstancetypes=None):
        expanded_types = {}
        for k, v in vminstancetypes.iteritems():
            if k in self.target_cloud_aliases.keys():
                exp = self.target_cloud_aliases[k]
                for cloud in exp:
                    expanded_types[cloud.lower()] = v
            else:
                expanded_types[k.lower()] = v
        return expanded_types

    def adjust_cloud_allocation(self, cloud_name, number):
        try:
            number = int(number)
        except:
            return "Need to use an integer value for vm_allocation"
        cluster = self.get_cluster(cloud_name.lower())
        if cluster:
            with(cluster.res_lock):
                # Determine current vm slot value - remaining+current vms
                total_slots = cluster.vm_slots + len(cluster.vms)
                # check if that is less than or greater than the new value
                # if current < new  bump up the remaining with difference
                if number > total_slots:
                    cluster.vm_slots += (number - total_slots)
                    log.info("Increasing quota on %s to %s." % (cluster.name, number))
                # if less need to subtract from remaining if remaining goes under 0, need to force retire 
                # and move the excess VMs into the extra retiring area so their resources won't be returned.
                elif number < total_slots:
                    num_remove = total_slots - number
                    # take it from vm_slots first
                    if num_remove > cluster.vm_slots:
                        # set slots to 0 and retire any remainder
                        remainder = num_remove - cluster.vm_slots
                        cluster.vm_slots = 0
                        # now force retire remaider VMs removing them from cluster.vms
                        for _ in range(0, remainder):
                            try:
                                vm = cluster.vms.pop()
                            except IndexError:
                                log.error("VM list empty on cloud: %s. Can't retire any more." % cluster.name)
                                break
                            if cluster not in self.retired_resources:
                                cluster_copy = copy.deepcopy(cluster)
                                cluster_copy.vms = []
                                cluster_copy.vms.append(vm)
                                self.retired_resources.append(cluster_copy)
                            else:
                                cluster_copy = self.get_cluster(cluster.name, True)
                                cluster_copy.vms.append(vm)
                            vm.return_resources = False
                            self.force_retire_vm(vm)
                    else:
                        # we have free space in vm_slots
                        cluster.vm_slots -= num_remove
                        log.info("Reducing quota on %s to %s using spare slots " % (cluster.name,number))
                        pass
                else:
                    return "VM slots already set at %s. Nothing to do." % total_slots
        else:
            return "Unable to find cluster with name: %s" % cloud_name

        self.update_cloud_resources(cluster.name, number)
        return "Attempt adjustment on cloud %s, change to %s slots" % (cloud_name, number)


    def update_cloud_resources(self, cloud, slots):
        try:
            cloud_config = ConfigParser.ConfigParser()
            cloud_config.read(self.config_file)
        except ConfigParser.ParsingError:
            log.exception("Cloud config problem: Couldn't " \
                  "parse your cloud config file. Check for spaces " \
                  "before or after variables.")
            sys.exit(1)
        cloud_config.set(cloud, "vm_slots", slots)
        with open(self.config_file, "wb") as cf:
            cloud_config.write(cf)


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
        self.init_time = time.time()
    def run(self):
        self.result = self.cluster.vm_destroy(self.vm, reason=self.reason)
        if self.result != 0:
            log.error("Failed to destroy vm %s on %s" % (self.vm.id, self.vm.clusteraddr))
    def get_result(self):
        return self.result
    def get_vm(self):
        return self.vm

#class VMDestroyCmd(multiprocessing.Process):
    """
    VMCmd - passing shutdown and destroy requests to a separate thread 
    """

#    def __init__(self, cluster, vm, reason=""):
#        multiprocessing.Process.__init__(self, name=self.__class__.__name__)
#        self.cluster = cluster
#        self.vm = vm
#        self.result = None
#        self.reason = reason
#        self.init_time = time.time()
#        print "Created a vmdestroy process"
#    def run(self):
#        print 'exec the vm_destroy call'
#        self.result = self.cluster.vm_destroy(self.vm, reason=self.reason)
#        if self.result != 0:
#            log.error("Failed to destroy vm %s on %s" % (self.vm.id, self.vm.clusteraddr))
#       print 'done destroy'
#    def get_result(self):
#       return self.result
#    def get_vm(self):
#        return self.vm


class VMMachine():
    """
    VMMachine - abstraction class to hold information about machines registered with the batch queue
    
    name - Full Name of the VM registered i.e. 'slot1@localhost'
    machine_name - the hostname of the VM registered i.e. 'localhost'
    job_id - ID of job running on the machine
    global_job_id - global job ID of job running on machine
    address_startd - address of condor startd of this machine
    address_master - address of condor master of this machine
    state - state of machine in condor this is typically Claimed, Unclaimed
    activity - activity of machine, in condor this is Busy, Idle, Retiring
    vmtype - the vmtype of the machine
    current_time - last time the machine info was updated
    entered_state_time - time that machine entered the current state/activity
    start_req - the Start expression of the machine in condor
    remote_owner - the user running jobs on the machine
    """

    def __init__(self, name="", machine_name="", job_id="", global_job_id="",
                 address_startd="", address_master="", state="", activity="",
                 vmtype="", current_time=0, entered_state_time=0, start_req="",
                 remote_owner="", slot_type="", total_slots = ""):
        self.name = name
        self.machine_name = machine_name
        self.job_id = job_id
        self.global_job_id = global_job_id
        self.address_startd = address_startd
        self.address_master = address_master
        self.state = state
        self.activity = activity
        self.vmtype = vmtype
        self.current_time = current_time
        self.entered_state_time = entered_state_time
        self.start_req = start_req
        self.remote_owner = remote_owner
        self.slot_type = slot_type
        self.total_slots = total_slots


    def get_uservmtype(self):
        return ''.join([self.remote_owner, self.vmtype])


    def __repr__(self):
        return "MachineName: %s, State: %s, Activity: %s, VMType: %s, SlotType: %s, TotalSlots: %s" % (self.machine_name, self.state, self.activity, self.vmtype, self.slot_type, self.total_slots)
