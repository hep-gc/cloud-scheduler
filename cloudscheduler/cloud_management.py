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

from subprocess import Popen
import subprocess
import datetime
import sys
import os
import sys
import logging
import string
import re
import nimbus_xml
import ConfigParser
import cluster_tools

##
## GLOBALS
##

##
## LOGGING
##

# Create a python logger
log = logging.getLogger("CloudLogger")

##
## CLASSES
##

# A class for storing created VM information. Used to populate Cluster classes
# 'vms' lists.

class VM:

    ## Instance Variables

    # The global VM states are:
    #    Starting - The VM is being created in the cloud
    #    Running  - The VM is running somewhere on the cloud (fully functional)
    #    Error    - The VM has been corrupted or is in the process of being destroyed
    # For a full state diagram, refer to the following development wiki page:
    # TODO: Add state dia. to wiki
    # States are defined in each Cluster subclass, in which a VM_STATES dictionary
    # maps specific cloud software state to these global states.

    ## Instance Methods

    # Constructor
    # name         - (str) The name of the vm (arbitrary)
    # id           - (str) The id tag for the VM. Whatever is used to access the vm
    #                by cloud software (Nimbus: epr file. OpenNebula: id number, etc.)
    # vmtype       - (str) The condor VMType attribute for the VM
    # clusteraddr  - (str) The address of the cluster hosting the VM
    # cloudtype   - (str) The cloud type of the VM (Nimbus, OpenNebula, etc)
    # network      - (str) The network association the VM uses
    # cpuarch      - (str) The required CPU architecture of the VM
    # imagelocation- (str) The location of the image from which the VM was created
    # memory       - (int) The memory used by the VM
    # mementry     - (int) The index of the entry in the host cluster's memory list
    #                from which this VM is taking memory
    def __init__(self, name="default_VM", id="default_VMID", vmtype="default_VMType", 
            clusteraddr="default_hostname", cloudtype="def_cloudtype", network="public",
            cpuarch="x86", imagelocation="default_imageloc", memory=0, mementry=0,
            cpucores=0, storage=0):
        self.name = name
        self.id = id
        self.vmtype = vmtype
        self.clusteraddr = clusteraddr
        self.cloudtype = cloudtype
        self.network = network
        self.cpuarch = cpuarch
        self.imagelocation = imagelocation
        self.memory = memory
        self.mementry = mementry
        self.cpucores = cpucores
        self.storage = storage

        # Set a status variable on new creation
        self.status = "Starting"
        
        log.debug("New VM object created:")
        log.debug("VM - Name: %s, id: %s, host: %s, image: %s, memory: %d" \
          % (name, id, clusteraddr, imagelocation, memory))

    def log(self):
        log.debug("VM Name: %s, ID: %s, Type: %s, Status: %s on %s" \
           % (self.name, self.id, self.vmtype, self.status, self.clusteraddr))

    def get_vm_info(self):
        return "VM Name: %s, ID: %s, Type: %s, Status: %s on %s" \
            % (self.name, self.id, self.vmtype, self.status, self.clusteraddr)


# A class that stores and organises a list of Cluster resources

class ResourcePool:

    ## Instance variables
    resources = []

    ## Instance methods

    # Constructor
    # name   - The name of the ResourcePool being created
    def __init__(self, name):
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
    # Return cluster that matches cluster_name 
    def get_cluster(self, cluster_name):
        for cluster in self.resources:
            if cluster.name == cluster_name:
                return cluster
        return None

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
    #    TODO: storage   - the amount of scratch space the VM requires
    #    TODO: cpucores  - the number of cores that a VM requires (dedicated? or general?)
    # Return: returns a Cluster object if one is found that vits VM requirments
    #         Otherwise, returns the 'None' object
    def get_resourceFF(self, network, cpuarch, memory):
        if len(self.resources) == 0:
            log.debug("Pool is empty... Cannot return FF resource")
            return None

        for cluster in self.resources:
            # If the cluster has no open VM slots
            if (cluster.vm_slots <= 0):
                continue
            # If the cluster has no sufficient memory entries for the VM
            if (cluster.find_mementry(memory) < 0):
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
    # Return: returns a Cluster object if one is found that fits VM requirments
    #         Otherwise, returns the 'None' object
    def get_resourceBF(self, network, cpuarch, memory, cpucores, storage):

        # Get a list of fitting clusters
        fitting_clusters = self.get_fitting_resources(network, cpuarch, memory, cpucores, storage)

        # If list is empty (no resources fit), return None
        if len(fitting_clusters) == 0:
            log.debug("No clusters fit requirements. Fitting resources list is empty.")
            return None

        # Iterate through fitting clusters - save "most balanced" cluster. (LINEAR search)
        # Note: mb_cluster stands for "most balanced cluster"
        mb_cluster = fitting_clusters.pop()
        mb_cluster_vms = mb_cluster.num_vms()
        for cluster in fitting_clusters:
            # If considered cluster has fewer running VMs, set it as the most balanced cluster
            if (cluster.num_vms() < mb_cluster_vms):
                mb_cluster = cluster
                mb_cluster_vms = cluster.num_vms()

        # Return the most balanced cluster after considering all fitting clusters.
        return mb_cluster


# The Cluster superclass, containing all general cluster instance variables
# and Cluster interface methods (stubs for implementation in subclasses).

class Cluster:
   
    
    ## Instance methods

    # Constructor
    def __init__(self, name="Dummy Cluster", host="localhost", cloud_type="Dummy",
                 memory=[], cpu_archs=[], networks=[], vm_slots=0,
                 cpu_cores=0, storage=0):
        self.name = name
        self.network_address = host
        self.cloud_type = cloud_type
        self.memory = memory
        self.cpu_archs = cpu_archs
        self.network_pools = networks
        self.vm_slots = vm_slots
        self.cpu_cores = cpu_cores
        self.storageGB = storage
        self.vms = [] # List of running VMs

        log.debug("New cluster %s created" % self.name)

        
    # Print cluster information
    def log_cluster(self):
        log.debug("-" * 30 + 
            "Name:\t\t%s\n"        % self.name +
            "Address:\t%s\n"       % self.network_address +
            "Type:\t\t%s\n"        % self.cloud_type +
            "VM Slots:\t%s\n"      % self.vm_slots +
            "CPU Cores:\t%s\n"     % self.cpu_cores +
            "Storage:\t%s\n"       % self.storageGB +
            "Memory:\t\t%s\n"      % self.memory +
            "CPU Archs:\t%s\n"     % string.join(self.cpu_archs, ", ") +
            "Network Pools:\t%s\n" % string.join(self.network_pools, ", ") +
            "-" * 30)
    
    # Print a short form of cluster information
    def log(self):
        log.debug("CLUSTER Name: %s, Address: %s, Type: %s, VM slots: %d, Mem: %s" \
          % (self.name, self.network_address, self.cloud_type, self.vm_slots, \
          self.memory))
    # Return a short form of cluster information
    def get_cluster_info_short(self):
        return "CLUSTER Name: %s, Address: %s, Type: %s, VM slots: %d, Mem: %s" \
          % (self.name, self.network_address, self.cloud_type, self.vm_slots, self.memory)
    # Print the cluster 'vms' list (via VM print)
    def log_vms(self):
        if len(self.vms) == 0:
            log.debug("CLUSTER %s has no running VMs..." % (self.name))
        else:
            log.debug("CLUSTER %s running VMs:" % (self.name))
            for vm in self.vms:
                vm.log_short("\t")
    # Return information about running VMs on Cluster
    def get_cluster_vms_info(self):
        if len(self.vms) == 0:
            return "CLUSTER %s has no running VMs..." % (self.name)
        else:
            output = "CLUSTER %s running VMs:" % (self.name)
            for vm in self.vms:
                output += "\n" + vm.get_vm_info()
            return output
    # Get VM with id 
    def get_vm(self, vm_id):
        for vm in self.vms:
            if vm_id == vm.id:
                return vm
        return None
    # VM manipulation methods
    #-!------------------------------------------------------------------------
    # NOTE: In implementing subclasses of Cluster, the following method prototypes
    #       should be used (standardize on these parameters)
    #-!------------------------------------------------------------------------

    # Note: vm_id is the identifier for a VM, used to query or change an already
    #       created VM. vm_id will be a different entity based on the subclass's
    #       cloud software. EG:
    #       - Nimbus vm_ids are epr files
    #       - OpenNebula (and Eucalyptus?) vm_ids are names/numbers
    # TODO: Explain all params
    
    def vm_create(self, vm_name, vm_type, vm_networkassoc, vm_cpuarch, 
            vm_imagelocation, vm_mem, vm_cores, vm_storage):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_create'

    def vm_recreate(self, vm):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_recreate'
    
    def vm_reboot(self, vm):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_reboot'
    
    def vm_destroy(self, vm):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_destroy'
    
    def vm_poll(self, vm):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_poll'


    ## Private VM methods
    
    # Finds a memory entry in the Cluster's 'memory' list which supports the
    # requested amount of memory for the VM. If multiple memory entries fit
    # the request, returns the first suitable entry. Returns an exact fit if
    # one exists.
    # Parameters: memory - the memory required for VM creation
    # Return: The index of the first fitting entry in the Cluster's 'memory'
    #         list.
    #         If no fitting memory entries are found, returns -1 (error!)
    def find_mementry(self, memory):
        # Check for exact fit
        if (memory in self.memory):
           return self.memory.index(memory)
        
        # Scan for any fit
        for i in range(len(self.memory)):
           if self.memory[i] >= memory:
               return i
        
        # If no entries found, return error code. 
        return(-1)

    # Checks out resources taken by a VM in creation from the internal rep-
    # resentation of the Cluster
    # Parameters:
    #    vm   - the VM object used to check out resources from the Cluster.
    #           The VMs memory and mementry fields are used to check out memory
    #           from the appropriate Cluster fields.
    # Note: No bounds checking is done as of yet.
    # Note: vm_slots is automatically decremeneted by one (1).
    # EXPAND HERE as checkout/return become more complex
    def resource_checkout(self, vm):
        self.vm_slots -= 1
        # NOTE: Currently, memory checking out is not supported
        # ISSUE: No way to know what mementry a VM is running on
        # self.memory[vm.mementry] -= vm.memory

    # Returns the resources taken by the passed in VM to the Cluster's internal
    # storage.
    # Parameters: (as for checkout() )
    # Notes: (as for checkout)
    def resource_return(self, vm):
        self.vm_slots += 1
        # NOTE: Currently, memory checking out is not supported
        # ISSUE: No way to know what mementry a VM is running on
        # self.memory[vm.mementry] += vm.memory       


## Implements cloud management functionality with the Nimbus service as part of
## the Globus Toolkit.

class NimbusCluster(Cluster):

    ## NimbusCluster specific instance variables
    
    # Nimbus global state finding regexp (parsing Poll output)
    STATE_RE = "State:\s(\w*)$"

    # Global Nimbus command variables
    VM_DURATION = "1000"
    VM_TARGETSTATE = "Running"
   
    # A dictionary mapping Nimbus states to global states (see VM class comments
    # for the global state information)
    # Nimbus VM states: Unstaged, Unpropagated, Propagated, Running, Paused, 
    # TransportReady, StagedOut, Corrupted, Cancelled.
    VM_STATES = {
         "Unstaged"       : "Starting",
         "Unpropagated"   : "Starting",
         "Propagated"     : "Starting",
         "Running"        : "Running",
         "Paused"         : "Running",   
         "TransportReady" : "Running",
         "StagedOut"      : "Running",
         "Corrupted"      : "Error",
         "Cancelled"      : "Error",
    }
    
    
    # TODO: Explain parameters and returns
    def vm_create(self, vm_name, vm_type, vm_networkassoc, vm_cpuarch,
            vm_imagelocation, vm_mem, vm_cores, vm_storage):
        
        log.debug("Nimbus cloud create command")

        # Create a workspace metadata xml file from passed parameters
        vm_metadata = nimbus_xml.ws_metadata_factory(vm_name, vm_networkassoc, \
          vm_cpuarch, vm_imagelocation)
        
        # Set a timestamp for VM creation
        now = datetime.datetime.now()
        
        # Create an EPR file name (unique with timestamp)
        vm_epr = "nimbusVM_" + now.isoformat() + ".epr"

        # Create the workspace command as a list (private method)
        ws_cmd = self.vmcreate_factory(vm_epr, vm_metadata, self.VM_DURATION, vm_mem, \
          self.VM_TARGETSTATE)
        log.debug("vm_create - workspace create command prepared.")
        log.debug("vm_create - Command: " + string.join(ws_cmd, " "))

        # Execute the workspace create command: returns immediately.
        create_return = self.vm_execute(ws_cmd)
        if (create_return != 0):
            log.debug("vm_create - Error in executing workspace create command.")
            log.debug("vm_create - VM %s (ID: %s) not created. Returning error code." \
              % (vm_name, vm_epr))
            return create_return
        log.debug("(vm_create) - workspace create command executed.")

        # Find the memory entry in the Cluster 'memory' list which _create will be 
        # subtracted from
        # NOTE: currently obsolete, as memory checkout is disabled (8/03/2009)
        vm_mementry = self.find_mementry(vm_mem)
        if (vm_mementry < 0):
            # At this point, there should always be a valid mementry, as the ResourcePool
            # get_resource methods have selected this cluster based on having an open 
            # memory entry that fits VM requirements.
            log.debug("(vm_create) - Cluster memory list has no sufficient memory " +\
              "entries (Not supposed to happen). Returning error.")
            return (1)
        log.debug("(vm_create) - vm_create - Memory entry found in given cluster: %d" % vm_mementry)
        
        # Create a VM object to represent the newly created VM
        new_vm = VM(name = vm_name, id = vm_epr, vmtype = vm_type, 
            clusteraddr = self.network_address, cloudtype = self.cloud_type, 
            network = vm_networkassoc, cpuarch = vm_cpuarch, 
            imagelocation = vm_imagelocation, memory = vm_mem, 
            mementry = vm_mementry, cpucores = vm_cores, storage = vm_storage)
        
        # Add the new VM object to the cluster's vms list And check out required resources
        self.vms.append(new_vm)
        self.resource_checkout(new_vm)
        
        log.debug("(vm_create) - VM created and stored, cluster updated.")
        return create_return


    # TODO: Explain parameters, returns, and purpose
    def vm_recreate(self, vm):
        log.debug("Recreating a Nimbus VM request")

        # Store VM attributes before destroy
        vm_name    = vm.name
        vm_id      = vm.id
        vm_type    = vm.vmtype
        vm_network = vm.network
        vm_cpuarch = vm.cpuarch
        vm_imagelocation = vm.imagelocation
        vm_memory  = vm.memory
        vm_cores   = vm.cpucores
        vm_storage = vm.storage

        # Print VM parameters
        log.debug("(vm_recreate) - name: %s network: %s cpuarch: %s imageloc: %s memory: %d" \
          % (vm_name, vm_network, vm_cpuarch, vm_imagelocation, vm_memory))

        # Call destroy on the given VM
        log.debug("(vm_recreate) - Destroying VM %s..." % vm_name)
        destroy_ret = self.vm_destroy(vm)
        if (destroy_ret != 0):
            log.warning("(vm_recreate) - Destroying VM failed. Aborting recreate.")
            return destroy_ret
        
        # Call create with the given VM's parameters
        log.debug("(vm_recreate) - Recreating VM %s..." % vm_name)
        create_ret = self.vm_create(vm_name, vm_type, vm_network, vm_cpuarch, \
          vm_imagelocation, vm_memory, vm_cores, vm_storage)
        if (create_ret != 0):
            log.warning("(vm_recreate) - Recreating VM %s failed. Aborting recreate.")
            return create_ret

        # Print success message and return
        log.debug("(vm_recreate) - VM %s successfully recreated." % vm_name)
        return create_ret
   

    # TODO: Explain parameters and returns
    def vm_reboot(self, vm):
        log.debug("dbg - Nimbus cloud reboot VM command")

        # Create workspace reboot command as a list (priv. method)
        ws_cmd = self.vmreboot_factory(vm.id)
        log.debug("(vm_reboot) - workspace reboot command prepared.")
        log.debug("(vm_reboot) - Command: " + string.join(ws_cmd, " "))
        
        # Execute the reboot command: wait for return
        reboot_return = self.vm_execute(ws_cmd)

        # Check reboot return code. If successful, continue. Otherwise, set
        # VM state to "Error" and return.
        if (reboot_return != 0):
            log.warning("(vm_reboot) - Error in executing workspace reboot command.")
            log.warning("(vm_reboot) - VM failed to reboot. Setting VM to error state and returning error code.")
            # Causes fatal exception. ??
            #print "(vm_reboot) - VM %s failed to reboot. Setting vm status to \'Error\' and returning error code." % vm.name
            vm.status = "Error"
            return reboot_return 
            
        # Set state to initial default state "Starting" and return
        vm.status = "Starting"
        log.debug("(vm_reboot) - workspace reboot command executed. VM rebooting...")
        return reboot_return


    # TODO: Explain parameters and returns
    def vm_destroy(self, vm):
        log.debug('Nimbus cloud destroy command')
        
        # Create the workspace command with destroy option as a list (priv.)
        ws_cmd = self.vmdestroy_factory(vm.id)
        log.debug("(vm_destroy) - workspace destroy command prepared.")
        log.debug("(vm_destroy) - Command: " + string.join(ws_cmd, " "))

        # Execute the workspace command: wait for return, stdout to log.
        destroy_return = self.vm_execute(ws_cmd)
        
        # Check destroy return code. If successful, continue. Otherwise, set VM to 
        # error state (wait, and the polling thread will attempt a destroy later)
        if (destroy_return != 0):
            log.debug("(vm_destroy) - Error in executing workspace destroy command.")
            log.debug("(vm_destroy) - VM was not correctly destroyed. Setting VM to error state and returning error code.")
            # Causes fatal exception, for some reason
            #print "(vm_destroy) - VM %s not correctly destroyed. Setting vm status to \'Error\' and returning error code." % vm.name
            vm.status = "Error"
            return destroy_return 
        log.debug("(vm_destroy) - workspace destroy command executed.")

        # Return checked out resources And remove VM from the Cluster's 'vms' list
        self.resource_return(vm)
        self.vms.remove(vm)

        log.debug("(vm_destroy) - VM destroyed and removed, cluster updated.")
        return destroy_return


    # TODO: Explain parameters and returns
    def vm_poll(self, vm):
        log.debug('Nimbus cloud poll command')
        
        # Create workspace poll command
        ws_cmd = self.vmpoll_factory(vm.id)
        log.debug("(vm_poll) - Nimbus poll command created:\n%s" % string.join(ws_cmd, " "))

        # Execute the workspace poll (wait, retrieve return code, stdout, and stderr)
        log.debug("(vm_poll) - Executing poll command (wait for completion)...")
        (poll_return, poll_out, poll_err) = self.vm_execwait(ws_cmd)
        log.debug("(vm_poll) - Poll command completed with return code: %d" % poll_return)

        # Check the poll command return
        if (poll_return != 0):
            log.debug("(vm_poll) - Failed polling VM %s (ID: %s)" % (vm.name, vm.id))
            #print "(vm_poll) - STDERR: %s" % poll_err
            log.debug("(vm_poll) - Setting VM status to \'Error\'")
            vm.status = "Error"

            # Return the VM status as a string (exit this method)
            return vm.status
        
        # Print output, and parse the VM status from it
        #print "(vm_poll) - STDOUT: %s" % poll_out
        log.debug("(vm_poll) - Parsing polling output...")

        #STATE_RE = "State:\s(\w*)$"
        match = re.search(self.STATE_RE, poll_out)
        if match:
            tmp_state = match.group(1)
            # Set VM status:
            if (tmp_state in self.VM_STATES):
                vm.status = self.VM_STATES[tmp_state]
                log.debug("(vm_poll) - VM state: %s" % vm.status)
            else:
                log.debug("(vm_poll) - Error: state %s not in VM_STATES." % tmp_state)
                log.debug("(vm_poll) - Setting VM status to \'Error\'")
                vm.status = "Error"

        else:
            log.debug("(vm_poll) - Parsing output failed. No regex match. Setting VM status to \'Error\'")
            vm.status = "Error"

        # Return the VM status as a string
        return vm.status



    ## NimbusCluster private methods

    # A function to contain the execution of the workspace command and surrounding
    # functionality (such as logging output).
    # Built in order to limit command execution to one function.
    # Parameters:
    #    ws_cmd   - The command to be executed, as a list of strings (commands
    #               created by the _factory methods).
    def vm_execute(self, cmd):
        # Execute a workspace command with the passed cmd list. Wait for return,
        # and return return value.
        sp = Popen(cmd, executable="workspace", shell=False)
        ret = sp.wait()
        return ret
    
    # A command execution with stdout and stderr output destination specified as a filehandle.
    # Waits on the command to finish, and returns the command's return code.
    # Parameters:
    #    cmd   - A list containing the command to execute.
    #    out   - A filehandle or file object into which stdout and stderr streams are
    #            dumped.
    # Returns:
    #    ret   - The return value of the executed command
    def vm_execdump(self, cmd, out):
        try:
            sp = Popen(cmd, executable="workspace", shell=False, stdout=out, stderr=out)
            ret = sp.wait()
            return ret
        except OSError:
            log.error("Couldn't run the following command: '%s' Are the Nimbus binaries in your $PATH?" 
                      % string.join(cmd, " "))
            raise SystemExit
        except:
            log.error("Couldn't run %s command." % cmd)
            raise
       
   
    # As above, a function to encapsulate command execution via Popen.
    # vm_execwait executes the given cmd list, waits for the process to finish,
    # and returns the return code of the process. STDOUT and STDERR are stored 
    # in given parameters.
    # Parameters:
    #    (cmd as above)
    # Returns:
    #    ret   - The return value of the executed command
    #    out   - The STDOUT of the executed command
    #    err   - The STDERR of the executed command
    # The return of this function is a 3-tuple
    def vm_execwait(self, cmd):
        sp = Popen(cmd, executable="workspace", shell=False,
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        ret = sp.wait()
        (out, err) = sp.communicate(input=None)
        return (ret, out, err)

    
    # The following _factory methods take the given parameters and return a list 
    # representing the corresponding workspace command.

    def vmcreate_factory(self, epr_file, metadata_file, duration, mem, \
      deploy_state):

        ws_list = ["workspace", 
           "-z", "none",
           "--poll-delay", "200",
           "--deploy",
           "--file", epr_file,
           "--metadata", metadata_file,
           "--trash-at-shutdown",
           "-s", "https://" + self.network_address  + ":8443/wsrf/services/WorkspaceFactoryService",
           "--deploy-duration", duration,    # minutes
           "--deploy-mem", str(mem),         # megabytes (convert from int)
           "--deploy-state", deploy_state,   # Running, Paused, etc.
           "--nosubscriptions",              # Causes the command to start workspace and return immediately
           #"--exit-state", "Running",       # Running, Paused, Propagated - hard set.
           # "--dryrun",                     
          ]

        # Return the workspace command list
        return ws_list
    
    def vmreboot_factory(self, epr_file):
        ws_list = [ "workspace", "-e", epr_file, "--reboot"]
        return ws_list

    def vmdestroy_factory(self, epr_file):
        ws_list = [ "workspace", "-e", epr_file, "--destroy"]
        return ws_list

    def vmpoll_factory(self, epr_file):
        ws_list = [ "workspace", "-e", epr_file, "--rpquery"]
        return ws_list

