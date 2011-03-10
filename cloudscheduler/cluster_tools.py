#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

# Copyright (C) 2009 University of Victoria
# You may distribute under the terms of either the GNU General Public
# License or the Apache v2 License, as specified in the README file.


##
## This file contains the VM class, ICluster interface, as well as the
## implementations of this interface.

from __future__ import with_statement

import os
import re
import sys
import time
import string
import logging
import datetime
import tempfile
import subprocess
import threading

from subprocess import Popen

import nimbus_xml
import config
import cloudscheduler.utilities as utilities

log = utilities.get_cloudscheduler_logger()

try:
    import boto.ec2
    import boto


except ImportError:
    log.error("To use EC2-style clouds, you need to have boto " \
            "installed. You can install it from your package manager, " \
            "or get it from http://code.google.com/p/boto/")


class VM:
    """
    A class for storing created VM information. Used to populate Cluster classes
    'vms' lists.


    Instance Variables

    The global VM states are:
       Starting - The VM is being created in the cloud
       Running  - The VM is running somewhere on the cloud (fully functional)
       Error    - The VM has been corrupted or is in the process of being destroyed

    States are defined in each Cluster subclass, in which a VM_STATES dictionary
    maps specific cloud software state to these global states.
    """

    def __init__(self, name="", id="", vmtype="",
            hostname="", ipaddress="", clusteraddr="",
            cloudtype="", network="public", cpuarch="x86",
            image="", memory=0, mementry=0,
            cpucores=0, storage=0, keep_alive=0, spot_id="",
            proxy_file=None, job_per_core=False):
        """
        Constructor

        name         - (str) The name of the vm (arbitrary)
        id           - (str) The id tag for the VM. Whatever is used to access the vm
                       by cloud software (Nimbus: epr file. OpenNebula: id number, etc.)
        vmtype       - (str) The condor VMType attribute for the VM
        hostname     - (str) The first part of hostname given to VM
        ipaddress    - (str) The IP Address of the VM
        condorname   - (str) The name of the VM as it's registered with Condor
        condoraddr   - (str) The Address of the VM as it's registered with Condor
        clusteraddr  - (str) The address of the cluster hosting the VM
        cloudtype    - (str) The cloud type of the VM (Nimbus, OpenNebula, etc)
        network      - (str) The network association the VM uses
        cpuarch      - (str) The required CPU architecture of the VM
        image        - (str) The location of the image from which the VM was created
        memory       - (int) The memory used by the VM
        mementry     - (int) The index of the entry in the host cluster's memory list
                       from which this VM is taking memory
        proxy_file   - the proxy that was used to authenticate this VM's creation
        errorcount   - (int) Number of Polling Errors VM has had
        force_retire - (bool) Flag to prevent a retiring VM from being turned back on
        """
        self.name = name
        self.id = id
        self.vmtype = vmtype
        self.hostname = hostname
        self.ipaddress = ipaddress
        self.condorname = None
        self.condoraddr = None
        self.clusteraddr = clusteraddr
        self.cloudtype = cloudtype
        self.network = network
        self.cpuarch = cpuarch
        self.image = image
        self.memory = memory
        self.mementry = mementry
        self.cpucores = cpucores
        self.storage = storage
        self.errorcount = 0
        self.lastpoll = None
        self.last_state_change = None
        self.initialize_time = int(time.time())
        self.startup_time = None
        self.keep_alive = keep_alive
        self.idle_start = None
        self.spot_id = spot_id
        self.proxy_file = proxy_file
        self.override_status = None
        self.job_per_core = job_per_core
        self.force_retire = False
        self.job_run_times = utilities.JobRunTrackQueue('Run_Times')

        # Set a status variable on new creation
        self.status = "Starting"

        global log
        log = logging.getLogger("cloudscheduler")
        log.debug("New VM object created:")
        log.debug("VM - Name: %s, id: %s, host: %s, image: %s, memory: %d" \
          % (name, id, clusteraddr, image, memory))

    def log(self):
        log.info("VM Name: %s, ID: %s, Type: %s, Status: %s on %s" % (self.name, self.id, self.vmtype, self.status, self.clusteraddr))
    def log_dbg(self):
        log.debug("VM Name: %s, ID: %s, Type: %s, Status: %s on %s" % (self.name, self.id, self.vmtype, self.status, self.clusteraddr))

    def get_vm_info(self):
        output = "%-11s %-23s %-20s %-8s %-23s\n" % (self.id[-11:], self.hostname[-23:], self.vmtype[-10:], self.status[-8:], self.clusteraddr[-23:])
        if self.override_status != None:
            output = "%-11s %-23s %-20s %-8s %-23s\n" % (self.id[-11:], self.hostname[-23:], self.vmtype[-10:], self.override_status[-8:], self.clusteraddr[-23:])
        return output

    @staticmethod
    def get_vm_info_header():
        return "%-11s %-23s %-20s %-8s %-23s\n" % ("ID", "HOSTNAME", "VMTYPE", "STATUS", "CLUSTER")

    def get_vm_info_pretty(self):
        output = get_vm_info_header()
        output += get_vm_info()
        return output

    def get_proxy_file(self):
        if hasattr(self, "proxy_file"):
            return self.proxy_file
        else:
            return None

    # The following method will return the environment that should
    # be used when executing subprocesses.  This is needed for setting
    # the user's x509 proxy for example.
    def get_env(self):
        env = None
        if self.get_proxy_file() != None:
            env = {'X509_USER_PROXY':self.get_proxy_file()}
        return env

class NoResourcesError(Exception):
    """Exception raised for errors where not enough resources are available

    Attributes:
        resource -- name of resource that is insufficient

    """

    def __init__(self, resource):
        self.resource = resource

## The ICluster interface provides the basic structure for cluster information,
## and provides the framework (interface) for cloud management functionality.
## Each of its subclasses should should correspond to a specific implementation
## for cloud management functionality. That is, each subclass should implement
## the functions in the ICluster interface according to a specific software.

class ICluster:

    ## Instance methods

    # Constructor
    def __init__(self, name="Dummy Cluster", host="localhost",
                 cloud_type="Dummy", memory=[], cpu_archs=[], networks=[],
                 vm_slots=0, cpu_cores=0, storage=0):
        self.name = name
        self.network_address = host
        self.cloud_type = cloud_type
        self.memory = memory
        self.max_mem = tuple(memory)
        self.cpu_archs = cpu_archs
        self.network_pools = networks
        self.vm_slots = vm_slots
        self.cpu_cores = cpu_cores
        self.storageGB = storage
        self.max_storageGB = (storage)
        self.vms = [] # List of running VMs
        self.vms_lock = threading.RLock()
        self.res_lock = threading.RLock()

        self.setup_logging()
        log.info("New cluster %s created" % self.name)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['vms_lock']
        del state['res_lock']
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self.vms_lock = threading.RLock()
        self.res_lock = threading.RLock()

    def setup_logging(self):
        global log
        log = logging.getLogger("cloudscheduler")


    # Print cluster information
    def log_cluster(self):
        log.info("-" * 30 +
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

    # Print the cluster 'vms' list (via VM print)
    def log_vms(self):
        if len(self.vms) == 0:
            log.info("CLUSTER %s has no running VMs..." % (self.name))
        else:
            log.info("CLUSTER %s running VMs:" % (self.name))
            for vm in self.vms:
                vm.log_short("\t")


    ## Support methods

    # Returns the number of VMs running on the cluster (in accordance
    # to the vms[] list)
    def num_vms(self):
        return len(self.vms)
    # Return a short form of cluster information
    def get_cluster_info_short(self):
        output = "Cluster: %s \n" % self.name
        output += "%-25s  %-15s  %-10s  %-10s \n" % ("ADDRESS", "CLOUD TYPE", "VM SLOTS", "MEMORY")
        output += "%-25s  %-15s  %-10s  %-10s \n" % (self.network_address, self.cloud_type, self.vm_slots, self.memory)
        return output
    # Return information about running VMs on Cluster
    def get_cluster_vms_info(self):
        if len(self.vms) == 0:
            return ""
        else:
            output = ""
            for vm in self.vms:
                output += vm.get_vm_info()
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

    def vm_create(self, **args):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_create'

    def vm_recreate(self, vm):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_recreate'

    def vm_reboot(self, vm):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_reboot'

    def vm_destroy(self, vm, return_resources=True):
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

    def find_potential_mementry(self, memory):
        potential_fit = False
        for i in range(len(self.max_mem)):
            if self.max_mem[i] >= memory:
                potential_fit = True
                break
        return potential_fit

    def resource_checkout(self, vm):
        """
        Checks out resources taken by a VM in creation from the internal rep-
        resentation of the Cluster

        Parameters:
        vm   - the VM object used to check out resources from the Cluster.

        Raises NoResourcesError if there are not enough available resources
        to check out.
        """
        log.debug("Checking out resources for VM %s from Cluster %s" % (vm.name, self.name))
        with self.res_lock:
            remaining_vm_slots = self.vm_slots - 1
            if remaining_vm_slots < 0:
                raise NoResourcesError("vm_slots")

            remaining_storage = self.storageGB - vm.storage
            if remaining_storage < 0:
                raise NoResourcesError("storage")

            remaining_memory = self.memory[vm.mementry] - vm.memory
            if remaining_memory < 0:
                raise NoResourcesError("memory")

            # Otherwise, we can check out these resources
            self.vm_slots = remaining_vm_slots
            self.storageGB = remaining_storage
            self.memory[vm.mementry] = remaining_memory

    # Returns the resources taken by the passed in VM to the Cluster's internal
    # storage.
    # Parameters: (as for checkout() )
    # Notes: (as for checkout)
    def resource_return(self, vm):
        log.info("Returning resources used by VM %s to Cluster %s" % (vm.id, self.name))
        with self.res_lock:
            self.vm_slots += 1
            self.storageGB += vm.storage
            # ISSUE: No way to know what mementry a VM is running on
            try:
                self.memory[vm.mementry] += vm.memory
            except:
                log.warning("Couldn't return memory because I don't know about that mem entry anymore...")


## Implements cloud management functionality with the Nimbus service as part of
## the Globus Toolkit.

class NimbusCluster(ICluster):

    ## NimbusCluster specific instance variables

    # Global Nimbus command variables
    VM_DURATION = config.vm_lifetime
    VM_TARGETSTATE = "Running"
    VM_NODES = "1"

    # Number of seconds to wait between executing a shutdown and a destroy.
    # (Used in vm_destroy method)
    VM_SHUTDOWN = 8

    ERROR = 1

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

    def __init__(self, name="Dummy Cluster", host="localhost", cloud_type="Dummy",
                 memory=[], cpu_archs=[], networks=[], vm_slots=0,
                 cpu_cores=0, storage=0,
                 access_key_id=None, secret_access_key=None, security_group=None):

        # Call super class's init
        ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, cpu_archs=cpu_archs, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage,)


    def vm_create(self, vm_name, vm_type, vm_networkassoc, vm_cpuarch,
            vm_image, vm_mem, vm_cores, vm_storage, customization=None, vm_keepalive=0,
            job_proxy_file_path=None, job_per_core=False):

        def _remove_files(files):
            for file in files:
                try:
                    log.debug("Deleting %s" % file)
                    os.remove(file)
                except:
                    log.debug("Couldn't delete %s" % file)

        log.debug("Nimbus cloud create command")

        if vm_networkassoc == "":
            # No network specified, so just pick the first available one
            try:
                vm_networkassoc = self.network_pools[0]
            except:
                log.exception("No network pool available? Aborting vm creation.")
                return self.ERROR

        # Create a workspace metadata xml file
        vm_metadata = nimbus_xml.ws_metadata_factory(vm_name, vm_networkassoc, \
                vm_cpuarch, vm_image)

        # Create a deployment request file
        vm_deploymentrequest = nimbus_xml.ws_deployment_factory(self.VM_DURATION, \
                self.VM_TARGETSTATE, vm_mem, vm_storage, self.VM_NODES, vm_cores=vm_cores)

        job_proxy = None
        if job_proxy_file_path:
            try:
                with open(job_proxy_file_path) as proxy:
                    job_proxy = proxy.read()
            except:
                log.exception("Couldn't read proxy file %s, continuing without it." % job_proxy_file_path)

        if customization or job_proxy:
            vm_optional = nimbus_xml.ws_optional_factory(custom_tasks=customization, credential=job_proxy)
        else:
            vm_optional = None


        # Set a timestamp for VM creation
        now = datetime.datetime.now()

        # Create an EPR file name (unique with timestamp)
        (epr_handle, vm_epr) = tempfile.mkstemp(suffix=".vm_epr")
        os.close(epr_handle)

        nimbus_files = [vm_epr, vm_metadata, vm_deploymentrequest, vm_optional]

        # Create the workspace command as a list (private method)
        ws_cmd = self.vmcreate_factory(vm_epr, vm_metadata, vm_deploymentrequest, optional_file=vm_optional)
        

        log.debug("Command: " + string.join(ws_cmd, " "))

        # Execute the workspace create command: returns immediately.
        env = None;
        if job_proxy_file_path != None:
            env = {'X509_USER_PROXY':job_proxy_file_path}
            log.debug("VM creation environment will contain:\n\tX509_USER_PROXY = %s" % (job_proxy_file_path))
        
        (create_return, create_out, create_err) = self.vm_execwait(ws_cmd, env)
        if (create_return != 0):
            log.warning("Error creating VM %s: %s %s" % (vm_name, create_out, create_err))
            _remove_files(nimbus_files)
            return create_return

        log.debug("Nimbus create command executed.")

        log.debug("Deleting temporary Nimbus Metadata files")
        _remove_files(nimbus_files)

        # Find the memory entry in the Cluster 'memory' list which _create will be
        # subtracted from
        vm_mementry = self.find_mementry(vm_mem)
        if (vm_mementry < 0):
            # At this point, there should always be a valid mementry, as the ResourcePool
            # get_resource methods have selected this cluster based on having an open
            # memory entry that fits VM requirements.
            log.error("Cluster memory list has no sufficient memory " +\
              "entries (Not supposed to happen). Returning error.")
        log.debug("Memory entry found in given cluster: %d" % vm_mementry)

        # Get the id of the VM from the output of workspace.sh
        try:
            vm_id = re.search("Workspace created: id (\d*)", create_out).group(1)
        except:
            log.error("Couldn't find workspace id for new VM")
        try:
            vm_ip = re.search("IP address: (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", create_out).group(1)
        except:
            log.error("Couldn't find the ip address for new VM")

        # Get the first part of the hostname given to the VM
        vm_hostname = self._extract_hostname(create_out)
        if vm_hostname:
            log.debug("Hostname for vm_id %s is %s" % (vm_id, vm_hostname))
        else:
            log.warning("Unable to get the VM hostname, for vm_id %s" % vm_id)


        # Create a VM object to represent the newly created VM
        new_vm = VM(name = vm_name, id = vm_id, vmtype = vm_type,
            hostname = vm_hostname, ipaddress = vm_ip, 
            clusteraddr = self.network_address,
            cloudtype = self.cloud_type,network = vm_networkassoc,
            cpuarch = vm_cpuarch, image = vm_image,
            memory = vm_mem, mementry = vm_mementry, cpucores = vm_cores,
            storage = vm_storage, keep_alive = vm_keepalive, 
            proxy_file = job_proxy_file_path, job_per_core = job_per_core)

        # Add the new VM object to the cluster's vms list And check out required resources
        self.vms.append(new_vm)
        try:
            self.resource_checkout(new_vm)
        except:
            log.exception("Unexpected error checking out resources when creating a VM. Programming error?")
            return self.ERROR

        log.info("Started vm %s on %s using image at %s" % (new_vm.id, new_vm.clusteraddr, new_vm.image))
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
        vm_image = vm.image
        vm_memory  = vm.memory
        vm_cores   = vm.cpucores
        vm_storage = vm.storage
        vm_proxy_file = vm.get_proxy_file()

        # Print VM parameters
        log.debug("(vm_recreate) - name: %s network: %s cpuarch: %s imageloc: %s memory: %d" \
          % (vm_name, vm_network, vm_cpuarch, vm_image, vm_memory))

        # Call destroy on the given VM
        log.debug("(vm_recreate) - Destroying VM %s..." % vm_name)
        destroy_ret = self.vm_destroy(vm)
        if (destroy_ret != 0):
            log.warning("(vm_recreate) - Destroying VM failed. Aborting recreate.")
            return destroy_ret

        # Call create with the given VM's parameters
        log.debug("(vm_recreate) - Recreating VM %s..." % vm_name)
        create_ret = self.vm_create(vm_name, vm_type, vm_network, vm_cpuarch, \
          vm_image, vm_memory, vm_cores, vm_storage, job_proxy_file_path=vm_proxy_file)
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
        (reboot_return, reboot_out, reboot_err) = self.vm_execwait(ws_cmd, env=vm.get_env())

        # Check reboot return code. If successful, continue. Otherwise, set
        # VM state to "Error" and return.
        if (reboot_return != 0):
            log.warning("vm_reboot - Error rebooting VM %s: %s %s" % (vm.id, reboot_out, reboot_err))
            log.warning("vm_reboot - Setting VM status 'Error'.")
            vm.status = "Error"
            return reboot_return

        # Set state to initial default state "Starting" and return
        vm.status = "Starting"
        log.debug("(vm_reboot) - workspace reboot command executed. VM rebooting...")
        return reboot_return


    def vm_destroy(self, vm, return_resources=True, shutdown_first=True):
        """
        Shutdown, destroy and return resources of a VM to it's cluster

        Parameters:
        vm -- vm to shutdown and destroy
        return_resources -- if set to false, do not return resources from VM to cluster
        shutdown_first -- if set to false, will first call a shutdown before destroying
        """

        # Create an epr for workspace.sh
        vm_epr = nimbus_xml.ws_epr_factory(vm.id, vm.clusteraddr)

        if shutdown_first:
            # Create the workspace command with shutdown option
            shutdown_cmd = self.vmshutdown_factory(vm_epr)
            log.verbose("Shutting down VM with command: " + string.join(shutdown_cmd, " "))

            # Execute the workspace shutdown command.
            shutdown_return = self.vm_exec_silent(shutdown_cmd, env=vm.get_env())
            if (shutdown_return != 0):
                log.debug("(vm_destroy) - VM shutdown request failed, moving directly to destroy.")
            else:
                log.debug("(vm_destroy) - workspace shutdown command executed successfully.")
                # Sleep for a few seconds to allow for proper shutdown
                log.debug("Waiting %ss for VM to shut down..." % self.VM_SHUTDOWN)
                time.sleep(self.VM_SHUTDOWN)


        # Create the workspace command with destroy option as a list (priv.)
        destroy_cmd = self.vmdestroy_factory(vm_epr)
        log.verbose("Destroying VM with command: " + string.join(destroy_cmd, " "))

        # Execute the workspace destroy command: wait for return, stdout to log.
        (destroy_return, destroy_out, destroy_error) = self.vm_execwait(destroy_cmd, env=vm.get_env())
        destroy_out = destroy_out + destroy_error


        # Check destroy return code. If successful, continue. Otherwise, set VM to
        # error state (wait, and the polling thread will attempt a destroy later)
        if (destroy_return != 0):

            if "Destroyed" == self._extract_state(destroy_error):
                log.debug("VM %s seems to have already been destroyed." % vm.id)
            else:
                log.warning("VM %s was not correctly destroyed: %s %s" % (vm.id, destroy_out, destroy_error))
                vm.status = "Error"
                os.remove(vm_epr)
                return destroy_return


        # Return checked out resources And remove VM from the Cluster's 'vms' list
        with self.vms_lock:
            try:
                self.vms.remove(vm)
            except ValueError:
                log.error("Attempted to remove vm from list that was already removed.")
                return_resources = False
        if return_resources:
            self.resource_return(vm)

        # Delete EPR
        os.remove(vm_epr)

        log.info("Destroyed vm %s on %s" % (vm.id, vm.clusteraddr))

        return destroy_return


    def vm_poll(self, vm):
        """
        vm_poll -- Polls a running VM, updates its status, and returns its state

        Parameters:
            vm -- vm to poll

        Note: If VM does not appear to be running any longer, it will be destroyed.
        """

        # Create an epr for our poll command
        vm_epr = nimbus_xml.ws_epr_factory(vm.id, vm.clusteraddr)

        # Create workspace poll command
        ws_cmd = self.vmpoll_factory(vm_epr)
        log.verbose("Polling Nimbus with:\n%s" % string.join(ws_cmd, " "))

        # Execute the workspace poll (wait, retrieve return code, stdout, and stderr)
        (poll_return, poll_out, poll_err) = self.vm_execwait(ws_cmd, env=vm.get_env())
        poll_out = poll_out + poll_err
        with self.vms_lock:

            # Print output, and parse the VM status from it
            vm.hostname = self._extract_hostname(poll_out)
            new_status = self._extract_state(poll_out)
            if new_status == "Destroyed":
                log.info("Discarding VM %s because Nimbus has destroyed it" % vm.id)
                self.vm_destroy(vm, shutdown_first=False)
                vm.status = new_status

            elif new_status == "NoProxy":
                vm.override_status = new_status
                log.error("Problem polling VM %s. You don't have a valid proxy." % vm.id)

            elif new_status == "ExpiredProxy":
                vm.override_status = new_status
                log.error("Problem polling VM %s. Your proxy expired." % vm.id)

            elif vm.status != new_status:
                vm.last_state_change = int(time.time())
                vm.status = new_status

            # If there was some other error we're not aware of (temporary network problem, etc...)
            elif (poll_return != 0):
                log.warning("There was a problem polling VM %s: %s %s" % (vm.id, poll_out, poll_err))

        # Tidy up and return
        os.remove(vm_epr)
        vm.lastpoll = int(time.time())
        return vm.status



    ## NimbusCluster private methods

    # A function to contain the execution of the workspace command and surrounding
    # functionality (such as logging output).
    # Built in order to limit command execution to one function.
    # Parameters:
    #    ws_cmd   - The command to be executed, as a list of strings (commands
    #               created by the _factory methods).
    def vm_execute(self, cmd, env=None):
        # Execute a workspace command with the passed cmd list. Wait for return,
        # and return return value.
        try:
            sp = Popen(cmd, executable=config.workspace_path, shell=False, env=env)
            if not utilities.check_popen_timeout(sp):
                (out, err) = sp.communicate(input=None)
            return sp.returncode
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (string.join(cmd, " "), e.errno, e.strerror))
            return -1
        except:
            log.error("Problem running %s, unexpected error" % string.join(cmd, " "))
            return -1

    # A command execution with stdout and stderr output destination specified as a filehandle.
    # Waits on the command to finish, and returns the command's return code.
    # Parameters:
    #    cmd   - A list containing the command to execute.
    #    out   - A filehandle or file object into which stdout and stderr streams are
    #            dumped.
    # Returns:
    #    ret   - The return value of the executed command
    def vm_execdump(self, cmd, out, env=None):
        try:
            sp = Popen(cmd, executable=config.workspace_path, shell=False, stdout=out, stderr=out, env=env)
            if not utilities.check_popen_timeout(sp):
                (out, err) = sp.communicate(input=None)
            return sp.returncode
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (string.join(cmd, " "),e.errno, e.strerror))
            return -1
        except:
            log.error("Problem running %s, unexpected error" % string.join(cmd, " "))
            return -1


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
    def vm_execwait(self, cmd, env=None):
        try:
            sp = Popen(cmd, executable=config.workspace_path, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            if not utilities.check_popen_timeout(sp):
                (out, err) = sp.communicate(input=None)
            return (sp.returncode, out, err)
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (string.join(cmd, " "), e.errno, e.strerror))
            return (-1, "", "")
        except:
            log.error("Problem running %s, unexpected error" % string.join(cmd, " "))
            return (-1, "", "")

    def vm_exec_silent(self, cmd, env=None):
        """
        vm_exec_silent executes a given command list, and discards the output

        parameter: cmd -- a list of a command and arguments

        returns: the return value of the command that was run

        """

        try:
            sp = Popen(cmd, executable=config.workspace_path, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            if not utilities.check_popen_timeout(sp):
                (out, err) = sp.communicate(input=None)
            return sp.returncode
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (string.join(cmd, " "), e.errno, e.strerror))
            return -1
        except:
            log.error("Problem running %s, unexpected error" % string.join(cmd, " "))
            return -1

    # The following _factory methods take the given parameters and return a list
    # representing the corresponding workspace command.
    def vmcreate_factory(self, epr_file, metadata_file, request_file, optional_file=None):

        ws_list = [config.workspace_path,
           "-z", "none",
           "--poll-delay", "200",
           "--deploy",
           "--file", epr_file,
           "--metadata", metadata_file,
           "--request", request_file,
           "-s", "https://" + self.network_address + ":8443/wsrf/services/WorkspaceFactoryService",
           "--nosubscriptions",              # Causes the command to start workspace and return immediately
          ]
        if optional_file:
            ws_list.append("--optional")
            ws_list.append(optional_file)

        # Return the workspace command list
        return ws_list

    def vmreboot_factory(self, epr_file):
        ws_list = [config.workspace_path, "-e", epr_file, "--reboot"]
        return ws_list

    def vmdestroy_factory(self, epr_file):
        ws_list = [config.workspace_path, "-e", epr_file, "--destroy"]
        return ws_list

    def vmshutdown_factory(self, epr_file):
        ws_list = [config.workspace_path, "-e", epr_file, "--shutdown"]
        return ws_list

    def vmpoll_factory(self, epr_file):
        ws_list = [config.workspace_path, "-e", epr_file, "--rpquery"]
        return ws_list

    @staticmethod
    def _extract_hostname(create_response):
        """
        _extract_hostname -- extracts the hostname from a Nimbus create call

        returns short hostname of VM as string
        """

        try:
            matches = re.search("Hostname:\s(.*)[\.\s]", create_response)
            hostname = matches.group(1)
        except:
            return ""

        return hostname

    @staticmethod
    def _extract_state(output):
        """
        _extract_state -- extract the state from a Nimbus workspace command

        Parameters:
            output -- expects the output from a workspace.sh command
        """

        STATE_RE = "State:\s(\w*)"

        # Check if VM has a regular state
        match = re.search(STATE_RE, output)
        if match:
            status = match.group(1)
            if (status in NimbusCluster.VM_STATES):
                return NimbusCluster.VM_STATES[status]
            else:
                return "Error"

        # Check if VM does not exist on server
        non_existant = re.search("This workspace is unknown to the service", output)
        if non_existant:
            return "Destroyed"

        # Check if you have no proxy
        no_proxy = re.search("Defective credential detected.*not found", output)
        if no_proxy:
            return "NoProxy"

        # Check if your proxy is expired
        expired_proxy = re.search("Expired credentials detected", output)
        if expired_proxy:
            return "ExpiredProxy"

        return "Error"


class EC2Cluster(ICluster):

    VM_STATES = {
            "running" : "Running",
            "pending" : "Starting",
            "shutting-down" : "Shutdown",
            "termimated" : "Shutdown",
            "error" : "Error",
    }

    ERROR = 1
    DEFAULT_INSTANCE_TYPE = "m1.small"

    def _get_connection(self):
        """
            _get_connection - get a boto connection object to this cluster

            returns a boto connection object, or none in the case of an error
        """

        connection = None

        if self.cloud_type == "AmazonEC2":
            try:
                region = boto.ec2.regioninfo.RegionInfo(name=self.name,
                                                 endpoint=self.network_address)
                connection = boto.connect_ec2(
                                   aws_access_key_id=self.access_key_id,
                                   aws_secret_access_key=self.secret_access_key,
                                   )
                log.debug("Created a connection to Amazon EC2")

            except boto.exception.EC2ResponseError, e:
                log.error("Couldn't connect to Amazon EC2 because: %s" %
                                                                e.error_message)

        elif self.cloud_type == "Eucalyptus":
            try:
                region = boto.ec2.regioninfo.RegionInfo(name=self.name,
                                                 endpoint=self.network_address)
                connection = boto.connect_ec2(
                                   aws_access_key_id=self.access_key_id,
                                   aws_secret_access_key=self.secret_access_key,
                                   is_secure=False,
                                   region=region,
                                   port=8773,
                                   path="/services/Eucalyptus",
                                   )
                log.debug("Created a connection to Eucalyptus (%s)" % self.name)

            except boto.exception.EC2ResponseError, e:
                log.error("Couldn't connect to Amazon EC2 because: %s" %
                                                               e.error_message)

        elif self.cloud_type == "OpenNebula":

            log.error("OpenNebula support isn't ready yet.")
            raise NotImplementedError
        else:
            log.error("EC2Cluster don't know how to handle a %s cluster." %
                                                               self.cloud_type)

        return connection


    def __init__(self, name="Dummy Cluster", host="localhost", cloud_type="Dummy",
                 memory=[], cpu_archs=[], networks=[], vm_slots=0,
                 cpu_cores=0, storage=0,
                 access_key_id=None, secret_access_key=None, security_group=None):

        # Call super class's init
        ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, cpu_archs=cpu_archs, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage,)

        if not security_group:
            security_group = "default"
        self.security_groups = [security_group]

        if not access_key_id or not secret_access_key:
            log.error("Cannot connect to cluster %s "
                      "because you haven't specified an access_key_id or "
                      "a secret_access_key" % self.name)

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

        connection = self._get_connection()


    def vm_create(self, vm_name, vm_type, vm_networkassoc, vm_cpuarch,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  vm_keepalive=0, instance_type="", maximum_price=0,
                  job_per_core=False):

        log.debug("Trying to boot %s on %s" % (vm_type, self.network_address))

        try:
            vm_ami = vm_image[self.network_address]
        except:
            log.debug("No AMI for %s, trying default" % self.network_address)
            try:
                vm_ami = vm_image["default"]
            except:
                log.exception("Can't find a suitable AMI")
                return

        try:
            i_type = instance_type[self.network_address]
        except:
            log.debug("No instance type for %s, trying default" % self.network_address)
            try:
                i_type = instance_type["default"]
            except:
                i_type = self.DEFAULT_INSTANCE_TYPE
        instance_type = i_type

        if customization:
            user_data = nimbus_xml.ws_optional(customization)
        else:
            user_data = ""

        if "AmazonEC2" == self.cloud_type and vm_networkassoc != "public":
            log.debug("You requested '%s' networking, but EC2 only supports 'public'" % vm_networkassoc)
            addressing_type = "public"
        else:
            addressing_type = vm_networkassoc

        try:
            connection = self._get_connection()
            image = None
            if not "Eucalyptus" == self.cloud_type:
                image = connection.get_image(vm_ami)

            else: #HACK: for some reason Eucalyptus won't respond properly to
                  #      get_image("whateverimg"). Use a linear search until
                  #      this is fixed
                  # This is Eucalyptus bug #495670
                  # https://bugs.launchpad.net/eucalyptus/+bug/495670
                images = connection.get_all_images()
                for potential_match in images:
                    if potential_match.id == vm_ami:
                        image = potential_match
                        break

            if image:
                if maximum_price is 0: # don't request a spot instance
                    try:
                        reservation = image.run(1,1,
                                                addressing_type=addressing_type,
                                                user_data=user_data,
                                                security_groups=self.security_groups,
                                                instance_type=instance_type)
                        instance_id = reservation.instances[0].id
                        log.debug("Booted VM %s" % instance_id)
                    except:
                        log.exception("There was a problem creating an EC2 instance...")
                        return self.ERROR

                else: # get a spot instance of no more than maximum_price
                    try:
                        price_in_dollars = str(float(maximum_price) / 100)
                        reservation = connection.request_spot_instances(
                                                  price_in_dollars,
                                                  image.id,
                                                  user_data=user_data,
                                                  addressing_type=addressing_type,
                                                  security_groups=self.security_groups,
                                                  instance_type=instance_type)
                        spot_id = str(reservation[0].id)
                        instance_id = ""
                        log.debug("Reserved instance %s at no more than %s" % (spot_id, price_in_dollars))
                    except AttributeError:
                        log.exception("Your version of boto doesn't seem to support "\
                                  "spot instances. You need at least 1.9")
                        return self.ERROR
                    except:
                        log.exception("Problem creating an EC2 spot instance...")
                        return self.ERROR


            else:
                log.error("Couldn't find image %s on %s" % (vm_image, self.name))
                return self.ERROR

        except:
            log.exception("Problem creating EC2 instance on on %s" % self.name)
            return self.ERROR

        vm_mementry = self.find_mementry(vm_mem)
        if (vm_mementry < 0):
            #TODO: this is kind of pointless with EC2...
            log.debug("Cluster memory list has no sufficient memory " +\
                      "entries (Not supposed to happen). Returning error.")
            return self.ERROR
        log.debug("vm_create - Memory entry found in given cluster: %d" %
                                                                    vm_mementry)
        new_vm = VM(name = vm_name, id = instance_id, vmtype = vm_type,
                    clusteraddr = self.network_address,
                    cloudtype = self.cloud_type, network = vm_networkassoc,
                    cpuarch = vm_cpuarch, image= vm_image,
                    memory = vm_mem, mementry = vm_mementry,
                    cpucores = vm_cores, storage = vm_storage, 
                    keep_alive = vm_keepalive, job_per_core = job_per_core)

        try:
            new_vm.spot_id = spot_id
        except:
            log.verbose("No spot ID to add to VM %s" % instance_id)

        try:
            self.resource_checkout(new_vm)
        except:
            log.exception("Unexpected Error checking out resources when creating a VM. Programming error?")
            self.vm_destroy(new_vm)
            return self.ERROR

        self.vms.append(new_vm)

        return 0


    def vm_poll(self, vm):
        try:
            log.verbose("Polling vm with instance id %s" % vm.id)
            connection = self._get_connection()

            if vm.spot_id:
                try:
                    spot_reservation = connection.get_all_spot_instance_requests(vm.spot_id)[0]
                    if spot_reservation.instance_id == None:
                        log.debug("Spot reservation %s doesn't have a VM id yet." % vm.spot_id)
                        return vm.status
                    vm.id = str(spot_reservation.instance_id)
                except AttributeError:
                    log.exception("Problem getting spot VM info. Do you have boto 2.0+?")
                    return vm.status
                except:
                    log.exception("Problem getting information for spot vm %s" % vm.spot_id)
                    return vm.status

            instance = None
            try:
                reservations = connection.get_all_instances([vm.id])
                instance = reservations[0].instances[0]
            except IndexError:
                log.error("%s on %s doesn't seem to exist anymore, setting status to Error" % (vm.id, self.network_address))
                vm.status = self.VM_STATES['error']
                vm.last_state_change = int(time.time())
                return vm.status
            except:
                log.exception("Unexpected error polling %s" % vm.id)
                return vm.status

        except boto.exception.EC2ResponseError, e:
            log.error("Couldn't update status because: %s" % e.error_message)
            return vm.status

        with self.vms_lock:
            if vm.status != self.VM_STATES.get(instance.state, "Starting"):

                vm.last_state_change = int(time.time())
            vm.status = self.VM_STATES.get(instance.state, "Starting")
            vm.hostname = instance.public_dns_name
            vm.lastpoll = int(time.time())
        return vm.status


    def vm_destroy(self, vm, return_resources=True):
        """
        Shutdown, destroy and return resources of a VM to it's cluster

        Parameters:
        vm -- vm to shutdown and destroy
        return_resources -- if set to false, do not return resources from VM to cluster
        """
        log.debug("Destroying vm with instance id %s" % vm.id)

        try:
            connection = self._get_connection()

            if vm.spot_id:
                connection.cancel_spot_instance_requests([vm.spot_id])

            if vm.id:
                reservations = connection.get_all_instances([vm.id])
                instance = reservations[0].instances[0]
                instance.terminate()

        except IndexError:
            log.warning("%s already seem to be gone... removing anyway." % vm.id)
        except boto.exception.EC2ResponseError, e:
            log.exception("Couldn't connect to cloud to destroy vm!")
            return self.ERROR
        except:
            log.exception("Unexpected error destroying vm!")

        # Delete references to this VM
        if return_resources:
            self.resource_return(vm)
        with self.vms_lock:
            self.vms.remove(vm)

        return 0

