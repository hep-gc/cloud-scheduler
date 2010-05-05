#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

# Copyright (C) 2009 University of Victoria
# You may distribute under the terms of either the GNU General Public
# License or the Apache v2 License, as specified in the README file.


##
## This file contains the VM class, ICluster interface, as well as the
## implementations of this interface.

import os
import re
import sys
import time
import string
import logging
import datetime
import tempfile
import subprocess

log = None

from subprocess import Popen
try:
    import boto.ec2
except ImportError:
        print >> sys.stderr, "To use EC2-style clouds, you need to have boto " \
              "installed. You can install it from your package manager, " \
              "or get it from http://code.google.com/p/boto/"

import nimbus_xml
import config

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
    # hostname     - (str) The first part of hostname given to VM
    # clusteraddr  - (str) The address of the cluster hosting the VM
    # cloudtype   - (str) The cloud type of the VM (Nimbus, OpenNebula, etc)
    # network      - (str) The network association the VM uses
    # cpuarch      - (str) The required CPU architecture of the VM
    # image        - (str) The location of the image from which the VM was created
    # memory       - (int) The memory used by the VM
    # mementry     - (int) The index of the entry in the host cluster's memory list
    #                from which this VM is taking memory
    # errorcount   - (int) Number of Polling Errors VM has had
    def __init__(self, name="default_VM", id="default_VMID", vmtype="default_VMType",
            hostname="default_vmhostname", clusteraddr="default_hostname",
            cloudtype="def_cloudtype", network="public", cpuarch="x86",
            image="default_image", memory=0, mementry=0,
            cpucores=0, storage=0):
        self.name = name
        self.id = id
        self.vmtype = vmtype
        self.hostname = hostname
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
        output = "%-15s %-10s %-15s %-25s\n" % (self.id[-15:], self.vmtype[-10:], self.status[-15:], self.clusteraddr[-25:])
        return output

    @staticmethod
    def get_vm_info_header():
        return "%-15s  %-10s  %-15 %-25s\n" % ("ID", "VMTYPE", "STATUS", "CLUSTER")

    def get_vm_info_pretty(self):
        output = get_vm_info_header()
        output += get_vm_info()
        return output


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
        self.cpu_archs = cpu_archs
        self.network_pools = networks
        self.vm_slots = vm_slots
        self.cpu_cores = cpu_cores
        self.storageGB = storage
        self.vms = [] # List of running VMs

        global log
        log = logging.getLogger("cloudscheduler")
        log.info("New cluster %s created" % self.name)


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
            return "CLUSTER %s has no running VMs..." % (self.name)
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

    def vm_create(self, vm_name, vm_type, vm_networkassoc, vm_cpuarch,
            vm_image, vm_mem, vm_cores, vm_storage, customization=None):
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
        log.info("Checking out resources for VM %s from Cluster %s" % (vm.name, self.name))
        self.vm_slots -= 1
        self.storageGB -= vm.storage
        # ISSUE: No way to know what mementry a VM is running on
        self.memory[vm.mementry] -= vm.memory

    # Returns the resources taken by the passed in VM to the Cluster's internal
    # storage.
    # Parameters: (as for checkout() )
    # Notes: (as for checkout)
    def resource_return(self, vm):
        log.info("Returning resources used by VM %s to Cluster %s" % (vm.id, self.name))
        self.vm_slots += 1
        self.storageGB += vm.storage
        # ISSUE: No way to know what mementry a VM is running on
        self.memory[vm.mementry] += vm.memory


## Implements cloud management functionality with the Nimbus service as part of
## the Globus Toolkit.

class NimbusCluster(ICluster):

    ## NimbusCluster specific instance variables

    # Nimbus global state finding regexp (parsing Poll output)
    STATE_RE = "State:\s(\w*)$"

    # Global Nimbus command variables
    VM_DURATION = "10080"
    VM_TARGETSTATE = "Running"
    VM_NODES = "1"

    # Number of seconds to wait between executing a shutdown and a destroy.
    # (Used in vm_destroy method)
    VM_SHUTDOWN = 8

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
            vm_image, vm_mem, vm_cores, vm_storage, customization=None):

        log.debug("Nimbus cloud create command")

        # Create a workspace metadata xml file from passed parameters
        vm_metadata = nimbus_xml.ws_metadata_factory(vm_name, vm_networkassoc, \
                vm_cpuarch, vm_image)

        # Create a deployment request file from given parameters
        vm_deploymentrequest = nimbus_xml.ws_deployment_factory(self.VM_DURATION, \
                self.VM_TARGETSTATE, vm_mem, vm_storage, self.VM_NODES, vm_cores=vm_cores)

        if customization:
            vm_optional = nimbus_xml.ws_optional_factory(customization)
        else:
            vm_optional = None


        # Set a timestamp for VM creation
        now = datetime.datetime.now()

        # Create an EPR file name (unique with timestamp)
        (epr_handle, vm_epr) = tempfile.mkstemp()
        os.close(epr_handle)

        # Create the workspace command as a list (private method)
        ws_cmd = self.vmcreate_factory(vm_epr, vm_metadata, vm_deploymentrequest, optional_file=vm_optional)
        log.debug("vm_create - workspace create command prepared.")
        log.debug("vm_create - Command: " + string.join(ws_cmd, " "))

        # Execute the workspace create command: returns immediately.
        (create_return, create_out, create_err) = self.vm_execwait(ws_cmd)
        if (create_return != 0):
            log.warning("vm_create - Error in executing workspace create command.")
            log.warning("vm_create - VM %s (ID: %s) not created. Returning error code." \
              % (vm_name, vm_epr))
            return create_return
        log.debug("(vm_create) - workspace create command executed.")

        log.debug("vm_create - Deleting temporary Nimbus Metadata files")
        os.remove(vm_metadata)
        os.remove(vm_deploymentrequest)
        if vm_optional:
            os.remove(vm_optional)

        # Find the memory entry in the Cluster 'memory' list which _create will be
        # subtracted from
        vm_mementry = self.find_mementry(vm_mem)
        if (vm_mementry < 0):
            # At this point, there should always be a valid mementry, as the ResourcePool
            # get_resource methods have selected this cluster based on having an open
            # memory entry that fits VM requirements.
            log.error("(vm_create) - Cluster memory list has no sufficient memory " +\
              "entries (Not supposed to happen). Returning error.")
        log.debug("(vm_create) - vm_create - Memory entry found in given cluster: %d" % vm_mementry)

        # Get the first part of the hostname given to the VM
        hostname = re.search("Hostname:\s(\w*)", create_out)
        vm_hostname = "default_vmhostname"
        if hostname:
            vm_hostname = hostname.group(1)
            log.debug("Hostname for vm_id %s is %s" % (vm_epr, vm_hostname))
        else:
            log.warning("Unable to get the VM hostname, for vm_id %s" % vm_epr)

        # Create a VM object to represent the newly created VM
        new_vm = VM(name = vm_name, id = vm_epr, vmtype = vm_type,
            hostname = vm_hostname, clusteraddr = self.network_address,
            cloudtype = self.cloud_type,network = vm_networkassoc,
            cpuarch = vm_cpuarch, image = vm_image,
            memory = vm_mem, mementry = vm_mementry, cpucores = vm_cores,
            storage = vm_storage)

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
        vm_image = vm.image
        vm_memory  = vm.memory
        vm_cores   = vm.cpucores
        vm_storage = vm.storage

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
          vm_image, vm_memory, vm_cores, vm_storage)
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

        # Create the workspace command with shutdown option
        shutdown_cmd = self.vmshutdown_factory(vm.id)
        log.debug("Shutting down VM with command: " + string.join(shutdown_cmd, " "))

        # Create the workspace command with destroy option as a list (priv.)
        destroy_cmd = self.vmdestroy_factory(vm.id)
        log.debug("Destroying VM with command: " + string.join(destroy_cmd, " "))

        # Execute the workspace shutdown command.
        shutdown_return = self.vm_execute(shutdown_cmd)
        if (shutdown_return != 0):
            log.warning("(vm_destroy) - VM shutdown request failed, moving directly to destroy.")
        else:
            log.debug("(vm_destroy) - workspace shutdown command executed successfully.")

        # Sleep for a few seconds to allow for proper shutdown
        log.debug("Waiting %ss for VM to shut down..." % self.VM_SHUTDOWN)
        time.sleep(self.VM_SHUTDOWN)

        # Execute the workspace destroy command: wait for return, stdout to log.
        destroy_return = self.vm_execute(destroy_cmd)

        # Check destroy return code. If successful, continue. Otherwise, set VM to
        # error state (wait, and the polling thread will attempt a destroy later)
        if (destroy_return != 0):
            log.warning("(vm_destroy) - Error in executing workspace destroy command.")
            log.warning("(vm_destroy) - VM was not correctly destroyed. Setting VM to error state and returning error code.")
            vm.status = "Error"
            return destroy_return

        # Return checked out resources And remove VM from the Cluster's 'vms' list
        self.resource_return(vm)
        self.vms.remove(vm)

        # Delete EPR
        os.remove(vm.id)

        log.debug("(vm_destroy) - VM destroyed and removed, cluster updated.")
        return destroy_return


    # TODO: Explain parameters and returns
    def vm_poll(self, vm):

        # Create workspace poll command
        ws_cmd = self.vmpoll_factory(vm.id)
        log.debug("(vm_poll) - Nimbus poll command created:\n%s" % string.join(ws_cmd, " "))

        # Execute the workspace poll (wait, retrieve return code, stdout, and stderr)
        log.debug("(vm_poll) - Executing poll command (wait for completion)...")
        (poll_return, poll_out, poll_err) = self.vm_execwait(ws_cmd)
        log.debug("(vm_poll) - Poll command completed with return code: %d" % poll_return)

        # Check the poll command return
        if (poll_return != 0):
            log.warning("(vm_poll) - Failed polling VM %s (ID: %s)" % (vm.name, vm.id))
            #print "(vm_poll) - STDERR: %s" % poll_err
            log.debug("(vm_poll) - Setting VM status to \'Error\'")
            vm.status = "Error"

            # Return the VM status as a string (exit this method)
            return vm.status

        # Print output, and parse the VM status from it

        #STATE_RE = "State:\s(\w*)$"
        match = re.search(self.STATE_RE, poll_out)
        if match:
            tmp_state = match.group(1)
            # Set VM status:
            if (tmp_state in self.VM_STATES):
                if vm.status != self.VM_STATES[tmp_state]:
                    vm.last_state_change = int(time.time())
                vm.status = self.VM_STATES[tmp_state]
                log.debug("(vm_poll) - VM state: %s, Nimbus state: %s" % (vm.status, tmp_state))
            else:
                log.error("(vm_poll) - Error: state %s not in VM_STATES." % tmp_state)
                log.debug("(vm_poll) - Setting VM status to \'Error\'")
                vm.status = "Error"

        else:
            log.warning("(vm_poll) - Parsing output failed. No regex match. Setting VM status to \'Error\'")
            vm.status = "Error"

        vm.lastpoll = int(time.time())
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
        try:
            sp = Popen(cmd, executable=config.workspace_path, shell=False)
            ret = sp.wait()
            return ret
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
    def vm_execdump(self, cmd, out):
        try:
            sp = Popen(cmd, executable=config.workspace_path, shell=False, stdout=out, stderr=out)
            ret = sp.wait()
            return ret
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
    def vm_execwait(self, cmd):
        try:
            sp = Popen(cmd, executable=config.workspace_path, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            ret = sp.wait()
            (out, err) = sp.communicate(input=None)
            return (ret, out, err)
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (string.join(cmd, " "), e.errno, e.strerror))
            return (-1, "", "")
        except:
            log.error("Problem running %s, unexpected error" % string.join(cmd, " "))
            return (-1, "", "")


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


class EC2Cluster(ICluster):

    VM_STATES = {
            "running" : "Running",
            "pending" : "Starting",
            "shutting-down" : "Shutdown",
            "termimated" : "Shutdown",
    }

    ERROR = 1

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
            #return None

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key


        if self.cloud_type == "AmazonEC2":
            try:
                self.connection = boto.connect_ec2(
                                   aws_access_key_id=self.access_key_id,
                                   aws_secret_access_key=self.secret_access_key,
                                   )
                log.debug("Created a connection to Amazon EC2")

            except boto.exception.EC2ResponseError, e:
                log.error("Couldn't connect to Amazon EC2 because: %s" %
                                                                e.error_message)
                return None

        elif self.cloud_type == "Eucalyptus":
            try:
                region = boto.ec2.regioninfo.RegionInfo(name=self.name,
                                                 endpoint=self.network_address)
                self.connection = boto.connect_ec2(
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
                return None


        elif self.cloud_type == "OpenNebula":

            log.error("OpenNebula support isn't ready yet.")
            raise NotImplementedError
        else:
            log.error("EC2Cluster don't know how to handle a %s cluster." %
                                                               self.cloud_type)
            return None


    def vm_create(self, vm_name, vm_type, vm_networkassoc, vm_cpuarch,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None):

        log.debug("Trying to boot %s on %s" % (vm_type, self.network_address))

        if customization:
            self.user_data = nimbus_xml.ws_optional(customization)
        else:
            self.user_data = ""

        try:
            image = None
            if not "Eucalyptus" == self.cloud_type:
                image = self.connection.get_image(vm_image)

            else: #HACK: for some reason Eucalyptus won't respond properly to
                  #      get_image("whateverimg"). Use a linear search until
                  #      this is fixed
                  # This is Eucalyptus bug #495670
                  # https://bugs.launchpad.net/eucalyptus/+bug/495670
                images = self.connection.get_all_images()
                for potential_match in images:
                    if potential_match.id == vm_image:
                        image = potential_match

            if image:
                reservation = image.run(1,1,
                                        user_data=self.user_data,
                                        security_groups=self.security_groups)
                instance = reservation.instances[0]
                log.debug("Booted VM %s" % instance.id)
            else:
                log.error("Couldn't find image %s on %s" % (vm_image, self.host))
                return self.ERROR

        except boto.exception.EC2ResponseError, e:
            log.error("Couldn't boot VM because: %s" % e.error_message)
            return self.ERROR

        vm_mementry = self.find_mementry(vm_mem)
        if (vm_mementry < 0):
            # At this point, there should always be a valid mementry, as the
            # ResourcePool get_resource methods have selected this cluster
            # based on having an open  memory entry that fits VM requirements.
            log.debug("Cluster memory list has no sufficient memory " +\
                      "entries (Not supposed to happen). Returning error.")
            return self.ERROR
        log.debug("vm_create - Memory entry found in given cluster: %d" %
                                                                    vm_mementry)
        new_vm = VM(name = vm_name, id = instance.id, vmtype = vm_type,
                    clusteraddr = self.network_address,
                    cloudtype = self.cloud_type, network = vm_networkassoc,
                    cpuarch = vm_cpuarch, image= vm_image,
                    memory = vm_mem, mementry = vm_mementry,
                    cpucores = vm_cores, storage = vm_storage)

        new_vm.status = self.VM_STATES.get(instance.state, "Starting")
        self.vms.append(new_vm)

        return 0


    def vm_poll(self, vm):
        log.debug("Polling vm with instance id %s" % vm.id)
        # We should only get on reservation, and one instance back
        try:
            reservations = self.connection.get_all_instances([vm.id])
            instance = reservations[0].instances[0]
        except boto.exception.EC2ResponseError, e:
            log.error("Couldn't update status because: %s" % e.error_message)
            return vm.status
        if vm.status != self.VM_STATES.get(instance.state, "Starting"):
            vm.last_state_change = int(time.time())
        vm.status = self.VM_STATES.get(instance.state, "Starting")
        vm.hostname = instance.public_dns_name
        vm.lastpoll = int(time.time())
        return vm.status


    def vm_destroy(self, vm):
        log.debug("Destroying vm with instance id %s" % vm.id)

        # Kill VM on EC2
        try:
            reservations = self.connection.get_all_instances([vm.id])
            instance = reservations[0].instances[0]
            instance.stop()
        except boto.exception.EC2ResponseError, e:
            log.error("Couldn't destroy vm because: %s" % e.error_message)
            return self.ERROR

        # Delete references to this VM
        self.resource_return(vm)
        self.vms.remove(vm)

        return 0

