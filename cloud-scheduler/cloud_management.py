#!/usr/bin/python

## Auth: Duncan Penfold-Brown. 6/15/2009.

## CLOUD MANAGEMENT
##
## The Cluster superclass provides the basic structure for cluster information,
## and provides the framework (interface) for cloud management functionality.
## Each of its subclasses should should correspond to a specific implementation 
## for cloud management functionality. That is, each subclass should implement 
## the functions in the Cluster superclass according to a specific software. 
## Currently, only a Nimbus subclass exists. If support is desired for other 
## cloud solutions, other subclasses (such as an OpenNebula subclass) might 
## also be desired.
##
## To import specific subclasses only, simply alter the cloud_scheduler import lines.
##     e.g.:    from cloud_management import  NimbusCluster
##


##
## IMPORTS
##

from subprocess import Popen
import subprocess
import datetime
import sys
import logging
import string
import re
import nimbus_xml

##
## GLOBALS
##

## Log files
#  TODO: Revise logging issue: vm commands and internal messages in the same log
#  Currently: vm command stdout is dumped to a file, and all debug information 
#             is printed to screen.

# Create a file to dump vm command outputs in to.
vm_logfile = "vm.log"
vm_log = open(vm_logfile, 'w')

# Create a python logger
log = logging.getLogger("CloudLogger")

nimbus_logfile = "nimbus.log"
logging.basicConfig(level=logging.DEBUG, 
                    format="%(asctime)s - %(levelname)s: %(message)s",
		    filename=nimbus_logfile, 
		    filemode='a')

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
    #              - by cloud software (Nimbus: epr file. OpenNebula: id number, etc.)
    # clusteraddr  - (str) The address of the cluster hosting the VM
    # type         - (str) The cloud type of the VM (Nimbus, OpenNebula, etc)
    # network      - (str) The network association the VM uses
    # cpuarch      - (str) The required CPU architecture of the VM
    # imagelocation- (str) The location of the image from which the VM was created
    # memory       - (int) The memory used by the VM
    # mementry     - (int) The index of the entry in the host cluster's memory list
    #                from which this VM is taking memory
    def __init__(self, name, id, clusteraddr, type, network, cpuarch, imagelocation,\
      memory, mementry):
        log.debug("New VM object created:")
	log.debug("VM - Name: %s, id: %s, host: %s, image: %s, memory: %d" \
	  % (name, id, clusteraddr, imagelocation, memory))
	self.name = name
	self.id = id
	self.clusteraddr = clusteraddr
	self.type = type
	self.network = network
	self.cpuarch = cpuarch
	self.imagelocation = imagelocation
	self.memory = memory
	self.mementry = mementry

	# Set a status variable on new creation
	self.status = "Starting"

    def log_short(self):
        log.debug( spacer + "VM Name: %s, ID: %s, Status: %s" % (self.name, self.id, self.status))

    # Print a short description of the VM
    # spacer - (str) a string to prepend to each VM line being printed
    def print_short(self, spacer):
        log.warning("print_short is DEPRECATED, use log_short instead")
        self.log_short()



# A simple class for storing a list of Cluster type resources (or Cluster sub-
# classes). Consists of a name and a list.

class ResourcePool:
    
    ## Instance variables    
    resources = []
    
    ## Instance methods

    # Constructor
    # name   - The name of the ResourcePool being created
    def __init__(self, name):
        log.debug("New ResourcePool " + name + " created")
	self.name = name

    # Add a cluster resource to the pool's resource list
    def add_resource(self, cluster):
        self.resources.append(cluster)

    # Print the name and address of every cluster in the resource pool
    def print_pool(self, ):
        log.warning("print_pool is DEPRECATED, use get_pool_info instead")
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
    #    TODO: storage   - the amount of scratch space the VM requires
    #    TODO: cpucores  - the number of cores that a VM requires (dedicated? or general?)
    # Return: returns a Cluster object if one is found that vits VM requirments
    #         Otherwise, returns the 'None' object
    def get_resourceFF(self, network, cpuarch, memory):
        if len(self.resources) == 0:
	    print "Pool is empty... Cannot return FF resource"
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
	    
	    # Return the cluster as an available resource (meets all job reqs)
	    return cluster
	
	# If no clusters are found (no clusters can host the required VM)
	return None


# The Cluster superclass, containing all general cluster instance variables
# and Cluster interface methods (stubs for implementation in subclasses).

class Cluster:
   
    ## Instance variables (preset to defaults)
    name            = 'default-cluster'
    network_address = '0.0.0.0'
    cloud_type      = 'default-type'
    vm_slots        = 0
    cpu_cores       = 0
    storageGB       = 0
    
    # A list of the memory available on each cluster worker node
    memory = []
    # A list of the network pools made available to VMs
    network_pools = []
    # A list of the available CPU architectures on the cluster
    cpu_archs = [] 
    # Running vms list (uses best-effort internal representation of resources)
    # A list of VM objects
    vms = []
    
    
    ## Instance methods

    # Constructor
    def __init__(self, ):
        log.debug("New Cluster created")

    # Set Cluster attributes from a parameter list
    def populate(self, attr_list):
        (self.name, self.network_address, self.cloud_type, self.vm_slots, self.cpu_cores, \
          self.storageGB, memory, cpu_archs, network_pools) = attr_list;
        
	# Strip the newline from the last config line item (network_pools string)
        network_pools = network_pools.rstrip("\n")

	# Split strings into lists for list fields (memory, cpuarchs, networkpools)
	self.memory = memory.split(",")
	self.cpu_archs = cpu_archs.split(",")
	self.network_pools = network_pools.split(",")
	
	# Convert numerical fields to ints
        self.vm_slots = int(self.vm_slots)
	self.cpu_cores = int(self.cpu_cores)
	self.storageGB = int(self.storageGB)
	# Set all self.memory values to ints (iterate through memory list)
	for i in range(len(self.memory)):
	    self.memory[i] = int(self.memory[i])
	    
        log.debug("Cluster populated successfully")
        
    # Print cluster information
    def print_cluster(self):
        log.warning("print_cluster is DEPRECATED")
        print "-" * 80
        print "Name:\t\t%s"        % self.name
        print "Address:\t%s"       % self.network_address
	print "Type:\t\t%s"        % self.cloud_type
        print "VM Slots:\t%s"      % self.vm_slots
        print "CPU Cores:\t%s"     % self.cpu_cores
        print "Storage:\t%s"       % self.storageGB
        print "Memory:\t\t%s"      % self.memory
        print "CPU Archs:\t%s"     % string.join(self.cpu_archs, ", ")
        print "Network Pools:\t%s" % string.join(self.network_pools, ", ")	
        print "-" * 80
    
    # Print a short form of cluster information
    def print_short(self):
       
        log.debug("CLUSTER Name: %s, Address: %s, Type: %s, VM slots: %d, Mem: %s" \
	  % (self.name, self.network_address, self.cloud_type, self.vm_slots, \
	  self.memory))

    # Print the cluster 'vms' list (via VM print)
    def print_vms(self):
        if len(self.vms) == 0:
	    log.debug("CLUSTER %s has no running VMs..." % (self.name))
	else:
	    log.debug("CLUSTER %s running VMs:" % (self.name))
            for vm in self.vms:
	        vm.print_short("\t")
    

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
    
    def vm_create(self, vm_name, vm_networkassoc, vm_cpuarch, vm_imagelocation, \
      vm_mem):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_create'

    def vm_destroy(self, vm):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_destroy'
    
    def vm_poll(self, vm):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_poll'
        
    ## More potential functions: vm_move, vm_pause, vm_resume, etc.


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
	 "Paused"         : "Running",   # TODO: Include a paused state? Will CS support pausing?
	 "TransportReady" : "Running",
	 "StagedOut"      : "Running",
	 "Corrupted"      : "Error",
	 "Cancelled"      : "Error",
    }
    
    ## NimbusCluster specific instance methods
    
    # Overridden constructor
    def __init__(self, ):
        log.debug("New NimbusCluster created")

    
    # TODO: Explain parameters and returns
    def vm_create(self, vm_name, vm_networkassoc, vm_cpuarch, vm_imagelocation,\
      vm_mem):
        
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

	# Execute the workspace create commdand: returns immediately. Dump
	# to vm_log
	create_return = self.vm_execdump(ws_cmd, vm_log)
	if (create_return != 0):
	    log.debug("vm_create - Error in executing workspace create command.")
	    log.debug("vm-create - VM %s (ID: %s) not created. Returning error code." \
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
	new_vm = VM(vm_name, vm_epr, self.network_address, self.cloud_type, \
	  vm_networkassoc, vm_cpuarch, vm_imagelocation, vm_mem, vm_mementry)
	
	# Add the new VM object to the cluster's vms list And check out required resources
	self.vms.append(new_vm)
	self.resource_checkout(new_vm)
	
	log.debug("(vm_create) - VM created and stored, cluster updated.")
	return create_return


    # TODO: Explain parameters, returns, and purpose
    def vm_recreate(self, vm):
        print 'dbg - Nimbus cloud destroy and create commands'

	# Store VM attributes before destroy
	vm_name    = vm.name
	vm_id      = vm.id
	vm_network = vm.network
	vm_cpuarch = vm.cpuarch
	vm_imagelocation = vm.imagelocation
	vm_memory  = vm.memory

        # Print VM parameters
	print "(vm_recreate) - name: %s network: %s cpuarch: %s imageloc: %s memory: %d" \
	  % (vm_name, vm_network, vm_cpuarch, vm_imagelocation, vm_memory)

        # Call destroy on the given VM
	print "(vm_recreate) - Destroying VM %s..." % vm_name
	destroy_ret = self.vm_destroy(vm)
	if (destroy_ret != 0):
	    print "(vm_recreate) - Destroying VM failed. Aborting recreate."
	    return destroy_ret
	
	# Call create with the given VM's parameters
	print "(vm_recreate) - Recreating VM %s..." % vm_name
        create_ret = self.vm_create(vm_name, vm_network, vm_cpuarch, \
	  vm_imagelocation, vm_memory)
	if (create_ret != 0):
	    print "(vm_recreate) - Recreating VM %s failed. Aborting recreate."
	    return create_ret

        # Print success message and return
	print "(vm_recreate) - VM %s successfully recreated." % vm_name
	return create_ret
   

    # TODO: Explain parameters and returns
    def vm_reboot(self, vm):
        print 'dbg - Nimbus cloud reboot VM command'

	# Create workspace reboot command as a list (priv. method)
	ws_cmd = self.vmreboot_factory(vm.id)
	print "(vm_reboot) - workspace reboot command prepared."
	print "(vm_reboot) - Command: " + string.join(ws_cmd, " ")
	
	# Execute the reboot command: wait for return, stdout to log.
	reboot_return = self.vm_execdump(ws_cmd, vm_log)

	# Check reboot return code. If successful, continue. Otherwise, set
	# VM state to "Error" and return.
	if (reboot_return != 0):
	    print "(vm_reboot) - Error in executing workspace reboot command."
	    print "(vm_reboot) - VM failed to reboot. Setting VM to error state and returning error code."
	    # Causes fatal exception. ??
	    #print "(vm_reboot) - VM %s failed to reboot. Setting vm status to \'Error\' and returning error code." % vm.name
	    vm.status = "Error"
	    return reboot_return 
	    
	# Set state to initial default state "Starting" and return
	vm.status = "Starting"
	print "(vm_reboot) - workspace reboot command executed. VM rebooting..."
	return reboot_return


    # TODO: Explain parameters and returns
    def vm_destroy(self, vm):
        log.debug('Nimbus cloud destroy command')
        
	# Create the workspace command with destroy option as a list (priv.)
	ws_cmd = self.vmdestroy_factory(vm.id)
	log.debug("(vm_destroy) - workspace destroy command prepared.")
	log.debug("(vm_destroy) - Command: " + string.join(ws_cmd, " "))

	# Execute the workspace command: wait for return, stdout to log.
	destroy_return = self.vm_execdump(ws_cmd, vm_log)
        
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
	# Execute a no-wait workspace command with the passed cmd list.
	# Returns immediately to parent program. Subprocess continues to execute, writing to stdout.
	# stdin, stdout, and stderr params set the filehandles for streams. PIPE opens a pipe to stream
	# (PIPE streams are accessed via popen_object.stdin/out/err)
	# Can also specify a filehandle or file object, or None (default).
	# At present, dumps all stdout and stderr to a logfile, 'vm_log'
	sp = Popen(cmd, executable="workspace", shell=False)
    
    # A command execution with stdout and stderr output destination specified as a filehandle.
    # Waits on the command to finish, and returns the command's return code.
    # Parameters:
    #    cmd   - A list containing the command to execute.
    #    out   - A filehandle or file object into which stdout and stderr streams are
    #            dumped.
    # Returns:
    #    ret   - The return value of the executed command
    def vm_execdump(self, cmd, out):
	sp = Popen(cmd, executable="workspace", shell=False, stdout=out, stderr=out)
	ret = sp.wait()
	return ret
   
    # As above, a function to encapsulate command execution via Popen.
    # vm_execwait executes the given cmd list, waits for the process to finish,
    # and returns the return code of the process. STDOUT and STDERR are stored 
    # in given parameters.
    # Parameters:
    #    (cmd as above)
    #    out   - A string to store the STDOUT of the executed command 
    #    err   - A string to store the STDERR of the executed command
    # Returns:
    #    ret   - The return value of the executed command
    #    out   - The STDOUT of the executed command
    #    err   - The STDERR of the executed command
    # The return of this function is a 3-tuple
    def vm_execwait(self, cmd):
        sp = Popen(cmd, executable="workspace", shell=False, stdout=subprocess.PIPE, \
	  stderr=subprocess.PIPE)
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

    
