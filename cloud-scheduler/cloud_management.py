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
## Imports
##

from subprocess import Popen
import datetime
import logging
import string
import nimbus_xml

##
## GLOBALS
##

## Log files
#  TODO: Revise logging issue: vm commands and internal messages in the same log
#  Currently: vm command stdout is dumped to a file, and log msgs are sent to a
#   python log object.

# Create a file to dump vm command outputs in to.
vm_logfile = "vm.log"
vm_log = open(vm_logfile, 'w')

# Create a python logger
nimbus_logfile = "nimbus.log"
logging.basicConfig(level=logging.DEBUG, 
                    format="%(asctime)s - %(levelname)s: %(message)s",
		    filename=nimbus_logfile, 
		    filemode='a')


# A simple class for storing a list of Cluster type resources (or Cluster sub-
# classes). Consists of a name and a list.

class ResourcePool:
    
    # Instance variables    
    name = "default"
    resources = []
    
    # Instance methods

    # Constructor
    def __init__(self, name):
        print "dbg - New ResourcePool " + name+ " created"
	self.name = name

    # Add a cluster resource to the pool's resource list
    def add_resource(self, cluster):
        self.resources.append(cluster)

    # Print the name+address of every cluster in the resource pool
    def print_pool(self, ):
        print "Resource pool " + self.name + ":"
        if len(self.resources) == 0:
	    print "Pool is empty... Cannot print pool"
	else:
	    for cluster in self.resources:
	        print "\t" + cluster.name + "\t" + cluster.cloud_type + "\t" + \
		  cluster.network_address
    
    # Return an arbitrary resource from the 'resources' list. Does not remove
    # the returned element from the list.
    # (Currently, the first cluster in the list)
    # TODO: overload with different parameters to return matching resources?
    def get_resource(self, ):
	if len(self.resources) == 0:
	    # TODO: Throw exception instead of exiting?
	    print "Pool is empty... Cannot return resource."
	    sys.exit(1)
	else:
	    return (self.resources[0])
	    


# The Cluster superclass, containing all general cluster instance variables
# and Cluster interface methods (stubs for implementation in subclasses).

class Cluster:
   
    ## Instance variables (preset to defaults)
    # TODO: Change network fields to lists. Add fields discussed in clouddev meetings
    name            = 'default-cluster'
    network_address = '0.0.0.0'
    cloud_type      = 'default-type'
    vm_slots        = 0
    cpu_cores       = 0
    storageGB       = 0
    memoryMB        = 0
    x86             = 'no'
    x86_64          = 'no'
    network_public  = 'no'
    network_private = 'no'
    
    # Running vms list (uses best-effort internal representation of resources)
    vms = []

    ## Instance methods

    # Constructor
    def __init__(self, ):
        print "dbg - New Cluster created"

    # Set Cluster attributes from a parameter list
    def populate(self, attr_list):
        (self.name, self.network_address, self.cloud_type, self.vm_slots, self.cpu_cores, \
          self.storageGB, self.memoryMB, self.x86, self.x86_64, \
          self.network_public, self.network_private) = attr_list;
        
	# Convert numerical fields to ints
        self.vm_slots = int(self.vm_slots)
	self.cpu_cores = int(self.cpu_cores)
	self.storageGB = int(self.storageGB)
	self.memoryMB = int(self.memoryMB)

        self.network_private = self.network_private.rstrip("\n");
        print "dbg - Cluster populated successfully"
        
    # Print cluster information
    def print_cluster(self):
        print "-" * 80
        print "Name:\t\t%s" % (self.name)
        print "Address:\t%s" % (self.network_address)
	print "Type:\t\t%s" % (self.cloud_type)
        print "VM Slots:\t%s" % (self.vm_slots)
        print "CPU Cores:\t%s" % (self.cpu_cores)
        print "Storage:\t%s" % (self.storageGB)
        print "Memory:\t\t%s" % (self.memoryMB)
        print "x86:\t\t%s" % (self.x86)
        print "x86_64:\t\t%s" % (self.x86_64)
        print "Network Pub:\t%s" % (self.network_public)
        print "Network Priv:\t%s" % (self.network_private)
        print "-" * 80
    
    # Print a short form of cluster information
    def print_short(self):
        print ">CLUSTER Name: %s, Address: %s, Type: %s, VM slots: %d, Mem: %d" \
	  % (self.name, self.network_address, self.cloud_type, self.vm_slots, \
	  self.memoryMB)

    # Workspace manipulation methods
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
        print 'This method should be defined by all subclasses of Cluster\n'
        assert 0, 'Must define workspace_create'

    def vm_destroy(self, vm_id):
        print 'This method should be defined by all subclasses of Cluster\n'
        assert 0, 'Must define workspace_destroy'
    
    def vm_poll(self, vm_id):
        print 'This method should be defined by all subclasses of Cluster\n'
        assert 0, 'Must define workspace_poll'
        
    ## More potential functions: vm_move, vm_pause, vm_resume, etc.



## Implements cloud management functionality with the Nimbus service as part of
## the Globus Toolkit.

class NimbusCluster(Cluster):

    ## NimbusCluster specific instance variables
    
    # Global Nimbus command variables
    VM_DURATION = "1000"
    VM_TARGETSTATE = "Running"
    
    
    ## NimbusCluster specific instance methods
    
    # Overridden constructor
    def __init__(self, ):
        print "dbg - New NimbusCluster created"

    def vm_create(self, vm_name, vm_networkassoc, vm_cpuarch, vm_imagelocation,\
      vm_mem):
        
	print "dbg - Nimbus cloud create command"

        # Creates a workspace metadata xml file from passed parameters
        vm_metadata = nimbus_xml.ws_metadata_factory(vm_name, vm_networkassoc, \
	  vm_cpuarch, vm_imagelocation)
	
	# Set a timestamp for VM creation
	now = datetime.datetime.now()
	
	# Create an EPR file name (unique with timestamp)
	vm_epr = "nimbusVM_" + now.isoformat() + ".epr"

        # Create the workspace command as a list (private method)
	ws_cmd = self.vmcreate_factory(vm_epr, vm_metadata, self.VM_DURATION, vm_mem, \
	  self.VM_TARGETSTATE)
	logging.debug("Nimbus: workspace create command prepared.")
	logging.debug("Command: " + string.join(ws_cmd, " "))

	# Execute a no-wait workspace command with the above cmd list.
	# Returns immediately to parent program. Subprocess continues to execute, writing to log.
	# stdin, stdout, and stderr set the filehandles for streams. PIPE opens a pipe to stream
	# (PIPE streams are accessed via popen_object.stdin/out/err)
	# Can also specify a filehandle or file object, or None (default).
	sp = Popen(ws_cmd, executable="workspace", shell=False, stdout=vm_log, stderr=vm_log)
	logging.debug("Nimbus: workspace create command executed.")

	# Add the newly created VM to the cluster list
	self.vms.append(vm_epr)

	# TODO: Maintain as-correct-as-possible internal information at all times.
	#       More refined resource subtraction is needed.
	# TODO: If clusters have worker-node granular information, subtract memory from
	#       a specific node.
	self.vm_slots = self.vm_slots - 1
	self.memoryMB = self.memoryMB - vm_mem       
	
	logging.info("Nimbus: Workspace create command executed. VM created and stored, cluster updated.")

    
    def vm_destroy(self, vm_id):
        print 'dbg - Nimbus cloud destroy command'
        
        # TODO: Poll vm to check vm state. If not destroyed, destroy.
	#       If destroyed, remove epr from 'vms' list.

	# Create the workspace command with destroy option as a list (priv.)
	ws_cmd = self.vmdestroy_factory(vm_id)
	logging.debug("Nimbus: workspace destroy command prepared.")
	logging.debug("Command: " + string.join(ws_cmd, " "))

	# Execute the workspace command: no-wait, stdout to log.
	sp = Popen(ws_cmd, executable="workspace", shell=False, stdout=vm_log, stderr=vm_log)
	logging.debug("Nimbus: workspace destroy command executed.")

	# TODO: Poll the vm to ensure it is properly destroyed? 
	#       Try to get most accurate info., or assume destroy works?

	# Return resources to cluster (currently only vm_slots and memory) and
	# remove VM epr from 'vms' list
	# TODO: Resolve issue of returning memory: How to know how much memory
	#       to return?
	#       Sol'n: store an (epr_file, mem) pair in the 'vms' list?
	self.vm_slots += 1
	#self.memoryMB += vm_mem
	vms.remove(vm_id)

	logging.info("Nimbus: Workspace destroy command executed. \
	  VM destroyed and removed, cluster updated.")


    def vm_poll(self, vm_id):
        print 'dbg - Nimbus cloud poll command'
	print 'dbg - should fork and execute (or possibly just execute) a workspace- \
          control command with the "rpquery" option'


    ## NimbusCluster private methods

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
           "--exit-state", "Running",        # Running, Paused, Propagated - hard set.
           "--dryrun",                       # TODO: Remove dryrun tag for full vm creation
          ]

        # Return the workspace command list
	return ws_list

    def vmdestroy_factory(self, epr_file):
	ws_list = [ "workspace", "-e", epr_file, "--destroy"]
	return ws_list

    def vmpoll_factory(self, epr_file):
	ws_list = [ "workspace", "-e", epr_file, "--rpquery"]
	return ws_list








