#!/usr/bin/python

## Auth: Duncan Penfold-Brown. 6/15/2009.

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
import nimbus_xml


##
## GLOBALS
##

# Log files
# TODO: Resolve: separate log files for each cluster, each cluster class, or 
#       single log file? How do we atomically write to one log? A log thread
#       with a queue? Log messages are written to tmp handles, which are queued,
#       and then closed by the write thread after writing?
#
# NOTE: Use python's log modules. Investigate.
#  
nimbus_logfile = "nimbus.log"
ws_log = open(nimbus_logfile, "a")


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
	    print "Pool is empty..."
	else:
	    for cluster in self.resources:
	        print "\t" + cluster.name + "\t" + cluster.cloud_type + "\t" + \
		  cluster.network_address
	    


# The Cluster superclass, containing all general cluster instance variables
# and Cluster interface methods (stubs for implementation in subclasses).

class Cluster:
   
    # Instance variables (preset to defaults)
    # TODO: Add storage for cluster type ("NIMBUS", "OPENNEBULA", etc.)
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

    # Instance methods

    # Constructor
    def __init__(self, ):
        print "dbg - New Cluster created"

    # Set Cluster attributes from a parameter list
    def populate(self, attr_list):
        (self.name, self.network_address, self.cloud_type, self.vm_slots, self.cpu_cores, \
          self.storageGB, self.memoryMB, self.x86, self.x86_64, \
          self.network_public, self.network_private) = attr_list;
        
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
    VM_DURATION = 1000
    VM_TARGETSTATE = "Running"
    
    # A list containing all deployed virtual machines (epr_files)
    vms = []

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
	ws_cmd = vmcreate_factory(vm_epr, vm_metadata, VM_DURATION, vm_mem, VM_TARGETSTATE)
	
	# Prep log 
        ws_log.write("-"*20 + "NIMBUS" + "-"*54 + "\n")
        ws_log.write( str(now) )
        ws_log.write(":\n")

	# Execute a no-wait workspace command with the above cmd list.
	# Returns immediately to parent program. Subprocess continues to execute, writing to log.
	# stdin, stdout, and stderr set the filehandles for streams. PIPE opens a pipe to stream
	# (PIPE streams are accessed via popen_object.stdin/out/err)
	# Can also specify and filehandle or file object, or None (default).
	sp = Popen(ws_cmd, executable="workspace", shell=False, stdout=ws_log, stderr=ws_log)

	# Add the newly created VM to the cluster list
	self.vms.append(vm_epr)

	# TODO: Change the cluster variables to reflect resources allocated to this vm.
	#       Maintain as-correct-as-possible internal information at all times.
	self.vm_slots = self.vm_slots - 1
	self.memoryMB = self.memoryMB - vm_mem       
	# TODO: If clusters have worker-node granular information, subtract memory from
	#       a specific node.

    
    def vm_destroy(self, vm_id):
        print 'dbg - Nimbus cloud destroy command'
	print 'dbg - should fork and execute (or possibly just execute) a workspace- \
          control command with the "destroy" option'
    
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
           "-s", "https://" + self.network_address  + "8443/wsrf/services/WorkspaceFactoryService",
           "--deploy-duration", duration,    # minutes
           "--deploy-mem", mem,              # megabytes
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








