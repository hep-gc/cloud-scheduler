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

class Cluster:
   
    # Instance variables (preset to defaults)
    name            = 'def_cluster'
    network_address = '0.0.0.0'
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
        (self.name, self.network_address, self.vm_slots, self.cpu_cores, \
          self.storageGB, self.memoryMB, self.x86, self.x86_64, \
          self.network_public, self.network_private) = attr_list;
        
        self.network_private = self.network_private.rstrip("\n");
        print "dbg - Cluster populated successfully"
        
    # Print cluster information
    def print_cluster(self):
        print "-" * 80
        print "Name:\t\t%s" % (self.name)
        print "Address:\t%s" % (self.network_address)
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
    def workspace_create(self):
        print 'This method should be defined by all subclasses of CloudManager\n'
        assert 0, 'Must define workspace_create'

    def workspace_destroy(self):
        print 'This method should be defined by all subclasses of CloudManager\n'
        assert 0, 'Must define workspace_destroy'

    ## More potential functions: workspace_move, workspace_pause, workspace_resume, etc.



## Implements cloud management functionality with the Nimbus service as part of
## the Globus Toolkit.

class NimbusCluster(Cluster):

    # NimbusCluster specific instance variables

    # NimbusCluster specific instance methods
    
    # Overridden constructor
    def __init__(self, ):
        print "dbg - New NimbusCluster created"


    def workspace_create(self):
        print 'dbg - Nimbus cloud create command'
	print 'dbg - should fork and execute (or possibly just execute) a workspace- \
          control command with the "create" option'
	

    def workspace_destroy(self):
        print 'dbg - Nimbus cloud destroy command'
	print 'dbg - should fork and execute (or possibly just execute) a workspace- \
          control command with the "destroy" option'


