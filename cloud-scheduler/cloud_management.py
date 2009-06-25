#!/usr/bin/python

## Auth: Duncan Penfold-Brown. 6/15/2009.

##
## The CloudManager superclass serves as something of an interface for cloud
## management functionality. Each of its subclasses should should correspond to
## a specific implementation for cloud management functionality. That is, each
## subclass should implement the functions in CloudManager according to a specific
## software. Currently, only a Nimbus subclass exists. If support is desired for
## other cloud solutions, an other subclasses (such as an OpenNebula subclass) might 
## also be desired.
##
## To import specific subclasses, simply alter the main scheduler's import lines.
##     e.g.:    from cloud_management import  NimbusCloud
##

class CloudManager:
    
    def workspace_create(self):
        print 'This method should be defined by all subclasses of CloudManager\n'
        assert 0, 'Must define workspace_create'

    def workspace_destroy(self):
        print 'This method should be defined by all subclasses of CloudManager\n'
        assert 0, 'Must define workspace_destroy'

    ## More potential functions: workspace_move, workspace_pause, workspace_resume, etc.



## Implements cloud management functionality with the Nimbus service as part of
## the Globus Toolkit.

class NimbusCloud(CloudManager):

    def workspace_create(self):
        print 'dbg - Nimbus cloud create command'
	print 'dbg - should fork and execute (or possibly just execute) a workspace- \
          control command with the "create" option'
	

    def workspace_destroy(self):
        print 'dbg - Nimbus cloud destroy command'
	print 'dbg - should fork and execute (or possibly just execute) a workspace- \
          control command with the "destroy" option'


