#!/usr/bin/python

## Auth: Patrick Armstrong. 8/28/2009.
##
## Cloud Scheduler Information Server
## This class implements an XMLRPC Server that serves information about the state
## of the cloud sceduler to information utilities (web interface, command line, whatever)
##
## Based on http://docs.python.org/library/simplexmlrpcserver.html

##
## IMPORTS
##

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class CloudSchedulerInfoServer:

    def __init__(self, cloud_resources):
		
        #create server
        server = SimpleXMLRPCServer(("localhost", 8000), requestHandler=RequestHandler)
        server.register_introspection_functions()

        # Register an instance; all the methods of the instance are
        # published as XML-RPC methods (in this case, just 'div').
        class externalFunctions:
            def get_cloud_resources(self):
                return cloud_resources.get_pool_info()

        server.register_instance(externalFunctions())

        # Run the server's main loop
        server.serve_forever()

