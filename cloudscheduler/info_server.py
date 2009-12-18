#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

# Copyright (C) 2009 University of Victoria
# You may distribute under the terms of either the GNU General Public
# License or the Apache v2 License, as specified in the README file.

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
import logging
import threading
import time
import socket
import sys
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

import cloudscheduler.config as config

log = logging.getLogger("CloudLogger")

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class CloudSchedulerInfoServer(threading.Thread,):

    cloud_resources = None

    def __init__(self, c_resources):
        #set up class
        threading.Thread.__init__(self)
        self.done = False
        cloud_resources = c_resources

        #set up server
        try:
            self.server = SimpleXMLRPCServer(("localhost",
                                              config.info_server_port),
                                              requestHandler=RequestHandler,
                                              logRequests=False)
            self.server.socket.settimeout(1)
            self.server.register_introspection_functions()
        except:
            log.error("Couldn't start info server: %s" % sys.exc_info()[0])
            sys.exit(1)

        # Register an instance; all the methods of the instance are
        # published as XML-RPC methods
        class externalFunctions:
            def get_cloud_resources(self):
                return cloud_resources.get_pool_info()
            def get_developer_information(self):
                try:
                    from guppy import hpy
                    h = hpy()
                    heap = h.heap()
                    return str(heap)
                except:
                    return "You need to have Guppy installed to get developer " \
                           "information" 

        self.server.register_instance(externalFunctions())

    def run(self):

        # Run the server's main loop
        log.info("Started info server on port %s" % config.info_server_port)
        while self.server:
            try:
                self.server.handle_request()
                if self.done:
                    log.debug("Killing info server...")
                    break
            except socket.timeout:
                pass

    def stop(self):
        self.done = True
