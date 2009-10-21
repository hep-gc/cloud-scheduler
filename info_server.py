#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

# Copyright 2009 University of Victoria
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.




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
            self.server = SimpleXMLRPCServer(("localhost", 8000), requestHandler=RequestHandler, logRequests=False)
            self.server.socket.settimeout(1)
            self.server.register_introspection_functions()

            # Register an instance; all the methods of the instance are
            # published as XML-RPC methods
            class externalFunctions:
                def get_cloud_resources(self):
                    return cloud_resources.get_pool_info()

            self.server.register_instance(externalFunctions())
        except:
            log.error("Couldn't start info server:", sys.exc_info()[0])
            raise SystemExit

    def run(self):

        # Run the server's main loop
        while self.server:
            try:
                self.server.handle_request()
                if self.done:
                    log.info("Killing info server...")
                    break
            except socket.timeout:
                pass

    def stop(self):
        self.done = True
