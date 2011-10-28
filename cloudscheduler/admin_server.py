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
import platform
import re
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

import cloudscheduler.config as config
from cluster_tools import ICluster
from cluster_tools import VM
from cloud_management import ResourcePool
from job_management import Job
from job_management import JobPool
# JSON lib included in 2.6+
if sys.version_info < (2, 6):
    try:
        import simplejson as json
    except:
        raise "Please install the simplejson lib for python 2.4 or 2.5"
else:
    import json

log = None

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class AdminServer(threading.Thread,):

    cloud_resources = None
    job_pool = None
    job_poller = None
    machine_poller = None
    vm_poller = None
    scheduler = None
    cleaner = None
    def __init__(self, c_resources, c_job_pool, c_job_poller, c_machine_poller, c_vm_poller, c_scheduler, c_cleaner):

        global log
        log = logging.getLogger("cloudscheduler")

        #set up class
        threading.Thread.__init__(self, name=self.__class__.__name__)
        self.done = False
        cloud_resources = c_resources
        job_pool = c_job_pool
        job_poller = c_job_poller
        machine_poller = c_machine_poller
        vm_poller = c_vm_poller
        scheduler = c_scheduler
        cleaner = c_cleaner
        host_name = "0.0.0.0"
        #set up server
        try:
            self.server = SimpleXMLRPCServer((host_name,
                                              config.admin_server_port),
                                              requestHandler=RequestHandler,
                                              logRequests=False)
            self.server.socket.settimeout(1)
            #self.server.register_introspection_functions()
        except:
            log.error("Couldn't start info server: %s" % sys.exc_info()[0])
            sys.exit(1)

        # Register an instance; all the methods of the instance are
        # published as XML-RPC methods
        class externalFunctions:
            def get_cloud_resources(self):
                return cloud_resources.get_pool_info()
            def get_cluster_resources(self):
                output = "Clusters in resource pool:\n"
                for cluster in cloud_resources.resources:
                    output += cluster.get_cluster_info_short()+"\n"
                return output
            def get_cluster_vm_resources(self):
                output = VM.get_vm_info_header()
                clusters = 0
                vm_count = 0
                for cluster in cloud_resources.resources:
                    clusters += 1
                    vm_count += len(cluster.vms)
                    output += cluster.get_cluster_vms_info()
                output += '\nTotal VMs: %i. Total Clouds: %i' % (vm_count, clusters)
                return output
            def get_cluster_info(self, cluster_name):
                output = "Cluster Info: %s\n" % cluster_name
                cluster = cloud_resources.get_cluster(cluster_name)
                if cluster:
                    output += cluster.get_cluster_info_short()
                else:
                    output += "Cluster named %s not found." % cluster_name
                return output
            def get_vm_info(self, cluster_name, vm_id):
                output = "VM Info for VM id: %s\n" % vm_id
                cluster = cloud_resources.get_cluster(cluster_name)
                vm = None
                if cluster:
                    vm = cluster.get_vm(vm_id)
                else:
                    output += "Cluster %s not found.\n" % cluster_name
                if vm:
                    output += vm.get_vm_info()
                else:
                    output += "VM with id: %s not found.\n" % vm_id
                return output
            def get_developer_information(self):
                try:
                    from guppy import hpy
                    h = hpy()
                    heap = h.heap()
                    return str(heap)
                except:
                    return "You need to have Guppy installed to get developer " \
                           "information" 
            def get_vm_startup_time(self):
                output = ""
                for cluster in cloud_resources.resources:
                    output += "Cluster: %s " % cluster.name
                    total_time = 0
                    for vm in cluster.vms:
                        pass
                        output += "%d, " % (vm.startup_time if vm.startup_time != None else 0)
                        total_time += (vm.startup_time if vm.startup_time != None else 0)
                    if len(cluster.vms) > 0:
                        output += " Avg: %d " % (int(total_time) / len(cluster.vms))
                return output
            def disable_cloud(self, cloudname):
                return cloud_resources.disable_cluster(cloudname)



        self.server.register_instance(externalFunctions())

    def run(self):

        # Run the server's main loop
        log.info("Started admin server on port %s" % config.admin_server_port)
        while self.server:
            try:
                self.server.handle_request()
                if self.done:
                    log.debug("Killing admin server...")
                    self.server.socket.close()
                    break
            except socket.timeout:
                log.warning("admin server's socket timed out. Don't panic!")

    def stop(self):
        self.done = True

