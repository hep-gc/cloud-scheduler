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

import cloudscheduler.utilities as utilities
import cloudscheduler.config as config
from cluster_tools import ICluster
from cluster_tools import VM
from cloud_management import ResourcePool
from job_management import Job
from job_management import JobPool
from proxy_refreshers import MyProxyProxyRefresher

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
            def disable_cloud(self, cloudname):
                return cloud_resources.disable_cluster(cloudname)
            def enable_cloud(self, cloudname):
                return cloud_resources.enable_cluster(cloudname)
            def delete_vm_entry(self, cloudname, vmid):
                return cloud_resources.remove_vm_no_shutdown(cloudname, vmid)
            def delete_all_vm_entry_cloud(self, cloudname):
                return cloud_resources.remove_all_vmcloud_no_shutdown(cloudname)
            def shutdown_cluster_all(self, cloudname):
                return cloud_resources.shutdown_cluster_all(cloudname)
            def shutdown_vm(self, cloudname, vmid):
                return cloud_resources.shutdown_cluster_vm(cloudname, vmid)
            def refresh_job_proxy_user(self, user):
                return MyProxyProxyRefresher.renew_job_proxy_user(job_pool, user)
            def refresh_vm_proxy_user(self, user):
                return MyProxyProxyRefresher.renew_vm_proxy_user(job_pool, user)
            def cloud_resources_reconfig(self):
                cloud_resources.setup()
                return ""
            def change_log_level(self, level):
                log.setLevel(utilities.LEVELS[level])
                return ""
            def perform_quick_shutdown(self):
                scheduler.toggle_quick_exit()
                return ""
            def list_user_limits(self):
                return str(cloud_resources.user_vm_limits)
            def user_limit_reload(self):
                cloud_resources.user_vm_limits = cloud_resources.load_user_limits(config.user_limit_file)
                return True if len(cloud_resources.user_vm_limits) > 0 else False
            def cloud_alias_reload(self):
                if config.target_cloud_alias_file:
                    cloud_resources.target_cloud_aliases = cloud_resources.load_cloud_aliases(config.target_cloud_alias_file)
                    return True if len(cloud_resources.target_cloud_aliases) > 0 else False
                else:
                    return False
            def list_cloud_alias(self):
                return str(cloud_resources.target_cloud_aliases)
            def force_retire_vm(self, cloudname, vmid):
                return cloud_resources.force_retire_cluster_vm(cloudname, vmid)
            def force_retire_all_vm(self, cloudname):
                return cloud_resources.force_retire_cluster_all(cloudname)
            def reset_override_state(self, cloudname, vmid):
                return cloud_resources.reset_override_state(cloudname, vmid)




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

