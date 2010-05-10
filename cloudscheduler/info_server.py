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
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

import cloudscheduler.config as config
from cluster_tools import ICluster
from cluster_tools import VM
from cloud_management import ResourcePool
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

class CloudSchedulerInfoServer(threading.Thread,):

    cloud_resources = None

    def __init__(self, c_resources):

        global log
        log = logging.getLogger("cloudscheduler")

        #set up class
        threading.Thread.__init__(self)
        self.done = False
        cloud_resources = c_resources
        host_name = "0.0.0.0"
        #set up server
        try:
            self.server = SimpleXMLRPCServer((host_name,
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
            def get_cluster_resources(self):
                output = "Clusters in resource pool:\n"
                for cluster in cloud_resources.resources:
                    output += cluster.get_cluster_info_short()+"\n"
                return output
            def get_cluster_vm_resources(self):
                output = "%-15s %-10s %-15s %-25s\n" % \
                          ("VM ID", "VM TYPE", "STATUS", "CLUSTER")
                for cluster in cloud_resources.resources:
                    output += cluster.get_cluster_vms_info()
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
            def get_json_vm(self, cluster_name, vm_id):
                output = "{}"
                cluster = cloud_resources.get_cluster(cluster_name)
                vm = None
                if cluster:
                    vm = cluster.get_vm(vm_id)
                    if vm:
                        output = VMJSONEncoder().encode(vm)
                return output
            def get_json_cluster(self, cluster_name):
                output = "{}"
                cluster = cloud_resources.get_cluster(cluster_name)
                if cluster:
                    output = ClusterJSONEncoder().encode(cluster)
                return output
            def get_json_resource(self):
                return ResourcePoolJSONEncoder().encode(cloud_resources)
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

class VMJSONEncoder(json.JSONEncoder):
    def default(self, vm):
        if not isinstance (vm, VM):
            log.error("Cannot use VMJSONEncoder on non VM object")
            return
        return {'name': vm.name, 'id': vm.id, 'vmtype': vm.vmtype,
                'hostname': vm.hostname, 'clusteraddr': vm.clusteraddr,
                'cloudtype': vm.cloudtype, 'network': vm.network, 
                'cpuarch': vm.cpuarch, 'imagelocation': vm.imagelocation,
                'memory': vm.memory, 'mementry': vm.mementry, 
                'cpucores': vm.cpucores, 'storage': vm.storage, 
                'status': vm.status}

class ClusterJSONEncoder(json.JSONEncoder):
    def default(self, cluster):
        if not isinstance (cluster, ICluster):
            log.error("Cannot use ClusterJSONEncoder on non Cluster object")
            return
        vmEncodes = []
        for vm in cluster.vms:
            vmEncodes.append(VMJSONEncoder().encode(vm))
        vmDecodes = []
        for vm in vmEncodes:
            vmDecodes.append(json.loads(vm))
        return {'name': cluster.name, 'network_address': cluster.network_address,
                'cloud_type': cluster.cloud_type, 'memory': cluster.memory, 
                'cpu_archs': cluster.cpu_archs, 
                'network_pools': cluster.network_pools, 
                'vm_slots': cluster.vm_slots, 'cpu_cores': cluster.cpu_cores, 
                'storageGB': cluster.storageGB, 'vms': vmDecodes}

class ResourcePoolJSONEncoder(json.JSONEncoder):
    def default(self, res_pool):
        if not isinstance (res_pool, ResourcePool):
            log.error("Cannot use ResourcePoolJSONEncoder on non ResourcePool Object")
            return
        pool = []
        for cluster in res_pool.resources:
            pool.append(ClusterJSONEncoder().encode(cluster))
        poolDecodes = []
        for cluster in pool:
            poolDecodes.append(json.loads(cluster))
        return {'resources': poolDecodes}
