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

class InfoServer(threading.Thread,):

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
                output = []
                output.append("Clusters in resource pool:\n")
                for cluster in cloud_resources.resources:
                    output.append(cluster.get_cluster_info_short())
                    output.append("\n")
                return ''.join(output)
            def get_cluster_vm_resources(self):
                output = []
                output.append(VM.get_vm_info_header())
                clusters = 0
                vm_count = 0
                for cluster in cloud_resources.resources:
                    clusters += 1
                    vm_count += len(cluster.vms)
                    output.append(cluster.get_cluster_vms_info())
                output.append('\nTotal VMs: %i. Total Clouds: %i' % (vm_count, clusters))
                return ''.join(output)
            def get_cluster_info(self, cluster_name):
                output = []
                output.append("Cluster Info: %s\n" % cluster_name)
                cluster = cloud_resources.get_cluster(cluster_name)
                if cluster:
                    output.append(cluster.get_cluster_info_short())
                else:
                    output.append("Cluster named %s not found." % cluster_name)
                return ''.join(output)
            def get_vm_info(self, cluster_name, vm_id):
                output = []
                output.append("VM Info for VM id: %s\n" % vm_id)
                cluster = cloud_resources.get_cluster(cluster_name)
                vm = None
                if cluster:
                    vm = cluster.get_vm(vm_id)
                else:
                    output.append("Cluster %s not found.\n" % cluster_name)
                if vm:
                    output.append(vm.get_vm_info())
                else:
                    output.append("VM with id: %s not found.\n" % vm_id)
                return ''.join(output)
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
            def get_newjobs(self):
                output = []
                jobs = job_pool.job_container.get_unscheduled_jobs()
                output.append(Job.get_job_info_header())
                for job in jobs:
                    output.append(job.get_job_info())
                return ''.join(output)
            def get_schedjobs(self):
                output = []
                jobs = job_pool.job_container.get_scheduled_jobs()
                output.append(Job.get_job_info_header())
                for job in jobs:
                    output.append(job.get_job_info())
                return ''.join(output)
            def get_highjobs(self):
                output = []
                jobs = job_pool.job_container.get_high_priority_jobs()
                output.append(Job.get_job_info_header())
                for job in jobs:
                    output.append(job.get_job_info())
                return ''.join(output)
            def get_idlejobs(self):
                output = []
                jobs = job_pool.job_container.get_idle_jobs()
                output.append(Job.get_job_info_header())
                for job in jobs:
                    output.append(job.get_job_info())
                return ''.join(output)
            def get_runningjobs(self):
                output = []
                jobs = job_pool.job_container.get_running_jobs()
                output.append(Job.get_job_info_header())
                for job in jobs:
                    output.append(job.get_job_info())
                return ''.join(output)
            def get_completejobs(self):
                output = []
                jobs = job_pool.job_container.get_complete_jobs()
                output.append(Job.get_job_info_header())
                for job in jobs:
                    output.append(job.get_job_info())
                return ''.join(output)
            def get_heldjobs(self):
                output = []
                jobs = job_pool.job_container.get_held_jobs()
                output.append(Job.get_job_info_header())
                for job in jobs:
                    output.append(job.get_job_info())
                return ''.join(output)
            def get_job(self, jobid):
                output = "Job not found."
                job = job_pool.job_container.get_job_by_id(jobid)
                if job != None:
                    output = job.get_job_info_pretty()
                return output
            def get_json_job(self, jobid):
                job_match = job_pool.job_container.get_job_by_id(jobid)
                return JobJSONEncoder().encode(job_match)
            def get_json_jobpool(self):
                return JobPoolJSONEncoder().encode(job_pool)
            def get_ips_munin(self):
                output = []
                for cluster in cloud_resources.resources:
                    for vm in cluster.vms:
                        if re.search("(10|192\.168|172\.(1[6-9]|2[0-9]|3[01]))\.", vm.ipaddress):
                            continue
                        else:
                            output.append("[%s]\n\taddress %s\n" % (vm.hostname, vm.ipaddress))
                return ''.join(output)
            def get_vm_startup_time(self):
                output = []
                for cluster in cloud_resources.resources:
                    output.append("Cluster: %s " % cluster.name)
                    total_time = 0
                    for vm in cluster.vms:
                        pass
                        output.append("%d, " % (vm.startup_time if vm.startup_time != None else 0))
                        total_time += (vm.startup_time if vm.startup_time != None else 0)
                    if len(cluster.vms) > 0:
                        output.append(" Avg: %d " % (int(total_time) / len(cluster.vms)))
                return ''.join(output)
            def get_diff_types(self):
                output = []
                current_types = cloud_resources.vmtype_distribution()
                desired_types = job_pool.job_type_distribution()
                # Negative difference means will need to create that type
                diff_types = {}
                for vmtype in current_types.keys():
                    if vmtype in desired_types.keys():
                        diff_types[vmtype] = current_types[vmtype] - desired_types[vmtype]
                    else:
                        diff_types[vmtype] = 1 # changed from 0 to handle users with multiple job types, back to 0 from 1.
                for vmtype in desired_types.keys():
                    if vmtype not in current_types.keys():
                        diff_types[vmtype] = -desired_types[vmtype]
        
                # With user limiting will need to reset any users that are at their limits
                # so they will not interfere with scheduling
                # will need to redistribute negatives to the non-limited users
                limited_users = []
                userjoblimits = job_pool.get_usertype_limits()
                for vmusertype in diff_types.keys():
                    user = vmusertype.split(':')[0]
                    if cloud_resources.user_at_limit(user):
                        if vmusertype not in limited_users:
                            limited_users.append(vmusertype)
                    if vmusertype in userjoblimits.keys():
                        if cloud_resources.uservmtype_at_limit(vmusertype, userjoblimits[vmusertype]):
                            if vmusertype not in limited_users:
                                limited_users.append(vmusertype)
                neg_total = 0
                for usertype in limited_users:
                    if diff_types[usertype] < 0:
                        neg_total += diff_types[usertype]
                splitby = len(diff_types) - len(limited_users)
                adjustby = 0
                if splitby > 0:
                    adjustby = neg_total / splitby
                elif splitby == 0:
                    log.verbose("All users are limited.")
                else:
                    log.error("More user vmtypes limited than what's in diff types, something weird here.")
        
                for usertype in diff_types.keys():
                    if usertype not in limited_users:
                        diff_types[usertype] += adjustby # the 'extra' will be negative so add it
                        
                #for type in current_types.keys():
                    #if type in desired_types.keys():
                        #diff_types[type] = current_types[type] - desired_types[type]
                    #else:
                        #diff_types[type] = 1 # changed from 0 to handle users with multiple job types
                #for type in desired_types.keys():
                    #if type not in current_types.keys():
                        #diff_types[type] = -desired_types[type]
                output.append("Diff Types dictionary\n")
                for key, value in diff_types.iteritems():
                    output.append("type: %s, dist: %f\n" % (key, value))
                output.append("Current Types (vms)\n")
                for key, value in current_types.iteritems():
                    output.append("type: %s, dist: %f\n" % (key, value))
                output.append("Desired Types (jobs)\n")
                for key, value in desired_types.iteritems():
                    output.append("type: %s, dist: %f\n" % (key, value))
                return ''.join(output)

            def get_vm_job_run_times(self):
                output = []
                output.append("Run Times of Jobs on VMs\n")
                for cluster in cloud_resources.resources:
                    for vm in cluster.vms:
                        output.append("%s : avg %f\n" % (vm.hostname, vm.job_run_times.average()))
                return ''.join(output)
            def get_cloud_config_values(self):
                return cloud_resources.get_cloud_config_output()
            def get_total_vms(self):
                return str(cloud_resources.vm_count())
            def get_total_vms_cloud(self, cluster_name):
                output = []
                cluster = cloud_resources.get_cluster(cluster_name)
                if cluster:
                    output.append(str(cluster.num_vms()))
                else:
                    output.append("Cluster named %s not found." % cluster_name)
                return ''.join(output)
            def check_shared_objs(self):
                output = []
                output.append("Scheduler Thread:\n" + scheduler.check_shared_objs())
                output.append("\n")
                output.append("Cleanup Thread:\n" + cleaner.check_shared_objs())
                output.append("\n")
                output.append("VMPoller Thread:\n" + vm_poller.check_shared_objs())
                output.append("\n")
                output.append("JobPoller Thread:\n" + job_poller.check_shared_objs())
                output.append("\n")
                output.append("MachinePoller Thread:\n" + machine_poller.check_shared_objs())
                output.append("\n")
                return ''.join(output)
            def get_vm_stats(self, cluster_name=""):
                vms = []
                if cluster_name:
                    cluster = cloud_resources.get_cluster(cluster_name)
                    if not cluster:
                        return "Could not find cloud: %s - check cloud_status for list of available clouds" % cluster_name
                    vms.extend(cluster.vms)
                else:
                    for cluster in cloud_resources.resources:
                        vms.extend(cluster.vms)
                state_count = {'Running':0, 'Starting':0, 'Error':0, 'Retiring':0, 'ExpiredProxy':0, 'NoProxy':0, 'ConnectionRefused':0}
                for vm in vms:
                    if vm.override_status:
                        state_count[vm.override_status] += 1
                    else:
                        state_count[vm.status] += 1
                output = []
                for state, count in state_count.iteritems():
                    if count > 0:
                        output.extend([str(count), ' ', state, ','])
                return ''.join(output)

        self.server.register_instance(externalFunctions())

    def run(self):

        # Run the server's main loop
        log.info("Started info server on port %s" % config.info_server_port)
        while self.server:
            try:
                self.server.handle_request()
                if self.done:
                    log.debug("Killing info server...")
                    self.server.socket.close()
                    break
            except socket.timeout:
                log.warning("info server's socket timed out. Don't panic!")

    def stop(self):
        self.done = True

class VMJSONEncoder(json.JSONEncoder):
    def default(self, vm):
        if not isinstance (vm, VM):
            log.error("Cannot use VMJSONEncoder on non VM object of type %s, %s" % (type(vm), vm))
            return
        return {'name': vm.name, 'id': vm.id, 'vmtype': vm.vmtype,
                'hostname': vm.hostname, 'clusteraddr': vm.clusteraddr,
                'ipaddress': vm.ipaddress,
                'cloudtype': vm.cloudtype, 'network': vm.network, 
                'image': vm.image, 'alt_hostname': vm.alt_hostname,
                'memory': vm.memory, 'mementry': vm.mementry, 
                'cpucores': vm.cpucores, 'storage': vm.storage, 
                'status': vm.status, 'condoraddr': vm.condoraddr,
                'condorname': vm.condorname, 'condormasteraddr': vm.condormasteraddr,
                'keep_alive': vm.keep_alive, 'user': vm.user, 'uservmtype': vm.uservmtype,
                'clusteraddr': vm.clusteraddr, 'clusterport': vm.clusterport,
                'errorcount': vm.errorcount, 'errorconnect': vm.errorconnect,
                'lastpoll': vm.lastpoll, 'last_state_change': vm.last_state_change,
                'initialize_time': vm.initialize_time, 'startup_time': vm.startup_time,
                'idle_start': vm.idle_start, 'spot_id': vm.spot_id,
                'proxy_file': vm.proxy_file, 'myproxy_creds_name': vm.myproxy_creds_name,
                'myproxy_server': vm.myproxy_server, 'myproxy_server_port': vm.myproxy_server_port,
                'myproxy_renew_time': vm.myproxy_renew_time, 'override_status': vm.override_status,
                'job_per_core': vm.job_per_core, 'force_retire': vm.force_retire,
                'failed_retire': vm.failed_retire, 'x509userproxy_expiry_time': str(vm.x509userproxy_expiry_time),
                'job_run_times': vm.job_run_times.data}

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
                'network_pools': cluster.network_pools, 
                'vm_slots': cluster.vm_slots, 'cpu_cores': cluster.cpu_cores, 
                'storageGB': cluster.storageGB, 'vms': vmDecodes, 'enabled':cluster.enabled,
                'hypervisor': cluster.hypervisor, 'max_mem': cluster.max_mem,
                'max_vm_mem': cluster.max_vm_mem, 'max_slots': cluster.max_slots,
                'max_storageGB': cluster.max_storageGB, 'boot_timeout': cluster.boot_timeout,
                'connection_fail_disable_time': cluster.connection_fail_disable_time,
                'connection_problem': cluster.connection_problem,
                'errorconnect': cluster.errorconnect}

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

class JobJSONEncoder(json.JSONEncoder):
    def default(self, job):
        if not isinstance(job, Job):
            log.error("Cannot use JobJSONEncoder on non Job Object")
            return
        return {'id': job.id, 'user': job.user, 'priority': job.priority,
                'job_status': job.job_status, 'cluster_id': job.cluster_id,
                'proc_id': job.proc_id, 'req_vmtype': job.req_vmtype,
                'req_network': job.req_network,
                'req_image': job.req_image, 'req_imageloc': job.req_imageloc,
                'req_ami': job.req_ami, 'req_memory': job.req_memory,
                'req_cpucores': job.req_cpucores, 'req_storage': job.req_storage,
                'keep_alive': job.keep_alive, 'status': job.status,
                'remote_host': job.remote_host, 'running_cloud': job.running_cloud,
                'banned': job.banned, 'ban_time': job.ban_time, 'target_clouds':job.target_clouds,
                'blocked_clouds':job.blocked_clouds, 'uservmtype': job.uservmtype,
                'high_priority': job.high_priority, 'instance_type': job.instance_type,
                'maximum_price': job.maximum_price, 'spool_dir': job.spool_dir,
                'myproxy_server': job.myproxy_server, 'myproxy_server_port': job.myproxy_server_port,
                'myproxy_creds_name': job.myproxy_creds_name, 'running_vm': job.running_vm,
                'x509userproxysubject': job.x509userproxysubject, 'x509userproxy': job.x509userproxy,
                'original_x509userproxy': job.original_x509userproxy,
                'x509userproxy_expiry_time': job.x509userproxy_expiry_time,
                'proxy_renew_time': job.proxy_renew_time, 'job_per_core': job.job_per_core,
                'servertime': job.servertime, 'jobstarttime': job.jobstarttime,
                'machine_reserved': job.machine_reserved, 'req_hypervisor': job.req_hypervisor,
                'proxy_non_boot': job.proxy_non_boot, 'vmimage_proxy_file': job.vmimage_proxy_file,
                'usertype_limit': job.usertype_limit, 'req_image_id': job.req_image_id,
                'req_instance_type_ibm': job.req_instance_type_ibm, 'location': job.location,
                'key_name': job.key_name, 'req_security_group': job.req_security_group,
                'override_status': job.override_status, 'block_time': job.block_time
                }

class JobPoolJSONEncoder(json.JSONEncoder):
    def default(self, job_pool):
        if not isinstance(job_pool, JobPool):
            log.error("Cannot use JobPoolJSONEncoder on non JobPool Object")
            return
        new_queue = []
        for job in job_pool.job_container.get_unscheduled_jobs():
            new_queue.append(JobJSONEncoder().encode(job))
        sched_queue = []
        for job in job_pool.job_container.get_scheduled_jobs():
            sched_queue.append(JobJSONEncoder().encode(job))
        new_decodes = []
        for job in new_queue:
            print job
            new_decodes.append(job)
        sched_decodes = []
        for job in sched_queue:
            sched_decodes.append(job)
        return {'new_jobs': new_decodes, 'sched_jobs': sched_decodes}
