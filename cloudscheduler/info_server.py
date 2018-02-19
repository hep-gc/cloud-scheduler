#!/usr/bin/env python

""" REST Server for cloud_status."""
import logging
import threading
import time
import sys
import re
import urllib
# JSON lib included in 2.6+
try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        raise "Please install the simplejson lib for python 2.4 or 2.5"

import web
import web.wsgiserver

import cloudscheduler.config as config
import cloudscheduler.__version__ as version
from cloudscheduler.cluster_tools import ICluster
from cloudscheduler.cluster_tools import VM
from cloudscheduler.job_management import Job
from cloudscheduler.job_management import JobPool
from cloudscheduler.cloud_management import ResourcePool
from cloudscheduler.openstackcluster import OpenStackCluster

log = None
config_val = config.config_options


class InfoServer(threading.Thread,):

    cloud_resources = None
    job_pool = None
    job_poller = None
    machine_poller = None
    vm_poller = None
    scheduler = None
    cleaner = None
    def __init__(self, c_resources, c_job_pool, c_job_poller,
                 c_machine_poller, c_vm_poller, c_scheduler, c_cleaner):

        global log
        log = logging.getLogger("cloudscheduler")

        #set up class
        threading.Thread.__init__(self, name=self.__class__.__name__)
        self.done = False
        host_name = "0.0.0.0"

        # Set up Web.py server
        web.cloud_resources = c_resources
        web.job_pool = c_job_pool
        web.job_poller = c_job_poller
        web.machine_poller = c_machine_poller
        web.vm_poller = c_vm_poller
        web.cleaner = c_cleaner
        web.scheduler = c_scheduler
        web.config.debug = False
        self.app = None
        self.listen = (host_name, config_val.getint('global', 'info_server_port'))
        self.urls = (
            r'/', Views.Version,
            r'/cloud', Views.Cloud,
            r'/cloud/config', Views.Cloudconfig,
            r'/clusters', Views.Clusters,
            r'/clusters()(\.json)', Views.Clusters,
            r'/clusters/([\w\%-]+)', Views.Clusters,
            r'/clusters/([\w\%-]+)(\.json)', Views.Clusters,
            r'/clusters/([\w\%-]+)/vms', Views.Vms,
            r'/clusters/([\w\%-]+)/vms/([\w\%-]+)', Views.Vms,
            r'/clusters/([\w\%-]+)/vms/([\w\%-]+)(\.json)', Views.Vms,
            r'/developer-info', Views.Developerinfo,
            r'/diff-types', Views.Difftypes,
            r'/failures/(boot|image)', Views.Failures,
            r'/ips', Views.Ips,
            r'/jobs', Views.Jobs,
            r'/jobs/([\w\%-]+)', Views.Jobs,
            r'/jobs/([\w\%-]+)(\.json)', Views.Jobs,
            r'/job-pool.json', Views.Jobpool,
            r'/shared-objs', Views.Sharedobjs,
            r'/thread-heart-beats', Views.Threadheartbeats,
            r'/vms', Views.Vms,
        )

    def run(self):

        # Run the server's main loop
        log.info("Started info server on port %s", config_val.getint('global', 'info_server_port'))
        try:
            self.app = web.application(self.urls, globals())
            self.server = web.wsgiserver.CherryPyWSGIServer(self.listen,
                                                            self.app.wsgifunc(),
                                                            server_name="localhost")
            self.server.start()
        except Exception as e:
            log.error("Could not start webpy server.\n{0}".format(e))
            sys.exit(1)

    def stop(self):
        self.server.stop()
        self.done = True





class Views(object):
    class Cloud(object):
        @staticmethod
        def GET():
            return web.cloud_resources.get_pool_info()

    class Cloudconfig(object):
        @staticmethod
        def GET():
            return web.cloud_resources.get_cloud_config_output()

    class Clusters(object):
        def GET(self, cluster_name=None, json_flag=None):

            if cluster_name:
                cluster_name = urllib.unquote(cluster_name)

                if json_flag:
                    return self.view_cluster_json(cluster_name)
                else:
                    return self.view_cluster(cluster_name)
            else:
                if json_flag:
                    return ResourcePoolJSONEncoder().encode(web.cloud_resources)
                else:
                    return self.view_resources()

        @staticmethod
        def view_cluster(cluster_name):
            output = []
            output.append("Cluster Info: %s\n" % cluster_name)
            cluster = web.cloud_resources.get_cluster(cluster_name)
            if cluster:
                output.append(cluster.get_cluster_info_short())
            else:
                output.append("Cluster named %s not found." % cluster_name)
            return ''.join(output)

        @staticmethod
        def view_cluster_json(cluster_name):
            output = "{}"
            cluster = web.cloud_resources.get_cluster(cluster_name)
            if cluster:
                output = ClusterJSONEncoder().encode(cluster)
            return output

        @staticmethod
        def view_resources():
            output = []
            output.append("Clusters in resource pool:\n")
            for cluster in web.cloud_resources.resources:
                output.append(cluster.get_cluster_info_short())
                output.append("\n")
            return ''.join(output)


    class Developerinfo(object):
        @staticmethod
        def GET():
            try:
                from guppy import hpy
                heapy = hpy()
                heap = heapy.heap()
                return str(heap)
            except:
                return "You need to have Guppy installed to get developer " \
                       "information"

    class Difftypes(object):
        @staticmethod
        def GET():
            output = []
            current_types = web.cloud_resources.vmtype_distribution()
            desired_types = web.job_pool.job_type_distribution()
            # Negative difference means will need to create that type
            diff_types = {}
            for vmtype in current_types.keys():
                if vmtype in desired_types.keys():
                    diff_types[vmtype] = current_types[vmtype] - desired_types[vmtype]
                else:
                    # changed from 0 to handle users with multiple job types, back to 0 from 1
                    diff_types[vmtype] = 1
            for vmtype in desired_types.keys():
                if vmtype not in current_types.keys():
                    diff_types[vmtype] = -desired_types[vmtype]

            # With user limiting will need to reset any users that are at their limits
            # so they will not interfere with scheduling
            # will need to redistribute negatives to the non-limited users
            limited_users = []
            userjoblimits = web.job_pool.get_usertype_limits()
            for vmusertype in diff_types.keys():
                user = vmusertype.split(':')[0]
                if web.cloud_resources.user_at_limit(user):
                    if vmusertype not in limited_users:
                        limited_users.append(vmusertype)
                if vmusertype in userjoblimits.keys():
                    if web.cloud_resources.uservmtype_at_limit(vmusertype,
                                                               userjoblimits[vmusertype]):
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
                log.error("More user vmtypes limited than what's in diff types, \
                           something weird here.")

            for usertype in diff_types.keys():
                if usertype not in limited_users:
                    diff_types[usertype] += adjustby # the 'extra' will be negative so add it

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

    class Failures(object):
        def GET(self, failure_type):
            if failure_type == 'boot':
                return self.view_boot_failures()
            elif failure_type == 'image':
                return self.view_image_failures()

            raise web.notfound()

        @staticmethod
        def view_boot_failures():
            output = []
            output.append("Job Failure Reasons:\n")
            reasons = web.job_pool.fetch_job_failure_reasons()
            for job in reasons:
                output.append("   Job ID: %s\n" % job.id)
                for reason in job.failed_boot_reason:
                    output.append("      %s\n" % reason)
            return ''.join(output)

        @staticmethod
        def view_image_failures():
            output = []
            output.append("Image Failure List\n")
            for cloud in web.cloud_resources.resources:
                if len(cloud.failed_image_set) > 0:
                    output.append("   Cloud: %s\n" % cloud.name)
                    for image in cloud.failed_image_set:
                        output.append("      Image: %s\n" % image)
            return ''.join(output)

    class Ips(object):
        @staticmethod
        def GET():
            output = []
            for cluster in web.cloud_resources.resources:
                for vm in cluster.vms:
                    if re.search(r"(10|192\.168|172\.(1[6-9]|2[0-9]|3[01]))\.", vm.ipaddress):
                        continue
                    else:
                        output.append("[%s]\n\taddress %s\n" % (vm.hostname, vm.ipaddress))
            return ''.join(output)

    class Jobs(object):
        def GET(self, jobid=None, json_flag=None):
            if jobid:
                jobid = urllib.unquote(jobid)

                if json_flag:
                    return self.view_job_json(jobid)
                else:
                    return self.view_job(jobid)

            elif 'state' in web.input():
                return self.view_jobs(web.input().state)

            raise web.notfound()

        @staticmethod
        def view_jobs(state):
            output = []

            state = web.input().state
            if state == 'complete':
                jobs = web.job_pool.job_container.get_complete_jobs()
            elif state == 'held':
                jobs = web.job_pool.job_container.get_held_jobs()
            elif state == 'high':
                jobs = web.job_pool.job_container.get_high_priority_jobs()
            elif state == 'idle':
                jobs = web.job_pool.job_container.get_idle_jobs()
            elif state == 'new':
                jobs = web.job_pool.job_container.get_unscheduled_jobs()
            elif state == 'running':
                jobs = web.job_pool.job_container.get_running_jobs()
            elif state == 'sched':
                jobs = web.job_pool.job_container.get_scheduled_jobs()
            else:
                return ''

            output.append(Job.get_job_info_header())
            for job in jobs:
                output.append(job.get_job_info())
            return ''.join(output)

        @staticmethod
        def view_job(jobid):
            output = "Job not found."
            job = web.job_pool.job_container.get_job_by_id(jobid)
            if job != None:
                output = job.get_job_info_pretty()
            return output

        @staticmethod
        def view_job_json(jobid):
            job_match = web.job_pool.job_container.get_job_by_id(jobid)
            return JobJSONEncoder().encode(job_match)

    class Jobpool(object):

        """
        Get json info about the job pool.
        """
        @staticmethod
        def GET():
            """Get json output of job pool."""
            return JobPoolJSONEncoder().encode(web.job_pool)

    class Sharedobjs(object):

        """
        Check info about shared objects in cloudscheduler.
        """
        @staticmethod
        def GET():
            """Check the info about shared objects."""
            output = []
            output.append("Scheduler Thread:\n" + web.scheduler.check_shared_objs())
            output.append("\n")
            output.append("Cleanup Thread:\n" + web.cleaner.check_shared_objs())
            output.append("\n")
            output.append("VMPoller Thread:\n" + web.vm_poller.check_shared_objs())
            output.append("\n")
            output.append("JobPoller Thread:\n" + web.job_poller.check_shared_objs())
            output.append("\n")
            output.append("MachinePoller Thread:\n" + web.machine_poller.check_shared_objs())
            output.append("\n")
            return ''.join(output)

    class Threadheartbeats(object):

        """
        Get heart beat info on threads to see if they're stalled.
        """
        @staticmethod
        def GET():
            """Get heartbeat info on threads."""
            now = time.time()
            output = []
            output.append("Thread Heart beat times:\n")
            output.append("   Scheduler Thread(%s): %s\n" %
                          (web.scheduler.scheduling_interval,
                           str(int(now - web.scheduler.heart_beat))))
            output.append("   Cleanup Thread(%s): %s\n" %
                          (web.cleaner.polling_interval,
                           str(int(now - web.cleaner.heart_beat))))
            output.append("   VMPoller Thread(%s): %s\n" %
                          (web.vm_poller.run_interval,
                           str(int(now - web.vm_poller.heart_beat))))
            output.append("   JobPoller Thread(%s): %s\n" %
                          (web.job_poller.polling_interval,
                           str(int(now - web.job_poller.heart_beat))))
            output.append("   MachinePoller Thread(%s): %s\n" %
                          (web.machine_poller.polling_interval,
                           str(int(now - web.machine_poller.heart_beat))))
            return ''.join(output)

    class Version(object):

        """
        Output the Version info.
        """
        @staticmethod
        def GET():
            """Output the Version info."""
            return "Cloud Scheduler version: %s" % version.version

    class Vms(object):

        """
        Deals with various calls concerning VMs.

        """
        def GET(self, cluster_name=None, vm_id=None, json_flag=None):

            """
            GET function for RESTful api.
            :param cluster_name:
            :param vm_id:
            :param json_flag:
            :return:
            """
            if cluster_name:
                cluster_name = urllib.unquote(cluster_name)
            if vm_id:
                vm_id = urllib.unquote(vm_id)

            if cluster_name and vm_id:
                if json_flag:
                    return self.view_vm_json(cluster_name, vm_id)
                else:
                    return self.view_vm(cluster_name, vm_id)

            elif 'metric' in web.input():
                return self.view_vm_metric(cluster_name, web.input().metric)

            else:
                return self.view_vm_resources()

        @staticmethod
        def view_vm(cluster_name, vm_id):
            """Displaying info about a vm."""
            output = []
            output.append("VM Info for VM id: %s\n" % vm_id)
            cluster = web.cloud_resources.get_cluster(cluster_name)
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

        @staticmethod
        def view_vm_json(cluster_name, vm_id):
            """get JSON info for a specific vm on a cloud."""
            output = "{}"
            cluster = web.cloud_resources.get_cluster(cluster_name)
            vm = None
            if cluster:
                vm = cluster.get_vm(vm_id)
                if vm:
                    output = VMJSONEncoder().encode(vm)
            return output

        @staticmethod
        def view_vm_metric(cluster_name, metric):
            """Get various metrics related to a cloud."""
            if metric == 'all':
                vms = []
                if cluster_name:
                    cluster = web.cloud_resources.get_cluster(cluster_name)
                    if not cluster:
                        return "Could not find cloud: %s - check cloud_status for \
                                list of available clouds" % cluster_name
                    vms.extend(cluster.vms)
                else:
                    for cluster in web.cloud_resources.resources:
                        vms.extend(cluster.vms)
                state_count = {'Running':0, 'Starting':0, 'Error':0, 'Retiring':0,
                               'ExpiredProxy':0, 'NoProxy':0, 'ConnectionRefused':0}
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

            elif metric == 'job_run_times':
                output = []
                output.append("Run Times of Jobs on VMs\n")
                for cluster in web.cloud_resources.resources:
                    for vm in cluster.vms:
                        output.append("%s : avg %f\n" % (vm.hostname, vm.job_run_times.average()))
                return ''.join(output)

            elif metric == 'missing':
                return web.cloud_resources.fetch_missing_vm_list()

            elif metric == 'startup_time':
                output = []
                for cluster in web.cloud_resources.resources:
                    output.append("Cluster: %s " % cluster.name)
                    total_time = 0
                    for vm in cluster.vms:
                        output.append("%d, " % (vm.startup_time if vm.startup_time != None else 0))
                        total_time += (vm.startup_time if vm.startup_time != None else 0)
                    if len(cluster.vms) > 0:
                        output.append(" Avg: %d " % (int(total_time) / len(cluster.vms)))
                return ''.join(output)

            elif metric == 'total':
                if cluster_name:
                    metric = web.input().metric
                    if metric == 'total':
                        output = []
                        cluster = web.cloud_resources.get_cluster(cluster_name)
                        if cluster:
                            output.append(str(cluster.num_vms()))
                        else:
                            output.append("Cluster named %s not found." % cluster_name)
                        return ''.join(output)
                    else:
                        return ''
                else:
                    return str(web.cloud_resources.vm_count())

            raise web.notfound()

        @staticmethod
        def view_vm_resources():
            """Get a listing of the VMs in the system, and the Retiring resources."""
            output = []
            output.append(VM.get_vm_info_header())
            clusters = 0
            vm_count = 0
            for cluster in web.cloud_resources.resources:
                clusters += 1
                vm_count += len(cluster.vms)
                output.append(cluster.get_cluster_vms_info())
            output.append('\nTotal VMs: %i. Total Clouds: %i' % (vm_count, clusters))
            extra_output = []
            for cluster in web.cloud_resources.retired_resources:
                extra_output.append(cluster.get_cluster_vms_info())
            if len(extra_output) > 0:
                output.append('\n\nRetiring VMs from removing resources:\n')
                output.extend(extra_output)
            return ''.join(output)


class VMJSONEncoder(json.JSONEncoder):

    """
    JSON Encoder for the VM class.

    """
    def default(self, vm):
        if not isinstance(vm, VM):
            log.error("Cannot use VMJSONEncoder on non VM object of type %s, %s" % (type(vm), vm))
            return
        return {'name': vm.name, 'id': vm.id, 'vmtype': vm.vmtype,
                'hostname': vm.hostname, 'clusteraddr': vm.clusteraddr,
                'ipaddress': vm.ipaddress, 'ssh_port': vm.ssh_port,
                'cloudtype': vm.cloudtype, 'network': vm.network,
                'image': vm.image, 'alt_hostname': vm.alt_hostname,
                'memory': vm.memory, 'flavor': vm.flavor,
                'cpucores': vm.cpucores, 'storage': vm.storage,
                'status': vm.status, 'condoraddr': vm.condoraddr,
                'condorname': vm.condorname, 'condormasteraddr': vm.condormasteraddr,
                'keep_alive': vm.keep_alive, 'user': vm.user, 'uservmtype': vm.uservmtype,
                'clusterport': vm.clusterport,
                'errorcount': vm.errorcount, 'errorconnect': vm.errorconnect,
                'lastpoll': vm.lastpoll, 'last_state_change': vm.last_state_change,
                'initialize_time': vm.initialize_time, 'startup_time': vm.startup_time,
                'idle_start': vm.idle_start, 'spot_id': vm.spot_id,
                'proxy_file': vm.proxy_file, 'myproxy_creds_name': vm.myproxy_creds_name,
                'myproxy_server': vm.myproxy_server, 'myproxy_server_port': vm.myproxy_server_port,
                'myproxy_renew_time': vm.myproxy_renew_time, 'override_status': vm.override_status,
                'job_per_core': vm.job_per_core, 'force_retire': vm.force_retire,
                'failed_retire': vm.failed_retire,
                'x509userproxy_expiry_time': str(vm.x509userproxy_expiry_time),
                'job_run_times': list(vm.job_run_times.data)}


class ClusterJSONEncoder(json.JSONEncoder):

    """
    JSON Encoder for the Cluster class.

    """
    def default(self, cluster):
        if not isinstance(cluster, ICluster):
            log.error("Cannot use ClusterJSONEncoder on non Cluster object")
            return
        vm_encodes = []
        for vm in cluster.vms:
            vm_encodes.append(VMJSONEncoder().encode(vm))
        vm_decodes = []
        for vm in vm_encodes:
            vm_decodes.append(json.loads(vm))
        cluster_dict = {'name': cluster.name, 'network_address': cluster.network_address,
                        'cloud_type': cluster.cloud_type, 'memory': cluster.memory,
                        'network_pools': cluster.network_pools,
                        'vm_slots': cluster.vm_slots, 'cpu_cores': cluster.cpu_cores,
                        'storageGB': cluster.storageGB, 'vms': vm_decodes,
                        'enabled':cluster.enabled, 'max_mem': cluster.max_mem,
                        'max_vm_mem': cluster.max_vm_mem, 'max_slots': cluster.max_slots,
                        'max_storageGB': cluster.max_storageGB,
                        'boot_timeout': cluster.boot_timeout,
                        'connection_fail_disable_time': cluster.connection_fail_disable_time,
                        'connection_problem': cluster.connection_problem,
                        'errorconnect': cluster.errorconnect}
        if isinstance(cluster, OpenStackCluster):
            cluster_dict['tenant'] = cluster.tenant_name
        return cluster_dict


class ResourcePoolJSONEncoder(json.JSONEncoder):

    """
    JSON Encoder for the ResourcePool class.
    """
    def default(self, res_pool):
        if not isinstance(res_pool, ResourcePool):
            log.error("Cannot use ResourcePoolJSONEncoder on non ResourcePool Object")
            return
        pool = []
        for cluster in res_pool.resources:
            pool.append(ClusterJSONEncoder().encode(cluster))
        pool_decodes = []
        for cluster in pool:
            pool_decodes.append(json.loads(cluster))
        return {'resources': pool_decodes}


class JobJSONEncoder(json.JSONEncoder):

    """
    JSON Encoder for Job class.

    """
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
                'myproxy_server': job.myproxy_server,
                'myproxy_server_port': job.myproxy_server_port,
                'myproxy_creds_name': job.myproxy_creds_name,
                'running_vm': job.running_vm,
                'x509userproxysubject': job.x509userproxysubject,
                'x509userproxy': job.x509userproxy,
                'original_x509userproxy': job.original_x509userproxy,
                'x509userproxy_expiry_time': job.x509userproxy_expiry_time,
                'proxy_renew_time': job.proxy_renew_time, 'job_per_core': job.job_per_core,
                'servertime': job.servertime, 'jobstarttime': job.jobstarttime,
                'machine_reserved': job.machine_reserved,
                'proxy_non_boot': job.proxy_non_boot, 'vmimage_proxy_file': job.vmimage_proxy_file,
                'usertype_limit': job.usertype_limit, 'req_image_id': job.req_image_id,
                'location': job.location,
                'key_name': job.key_name, 'req_security_group': job.req_security_group,
                'override_status': job.override_status, 'block_time': job.block_time
               }


class JobPoolJSONEncoder(json.JSONEncoder):

    """
    JSON Encoder for the Job Pool Class.

    """
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
