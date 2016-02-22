#!/usr/bin/env python

""" REST server for cloud_admin.
"""

import logging
import threading
import socket
import sys
import web
import web.wsgiserver
import urllib
import cloudscheduler.utilities as utilities
import cloudscheduler.config as config
from proxy_refreshers import MyProxyProxyRefresher

log = None


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

        # set up class
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
        self.listen = (host_name, config.admin_server_port)
        self.urls = (
            r'/',                                 views.config,
            r'/clouds/([\w\%-]+)',                views.clouds,
            r'/clouds/([\w\%-]+)/vms',            views.vms,
            r'/clouds/([\w\%-]+)/vms/([\w\%-]+)', views.vms,
            r'/cloud-aliases',                    views.cloud_aliases,
            r'/users/([\w\%-]+)',                 views.users,
            r'/user-limits',                      views.user_limits,
        )

    def run(self):

        # Run the server's main loop
        log.info("Started admin server on port %s" % config.admin_server_port)
        try:
            self.app = web.application(self.urls, globals())
            self.server = web.wsgiserver.CherryPyWSGIServer(self.listen, self.app.wsgifunc(), server_name="localhost")
            self.server.start()

        except Exception as e:
            log.error("Could not start webpy server.\n{0}".format(e))
            sys.exit(1)

    def stop(self):
        self.server.stop()
        self.done = True


class views:
    class config:
        def PUT(self):
            if 'log_level' in web.input():
                log.setLevel(utilities.LEVELS[web.input().log_level])
                return ''

            raise web.notfound()
        
        def POST(self):
            if 'action' in web.input():
                action = web.input().action
                if action == 'quick_shutdown':
                    web.scheduler.toggle_quick_exit()
                    return ''
                elif action == 'reconfig':
                    web.cloud_resources.setup()
                    return ''

            raise web.notfound()

    class clouds:
        def PUT(self, cloudname):
            cloudname = urllib.unquote(cloudname)

            if 'action' in web.input():
                action = web.input().action
                if action == 'enable':
                    return web.cloud_resources.enable_cluster(cloudname)
                elif action == 'disable':
                    return web.cloud_resources.disable_cluster(cloudname)
                else:
                    raise web.notfound()

                return web.input().action + "_cloud(" + cloudname + ")"
            elif 'allocations' in web.input():
                return web.cloud_resources.adjust_cloud_allocation(cloudname, web.input().allocations)

            raise web.notfound()

    class cloud_aliases:
        def GET(self):
            return str(web.cloud_resources.target_cloud_aliases)

        def POST(self):
            if config.target_cloud_alias_file:
                web.cloud_resources.target_cloud_aliases = web.cloud_resources.load_cloud_aliases(config.target_cloud_alias_file)
                return True if len(web.cloud_resources.target_cloud_aliases) > 0 else False
            else:
                return False

    class users:
        def POST(self, user):
            user = urllib.unquote(user)

            if 'refresh' in web.input():
                if web.input().refresh == 'job_proxy':
                    return MyProxyProxyRefresher.renew_job_proxy_user(web.job_pool, user)
                elif web.input().refresh == 'vm_proxy':
                    return MyProxyProxyRefresher.renew_vm_proxy_user(web.job_pool, user)
            
            raise web.notfound()

    class user_limits:
        def GET(self):
            return str(web.cloud_resources.user_vm_limits)

        def POST(self):
            web.cloud_resources.user_vm_limits = web.cloud_resources.load_user_limits(config.user_limit_file)
            return True if len(web.cloud_resources.user_vm_limits) > 0 else False

    class vms:
        def PUT(self, cloudname, vmid=None):
            cloudname = urllib.unquote(cloudname)
            if vmid: vmid = urllib.unquote(vmid)

            if 'action' in web.input():
                action = web.input().action

                if action == 'force_retire':
                    if vmid:
                        return web.cloud_resources.force_retire_cluster_vm(cloudname, vmid)
                    elif 'count' in web.input():
                        if web.input().count == 'all':
                            return web.cloud_resources.force_retire_cluster_all(cloudname)
                        else:
                            return web.cloud_resources.force_retire_cluster_number(cloudname, web.input().count)

                elif action == 'shutdown':
                    if vmid:
                        return web.cloud_resources.shutdown_cluster_vm(cloudname, vmid)
                    elif 'count' in web.input():
                        if web.input().count == 'all':
                            return web.cloud_resources.shutdown_cluster_all(cloudname)
                        else:
                            return web.cloud_resources.shutdown_cluster_number(cloudname, web.input().count)
                
                elif action == 'reset_override_state' and vmid:
                    return web.cloud_resources.reset_override_state(cloudname, vmid)
            
                elif action == 'remove':
                    if vmid:
                        return web.cloud_resources.remove_vm_no_shutdown(cloudname, vmid)
                    elif 'count' in web.input() and web.input().count == 'all':
                        return web.cloud_resources.remove_all_vmcloud_no_shutdown(cloudname)

            raise web.notfound()