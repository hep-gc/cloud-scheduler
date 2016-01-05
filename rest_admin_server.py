#!/usr/bin/env python

import web

class views:
    class config:
        def PUT(self):
            if 'log_level' in web.input():
                return "change_log_level(" + web.input().log_level + ")"
        
        def POST(self):
            if 'action' in web.input() and web.input().action == 'quick_shutdown':
                return 'perform_quick_shutdown'

    class clouds:
        def PUT(self, cloud):
            if 'action' in web.input():
                return web.input().action + "_cloud(" + cloud + ")"
            elif 'allocations' in web.input():
                return "adjust_cloud_allocation(" + cloud + ", " + web.input().allocations + ")"

    class cloud_aliases:
        def GET(self):
            return 'list_cloud_alias'

        def POST(self):
            return 'cloud_alias_reload'

    class users:
        def POST(self, user):
            if 'refresh' in web.input():
                if web.input().refresh == 'job_proxy':
                    return "refresh_job_proxy_user"
                elif web.input().refresh == 'vm_proxy':
                    return "refresh_vm_proxy_user"

    class user_limits:
        def GET(self):
            return 'list_user_limits'

        def POST(self):
            return 'user_limit_reload'

    class vms:
        def PUT(self, cloud, vm=None):
            if 'action' in web.input():
                if web.input().action == 'shutdown':
                    if vm:
                        return "shutdown_vm(" + cloud + ", " + vm + ")"
                    elif 'count' in web.input():
                        if web.input().count == 'all':
                            return "shutdown_cluster_all(" + cloud + ")"
                        else:
                            return "shutdown_cluster_count(" + cloud + ", " + web.input().count + ")"
                elif web.input().action == 'force_retire':
                    if vm:
                        return "force_retire_vm(" + cloud + ", " + vm + ")"
                    elif 'count' in web.input():
                        if web.input().count == 'all':
                            return "force_retire_all_vm(" + cloud + ")"
                        else:
                            return "force_retire_count_vm(" + cloud + ", " + web.input().count + ")"
                elif web.input().action == 'reset_override_state' and vm:
                    return "reset_override_state(" + cloud + ", " + vm + ")"
        def POST(self, cloud, vm=None):
            pass

        def DELETE(self, cloud, vm=None):
            if vm:
                return "delete_vm_entry(" + cloud + ", " + vm + ")"
            else:
                return "delete_all_vm_entry_cloud(" + cloud + ")"


urls = (
    r'/',                                   views.config,
    r'/clouds/(\w+)',                       views.clouds,
    r'/clouds/(\w+)/vms',                   views.vms,
    r'/clouds/(\w+)/vms/(\w+)',             views.vms,
    r'/cloud-aliases',                      views.cloud_aliases,
    r'/users/(\w+)',                        views.users,
    r'/user-limits',                        views.user_limits,
)

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()