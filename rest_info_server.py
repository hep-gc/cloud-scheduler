#!/usr/bin/env python

import web

class views:
    class cloud:
        def GET(self, json=None):
            return json
            if json:
                return "get_json_resource"
            else:
                return "get_cloud_resources"

    class cloud_config:
        def GET(self):
            return "get_cloud_config_values"

    class clusters:
        def GET(self, cluster=None, json=None):
            if cluster:
                if json:
                    return "get_json_cluster(" + cluster + ")"
                else:
                    return "get_cluster_info(" + cluster + ")"
            else:
                if json:
                    return "get_json_resource"
                else:
                    return "get_cluster_resources"

    class developer_info:
        def GET(self):
            return "get_developer_information"

    class diff_types:
        def GET(self):
            return "get_diff_types"

    class failures:
        def GET(self, failure_type):
            if failure_type == 'boot':
                return "get_job_failure_reasons"
            elif failure_type == 'image':
                return "get_image_failures"

    class ips:
        def GET(self):
            return "get_ips_munin"

    class jobs:
        def GET(self):
            return 'get_' + web.input().state + 'jobs'

    class thread_heart_beats:
        def GET(self):
            return "get_thread_heart_beats"

    class version:
        def GET(self):
            return "get_version"

    class vms:
        def GET(self, cluster=None, vm=None, json=None):
            if cluster and vm:
                if json:
                    return "get_json_vm(" + vm + ")"
                else:
                    return "get_vm_info(" + vm + ")"
            elif cluster and not vm:
                return "get_vm_stats(" + cluster + ")"
            elif 'metric' in web.input():
                return "get_vm_" + web.input().metric
            else:
                return "get_vm_stats"

urls = (
    r'/',                                   views.version,
    r'/cloud',                              views.cloud,
    r'/cloud/config',                       views.cloud_config,
    r'/clusters',                           views.clusters,
    r'/clusters()(\.json)',                 views.clusters,
    r'/clusters/(\w+)',                     views.clusters,
    r'/clusters/(\w+)(\.json)',             views.clusters,
    r'/clusters/(\w+)/vms',                 views.vms,
    r'/clusters/(\w+)/vms/(\w+)',           views.vms,
    r'/clusters/(\w+)/vms/(\w+)(\.json)',   views.vms,
    r'/developer-info',                     views.developer_info,
    r'/diff-types',                         views.diff_types,
    r'/failures/(boot|image)',              views.failures,
    r'/ips',                                views.ips,
    r'/jobs',                               views.jobs,
    r'/thread-heart-beats',                 views.thread_heart_beats,
    r'/vms',                                views.vms,
)

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()