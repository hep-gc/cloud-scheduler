import httplib2
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run
from apiclient.discovery import build
import cluster_tools
import time
import os
import ConfigParser
import threading
import cloudscheduler.utilities as utilities

log = utilities.get_cloudscheduler_logger()


class GoogleComputeEngineCluster(cluster_tools.ICluster):
    CLIENT_SECRETS = 'client_secrets.json' # this will need to be configurable - may not need this once the oauth2.dat is present
    OAUTH2_STORAGE = 'oauth2.dat' # this will need to be configurable
    GCE_SCOPE = 'https://www.googleapis.com/auth/compute'
    
    API_VERSION = 'v1beta12'
    GCE_URL = 'https://www.googleapis.com/compute/%s/projects/' % (API_VERSION)
    PROJECT_ID = 'mhpcloud' # this will need to be configurable
    def __init__(self, name="Dummy Cluster", host="localhost",
                 cloud_type="Dummy", memory=[], max_vm_mem= -1, cpu_archs=[], networks=[],
                 vm_slots=0, cpu_cores=0, storage=0, hypervisor='xen', boot_timeout=None):

        # Build service object  
        self.gce_service = build('compute', API_VERSION)
        self.project_url = GCE_URL + PROJECT_ID
        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, cpu_archs=cpu_archs, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, hypervisor=hypervisor, boot_timeout=boot_timeout)

    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc, vm_cpuarch,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  vm_keepalive=0, instance_type="", maximum_price=0,
                  job_per_core=False, securitygroup=[]):
        pass

    def vm_destroy(self, vm, return_resources=True, reason=""):
        # Delete an Instance
        request = gce_service.instances().delete(
            project=PROJECT_ID, instance=NEW_INSTANCE_NAME)
        response = request.execute(auth_http)
        response = _blocking_call(gce_service, auth_http, response)
        print response

        # Delete references to this VM
        if return_resources:
            self.resource_return(vm)
        with self.vms_lock:
            self.vms.remove(vm)
        pass

    def vm_poll(self, vm):
        print "LIST of Instances:"
        # List instances
        request = gce_service.instances().list(project=PROJECT_ID, filter="id eq %s" % vm.id)
        print 'made request'
        response = request.execute(auth_http)
        print 'executed request'
        if response and 'items' in response:
            instances = response['items']
            for instance in instances:
                if instance.id == vm.id:
                    pass # update state etc
                print instance['name'], instance['id']
        pass


    def _blocking_call(gce_service, auth_http, response):
        """Blocks until the operation status is done for the given operation."""
        status = response['status']
        while status != 'DONE' and response:
            operation_id = response['name']
            request = gce_service.operations().get(
                project=PROJECT_ID, operation=operation_id)
            response = request.execute(auth_http)
            if response:
                status = response['status']
        return response
