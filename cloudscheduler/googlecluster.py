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

    def vm_create(self, **args):
        pass

    def vm_destroy(self, vm, return_resources=True, reason=""):
        pass

    def vm_poll(self, vm):
        pass
