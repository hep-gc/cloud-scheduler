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
    
    API_VERSION = 'v1beta13'
    GCE_URL = 'https://www.googleapis.com/compute/%s/projects/' % (API_VERSION)
    PROJECT_ID = 'mhpcloud' # this will need to be configurable
    
    DEFAULT_ZONE = 'us-central1-a'
    DEFAULT_MACHINE_TYPE = 'n1-standard-1'
    DEFAULT_IMAGE = 'condor'  # do away with this part and just use the image name provided by user 'condorbaseimage2'
    DEFAULT_IMAGES = {
        'condor': 'condorbaseimage2',
    }
    DEFAULT_NETWORK = 'default'
    DEFAULT_SERVICE_EMAIL = 'default'
    DEFAULT_SCOPES = ['https://www.googleapis.com/auth/devstorage.full_control',
                  'https://www.googleapis.com/auth/compute']

    NEW_INSTANCE_NAME = 'my-new-instance'  # this will need to be some kind of counter

    def __init__(self, name="Dummy Cluster", host="localhost",
                 cloud_type="Dummy", memory=[], max_vm_mem= -1, cpu_archs=[], networks=[],
                 vm_slots=0, cpu_cores=0, storage=0, hypervisor='xen', boot_timeout=None,
                 auth_dat_file=None, secret_file=None, security_group=None):

        self.security_group = security_group
        self.auth_dat_file_path = auth_dat_file
        self.secret_file_path = secret_file
        
        # Perform OAuth 2.0 authorization.
        flow = flow_from_clientsecrets(self.secret_file_path, scope=self.GCE_SCOPE)
        #print "flow obj created"
        auth_storage = Storage(self.auth_dat_file_path)
        #print "storage obj created"
        credentials = auth_storage.get()
        #print "got credentials"
      
        if credentials is None or credentials.invalid:
            #print "invalid creds"
            credentials = run(flow, auth_storage)
            #print "run creds done"
        http = httplib2.Http()
        #print "http created"
        self.auth_http = credentials.authorize(http)
        #print "auth http done"

        # Build service object
        self.gce_service = build('compute', self.API_VERSION)
        self.project_url = self.GCE_URL + self.PROJECT_ID
        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, cpu_archs=cpu_archs, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, hypervisor=hypervisor, boot_timeout=boot_timeout)

    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc, vm_cpuarch,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  vm_keepalive=0, instance_type="", maximum_price=0,
                  job_per_core=False, securitygroup=[]):
        # Construct URLs
        #   "image": "https://www.googleapis.com/compute/v1beta13/projects/mhpcloud/images/condorbaseimage2"
    #GCE_URL = 'https://www.googleapis.com/compute/v1beta12/projects/'
    #PROJECT_ID = 'mhpcloud' # this will need to be configurable
        image_url = '%s%s/images/%s' % (
               self.GCE_URL, self.PROJECT_ID, self.DEFAULT_IMAGES['condor'])
        machine_type_url = '%s/machineTypes/%s' % (
              self.project_url, self.DEFAULT_MACHINE_TYPE)
        zone_url = '%s/zones/%s' % (self.project_url, self.DEFAULT_ZONE)
        network_url = '%s/networks/%s' % (self.project_url, self.DEFAULT_NETWORK)
      
        # Construct the request body
        instance = {
          'name': self.NEW_INSTANCE_NAME,
          'machineType': machine_type_url,
          'image': image_url,
          'zone': zone_url,
          'networkInterfaces': [{
            'accessConfigs': [{
              'type': 'ONE_TO_ONE_NAT',
              'name': 'External NAT'
             }],
            'network': network_url
          }],
          'serviceAccounts': [{
               'email': self.DEFAULT_SERVICE_EMAIL,
               'scopes': self.DEFAULT_SCOPES
          }]
        }
  
        # Create the instance
        request = self.gce_service.instances().insert(
             project=self.PROJECT_ID, body=instance)
        response = request.execute(self.auth_http)
        response = self._blocking_call(self.gce_service, self.auth_http, response)
      
        #print "VM Creation Response"
        #print response
        vm_mementry = self.find_mementry(vm_mem)
        if (vm_mementry < 0):
            #TODO: this is kind of pointless with EC2...
            log.debug("Cluster memory list has no sufficient memory " +\
                      "entries (Not supposed to happen). Returning error.")
            return self.ERROR
        new_vm = cluster_tools.VM(name = vm_name, vmtype = vm_type, user = vm_user,
                    clusteraddr = self.network_address, id = response['targetId'],
                    cloudtype = self.cloud_type, network = vm_networkassoc,
                    cpuarch = vm_cpuarch, image= vm_image, mementry = vm_mementry,
                    memory = vm_mem, cpucores = vm_cores, storage = vm_storage, 
                    keep_alive = vm_keepalive, job_per_core = job_per_core)
    
        try:
            self.resource_checkout(new_vm)
        except:
            log.exception("Unexpected Error checking out resources when creating a VM. Programming error?")
            self.vm_destroy(new_vm, reason="Failed Resource checkout")
            return self.ERROR
    
        self.vms.append(new_vm)
        return 0


    def vm_destroy(self, vm, return_resources=True, reason=""):
        # Delete an Instance
        request = self.gce_service.instances().delete(
            project=self.PROJECT_ID, instance=self.NEW_INSTANCE_NAME)
        response = request.execute(self.auth_http)
        response = self._blocking_call(self.gce_service, self.auth_http, response)
        #print response

        # Delete references to this VM
        if return_resources:
            self.resource_return(vm)
        with self.vms_lock:
            self.vms.remove(vm)
        pass

    def vm_poll(self, vm):
        print "LIST of Instances:"
        # List instances
        filter_str = ''.join(["id eq ", vm.id])
        #print filter_str
        #request = self.gce_service.instances().list(project=self.PROJECT_ID, filter=filter_str)
        request = self.gce_service.instances().list(project=self.PROJECT_ID)
        #print 'made request'
        response = request.execute(self.auth_http)
        #print 'executed request'
        #print '---------------------------------------'
        #print '---------------------------------------'
        #print response
        #print response.keys()

        if response and 'items' in response:
            instances = response['items']
            #print instances
            for instance in instances:
                if 'id' in instance and instance['id'] == vm.id:
                    vm.status = instance['status']
                    #print type(instance['networkInterfaces'])
                    #try:
                        #print instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
                    #except:

                        ##print 'guess that was wrong'
                    vm.ipaddress = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
                    #print instance['name'], instance['id']
                    pass # update state etc
                
        pass


    def _blocking_call(self, gce_service, auth_http, response):
        """Blocks until the operation status is done for the given operation."""
        status = response['status']
        while status != 'DONE' and response:
            operation_id = response['name']
            request = gce_service.operations().get(
                project=self.PROJECT_ID, operation=operation_id)
            response = request.execute(auth_http)
            if response:
                status = response['status']
        return response
