import os
import time
import uuid
import threading
import nimbus_xml
import ConfigParser
import cluster_tools
import cloud_init_util
import base64
import cloudscheduler.config as config
import cloudscheduler.utilities as utilities
from cloudscheduler.job_management import _attr_list_to_dict
try:
    import httplib2
    from oauth2client.client import flow_from_clientsecrets
    from oauth2client.file import Storage
    from oauth2client.tools import run
    from oauth2client.tools import run_flow
    from apiclient.discovery import build
except:
    pass

log = utilities.get_cloudscheduler_logger()


class GoogleComputeEngineCluster(cluster_tools.ICluster):
    VM_STATES = {
            "RUNNING" : "Running",
            "Starting" : "Starting",
            "shutting-down" : "Shutdown",
            "terminated" : "Shutdown",
            "PROVISIONING" : "Provisioning",
            "error" : "Error",
    }
    
    GCE_SCOPE = 'https://www.googleapis.com/auth/compute'
    
    API_VERSION = 'v1'
    GCE_URL = 'https://www.googleapis.com/compute/%s/projects/' % (API_VERSION)

    DEFAULT_ZONE = 'us-central1-b' # will need to be option in job
    DEFAULT_MACHINE_TYPE = 'n1-standard-1'  # option specified in job config
    DEFAULT_INSTANCE_TYPE_LIST = _attr_list_to_dict(config.default_VMInstanceTypeList)
    DEFAULT_IMAGE = 'cloudscheduler-centos-9'
    DEFAULT_ROOT_PD_NAME = 'hepgc-uvic-root-pd'  

    DEFAULT_NETWORK = 'default' # job option setup
    DEFAULT_SERVICE_EMAIL = 'default' 
    DEFAULT_SCOPES = ['https://www.googleapis.com/auth/devstorage.full_control',
                  'https://www.googleapis.com/auth/compute']

    def __init__(self, name="Dummy Cluster", host="localhost",
                 cloud_type="Dummy", memory=[], max_vm_mem= -1, networks=[],
                 vm_slots=0, cpu_cores=0, storage=0, hypervisor='xen', boot_timeout=None,
                 auth_dat_file=None, secret_file=None, security_group=None, project_id=None,enabled=True, priority = 0,
                 total_cpu_cores=-1):
        log.debug("Init GCE cores %s, storage %s"%(cpu_cores,storage))
        self.gce_hostname_prefix = 'gce-cs-vm'
        self.security_group = security_group
        self.auth_dat_file_path = auth_dat_file
        self.secret_file_path = secret_file
        self.project_id = project_id
        self.total_cpu_cores = total_cpu_cores
        if not project_id:
            return None
        
        
        # Perform OAuth 2.0 authorization.
        flow = flow_from_clientsecrets(self.secret_file_path, scope=self.GCE_SCOPE)
        auth_storage = Storage(self.auth_dat_file_path)
        credentials = auth_storage.get()
      
        if credentials is None or credentials.invalid:
            credentials = run_flow(flow, auth_storage)
        http = httplib2.Http()
        self.auth_http = credentials.authorize(http)

        #if not security_group:
        #    security_group = ["default"]
        #self.security_groups = security_group
        
        # Build service object
        self.gce_service = build('compute', self.API_VERSION)
        self.project_url = '%s%s' % (self.GCE_URL, self.project_id)
        # Call super class's init
        
        cluster_tools.ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, hypervisor=hypervisor, boot_timeout=boot_timeout,enabled=enabled, priority=priority)

    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  vm_keepalive=0, instance_type="", maximum_price=0,
                  job_per_core=False, securitygroup=[],pre_customization=None,use_cloud_init=False, extra_userdata=[]):
        
        try:
            if self.network_address in vm_image.keys():
                vm_ami = vm_image[self.network_address]
            elif self.name in vm_image.keys():
                vm_ami = vm_image[self.name]
        except:
            log.debug("No AMI for %s, trying default" % self.network_address)
            try:
                vm_ami = vm_image["default"]
            except:
                log.exception("Can't find a suitable AMI")
                return
        # Construct URLs
       
        try:
            if self.name in instance_type.keys():
                i_type = instance_type[self.name]
            else:
                i_type = instance_type[self.network_address]
        except:
            log.debug("No instance type for %s, trying default" % self.network_address)
            try:
                if self.name in self.DEFAULT_INSTANCE_TYPE_LIST.keys():
                    i_type = self.DEFAULT_INSTANCE_TYPE_LIST[self.name]
                else:
                    i_type = self.DEFAULT_INSTANCE_TYPE_LIST[self.network_address]
            except:
                log.debug("No default instance type found for %s, trying single default" % self.network_address)
                i_type = self.DEFAULT_MACHINE_TYPE
        vm_instance_type = i_type
        
        if vm_image:
            vm_image_name = vm_ami
        else:
            vm_image_name = self.DEFAULT_IMAGE

        #this should replace disk_url when cloud-init supports GCE in CERNVM3
        image_url = '%s%s/global/images/%s' % (
              self.GCE_URL, self.project_id, vm_image_name)
        
        #Ensures the VM's Root Disks are Unique
        self.DEFAULT_ROOT_PD_NAME = '%s-%s'%('hepgc-uvic-root-pd',self.generate_next_instance_name())
        
        #temporary variable for disk_url
        #https://www.googleapis.com/compute/v1/projects/atlasgce/zones/us-central1-b/disks/hepgc-uvic-root-pd
        disk_url = '%s%s/zones/%s/disks/%s'%(self.GCE_URL,self.project_id,self.DEFAULT_ZONE,self.DEFAULT_ROOT_PD_NAME)
        
        machine_type_url = '%s/zones/%s/machineTypes/%s' % (
              self.project_url, self.DEFAULT_ZONE, vm_instance_type)
        #zone_url = '%s/zones/%s' % (self.project_url, self.DEFAULT_ZONE)
        network_url = '%s/global/networks/%s' % (self.project_url, self.DEFAULT_NETWORK)
        
        # Construct the request body
        disk = {
               'name': self.DEFAULT_ROOT_PD_NAME,
               'sourceSnapshot':'https://www.googleapis.com/compute/v1/projects/atlasgce/global/snapshots/%s'%vm_image_name,
               'sizeGb':vm_storage
        }

        # Create the root pd
        try:
            request = self.gce_service.disks().insert(project=self.project_id, body=disk, zone=self.DEFAULT_ZONE)
            response = request.execute(http=self.auth_http)
            response = self._blocking_call(self.gce_service, self.auth_http, response)
        except:
            log.exception('Error Trying to create disk, one already exists ... returning ')
            return
        use_cloud_init = use_cloud_init or config.use_cloud_init
        if customization:
            if not use_cloud_init:
                user_data = nimbus_xml.ws_optional(customization)
            else:
                user_data = cloud_init_util.build_write_files_cloud_init(customization)
        else:
            user_data = ""
        
        
        if pre_customization:
            if not use_cloud_init:
                for item in pre_customization:
                    user_data = '\n'.join([item, user_data])
            else:
                user_data = cloud_init_util.inject_customizations(pre_customization, user_data)
        elif use_cloud_init:
            user_data = cloud_init_util.inject_customizations([], user_data)[0]
        if len(extra_userdata) > 0:
            # need to use the multi-mime type functions
            user_data = cloud_init_util.build_multi_mime_message([(user_data, 'cloud-config')], extra_userdata)     
        next_instance_name = self.generate_next_instance_name()
        
        instance = {
          'name': next_instance_name,
          'machineType': machine_type_url,
          #'disks': [{
          #      'autoDelete': 'true',
          #      'boot': 'true',
          #      'type': 'PERSISTENT',
          #      'initializeParams' : {
          #              'diskname': self.DEFAULT_ROOT_PD_NAME,
          #              'sourceImage': image_url
          #              }
          #      }],
          'disks': [{
                'source':disk_url,
                'boot': 'true',
                'autoDelete':'true',
                'type': 'PERSISTENT',
                }],
          #'image': image_url,
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
          }],
          'metadata': {
              'items': [
                {
                  'key': 'user-data',
                  'value': user_data ,
                },
                
            ]
          }
        }
        
        
        # Create the instance
        response = None
        request = self.gce_service.instances().insert(
             project=self.project_id, body=instance, zone=self.DEFAULT_ZONE)
        try:
            response = request.execute(http=self.auth_http)
            response = self._blocking_call(self.gce_service, self.auth_http, response)
            #log.info('Created VM ')
        except Exception, e:
            log.error("Error creating VM on gce: %s" % e)
            pass

        if response and 'targetId' in response:
            target_id = response['targetId']
        elif response:
            return
        else:
            #print 'no response'
            return
        vm_mementry = self.find_mementry(vm_mem)
        if (vm_mementry < 0):
            #TODO: this is kind of pointless with EC2..., but the resource code depends on it
            log.debug("Cluster memory list has no sufficient memory " +\
                      "entries (Not supposed to happen). Returning error.")
            return self.ERROR
        new_vm = cluster_tools.VM(name = next_instance_name, vmtype = vm_type, user = vm_user,
                    clusteraddr = self.network_address, id = target_id,
                    cloudtype = self.cloud_type, network = vm_networkassoc,
                    hostname = self.construct_hostname(next_instance_name),
                    cpuarch = "x86", image= vm_image, mementry = vm_mementry,
                    memory = vm_mem, cpucores = vm_cores, storage = vm_storage, 
                    keep_alive = vm_keepalive, job_per_core = job_per_core)
    
        try:
            self.resource_checkout(new_vm)
        except:
            log.exception("Unexpected Error checking out resources when creating a VM. Programming error?")
            self.vm_destroy(new_vm, reason="Failed Resource checkout")
            return self.ERROR
    
        self.vms.append(new_vm)
        #log.info("added a new vm %s"%new_vm)
        return 0


    def vm_destroy(self, vm, return_resources=True, reason=""):
        # Delete an Instance
        log.info("googlecluster::destroy vm::%s"%vm.name)
        request = self.gce_service.instances().delete(
            project=self.project_id, instance=vm.name, zone=self.DEFAULT_ZONE)
        try:
            response = request.execute(http=self.auth_http)
            #log.info("Destroy vm, response execute  %s ret res %s"%(vm.name,return_resources))
            response = self._blocking_call(self.gce_service, self.auth_http, response)
            #log.info("Destroy vm, response waiting  %s"%vm.name)
        except:
            log.error("Failure while destroying VM %s. return leaving with removing resource from cloud sched" % (vm.name))
            return
        #log.info("Destroy VM %s, check response %s"%(vm.name,response))
        try:
            if response and response['status'] == 'DONE':
                # Delete references to this VM
                if return_resources:
                    self.resource_return(vm)
                with self.vms_lock:
                    self.vms.remove(vm)
                return 0
            else:
                log.debug("Error Destroying GCE VM: %s" % (vm.name))
                return 1
        except:
            log.exception("Error removing vm, possibly already removed")
            return 1

    def vm_poll(self, vm):
        #filter_str = ''.join(["id eq ", vm.id])
        #request = self.gce_service.instances().list(project=self.project_id, filter=filter_str)
        request = self.gce_service.instances().list(project=self.project_id, filter=None, zone=self.DEFAULT_ZONE)
        try:
            response = request.execute(http=self.auth_http)
        except Exception as e:
            log.error("Problem polling gce vm %s error %s will retry later." % (vm.id, e))
            return

        if response and 'items' in response:
            instances = response['items']
            
            for instance in instances:
                
                if 'id' in instance and instance['id'] == vm.id:
                    log.info("googlecluster::state::%s::inst %s::vm %s"%(vm.name,instance['status'],vm.status))
                    if instance and hasattr(vm, 'status') and vm.status != self.VM_STATES.get(instance['status'], "Starting"):
                        vm.last_state_change = int(time.time())
                        
                
                    vm.status = self.VM_STATES[instance['status']]
                    vm.ipaddress = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
                
        else:
            pass


    def _blocking_call(self, gce_service, auth_http, response):
        """Blocks until the operation status is done for the given operation."""
        if 'status' in response:
            status = response['status']
            
        while status != 'DONE' and response:
            if 'name' in response:
                operation_id = response['name']
            else:
                break
            if 'zone' in response:
                zone_name = response['zone'].split('/')[-1]
                request = gce_service.zoneOperations().get(project=self.project_id, operation=operation_id, zone=zone_name)
            else:
                request = gce_service.globalOperations().get(project=self.project_id, operation=operation_id)
            try:
                response = request.execute(http=auth_http)
            except:
                pass
            
            if response and 'status' in response:
                status = response['status']

            time.sleep(1)
        return response
    
    def generate_next_instance_name(self):
        potential_name = ''.join([self.gce_hostname_prefix, str(uuid.uuid4())])
        potential_name = potential_name[0:15]
        collision = False
        for vm in self.vms:
            if potential_name == vm.name:
                collision = True
                break
        if collision:
            potential_name = None
        return potential_name
    def construct_hostname(self, instance_name):
        return ''.join([instance_name, '.c.', self.project_id, '.internal'])
            
