import os
import sys
import time
import string
import shutil
import logging
import nimbus_xml
import subprocess
import cluster_tools
import cloudscheduler.config as config
import cloudscheduler.utilities as utilities
from cloudscheduler.job_management import _attr_list_to_dict

log = utilities.get_cloudscheduler_logger()

class OpenStackCluster(cluster_tools.ICluster):
    ERROR = 1
    VM_STATES = {
            "BUILD" : "Starting",
            "ACTIVE" : "Running",
            "SHUTOFF" : "Shutdown",
            "SUSPENDED": "Suspended",
            "PAUSED": "Paused",
            "ERROR" : "Error",
    }
    def __init__(self, name="Dummy Cluster", host="localhost", cloud_type="Dummy",
                 memory=[], max_vm_mem= -1, cpu_archs=[], networks=[], vm_slots=0,
                 cpu_cores=0, storage=0,
                 access_key_id=None, secret_access_key=None, security_group=None,
                 username=None, password=None, tenant_name=None, auth_url=None,
                 hypervisor='xen', key_name=None, boot_timeout=None, secure_connection="",
                 regions=[], vm_domain_name="", reverse_dns_lookup=False,placement_zone=None, enabled=True):

        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, cpu_archs=cpu_archs, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, hypervisor=hypervisor, boot_timeout=boot_timeout, enabled=enabled)
        try:
            import novaclient.v1_1.client as nvclient
            import keystoneclient.v2_0.client as ksclient
        except:
                print "Unable to import novaclient - cannot use native openstack cloudtypes"
                sys.exit(1)
        if not security_group:
            security_group = ["default"]
        self.security_groups = security_group

        if not access_key_id or not secret_access_key:
            log.error("Cannot connect to cluster %s "
                      "because you haven't specified an access_key_id or "
                      "a secret_access_key" % self.name)

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.username = username
        self.password = password
        self.tenant_name = tenant_name
        self.auth_url = auth_url
        self.key_name = key_name
        self.secure_connection = secure_connection in ['True', 'true', 'TRUE']
        self.total_cpu_cores = -1
        self.regions = regions
        self.vm_domain_name = vm_domain_name if vm_domain_name != None else ""
        self.reverse_dns_lookup = reverse_dns_lookup in ['True', 'true', 'TRUE']
        self.placement_zone = placement_zone
        self.vm_counter = 0
    
    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc, vm_cpuarch,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  vm_keepalive=0, instance_type="", job_per_core=False, 
                  securitygroup=[],key_name=""):
        """ Create a VM on OpenStack."""
        nova = self._get_creds_nova()
        if len(key_name) > 0:
            if not nova.keypairs.findall(name=key_name):
                key_name = ""
        try:
            image = vm_image[self.name]
        except:
            try:
                image = vm_image[self.network_address]
            except:
                try:
                    vm_default_ami = _attr_list_to_dict(config.default_VMAMI)
                    if self.name in vm_default_ami.keys():
                        image = vm_default_ami[self.name]
                    else:
                        image = vm_default_ami[self.network_address]
                except:
                    try:
                        image = vm_default_ami["default"]
                    except:
                        log.exception("Can't find a suitable AMI")
                        return
        image = nova.images.find(name=image)
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
                i_type = self.DEFAULT_INSTANCE_TYPE   
        flavor = nova.flavors.find(name=i_type)   
        # Need to get the rotating hostname from the google code to use for here.  
        name = self._generate_next_name()
        if name:
            try:
                instance = nova.servers.create(name=name, image=image, flavor=flavor, key_name=key_name)
                #print instance.__dict__
            except Exception as e:
                #print e
                log.exception(e)
            if instance:
                instance_id = instance.id
            
            new_vm = cluster_tools.VM(name = vm_name, id = instance_id, vmtype = vm_type, user = vm_user,
                        clusteraddr = self.network_address, hostname = name,
                        cloudtype = self.cloud_type, network = vm_networkassoc,
                        cpuarch = vm_cpuarch, image= vm_image,
                        memory = vm_mem, cpucores = vm_cores, storage = vm_storage, 
                        keep_alive = vm_keepalive, job_per_core = job_per_core)
    
            try:
                self.resource_checkout(new_vm)
            except:
                log.exception("Unexpected Error checking out resources when creating a VM. Programming error?")
                self.vm_destroy(new_vm, reason="Failed Resource checkout")
                return self.ERROR
    
            self.vms.append(new_vm)
        else:
            log.debug("Unable to generate name for %" % self.name)
            return self.ERROR

        return 0

    def vm_destroy(self, vm, return_resources=True, reason=""):
        """ Destroy a VM on OpenStack."""
        nova = self._get_creds_nova()
        instance = nova.servers.get(vm.id)
        try:
            instance.delete()
        except Exception as e:
            log.exception(e)
            return 1

        
        # Delete references to this VM
        if return_resources:
            self.resource_return(vm)
        with self.vms_lock:
            self.vms.remove(vm)

        return 0
    def vm_poll(self, vm):
        """ Query OpenStack for status information of VMs."""
        nova = self._get_creds_nova()
        instance = nova.servers.get(vm.id)
        with self.vms_lock:
            #print instance.status
            if instance and vm.status != self.VM_STATES.get(instance.status, "Starting"):

                vm.last_state_change = int(time.time())
                log.debug("VM: %s on %s. Changed from %s to %s." % (vm.id, self.name, vm.status, self.VM_STATES.get(instance.status, "Starting")))
            if instance.status in self.VM_STATES.keys():
                vm.status = self.VM_STATES[instance.status]
            else:
                vm.status = instance.status
        return vm.status

    def _get_creds_ks(self):
        """Get an auth token to Keystone."""
        return ksclient.Client(username=self.username, password=self.password, auth_url=self.auth_url, tenant_name=self.tenant_name)
    def _get_creds_nova(self):
        """Get an auth token to Nova."""
        try:
            import novaclient.v1_1.client as nvclient
        except:
                print "Unable to import novaclient - cannot use native openstack cloudtypes"
                sys.exit(1)

        return nvclient.Client(username=self.username, api_key=self.password, auth_url=self.auth_url, project_id=self.tenant_name)

    def _generate_next_name(self):
        name = ''.join(['csvm-', self.name.replace('_', '-'), str(self.vm_counter)])
        self.vm_counter += 1
        if self.vm_counter > 500000:
            self.vm_counter = 0
        collision = False
        for vm in self.vms:
            if name == vm.hostname:
                collision= True
                break
        if collision:
            name = None
        return name

