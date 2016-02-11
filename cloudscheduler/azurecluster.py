import os
import sys
import time
import uuid
import string
import shutil
import logging
import subprocess
import cluster_tools
import cloud_init_util
import cloudscheduler.config as config
import cloudscheduler.utilities as utilities
from cloudscheduler.job_management import _attr_list_to_dict
try:
    import azure
    import azure.servicemanagement
except:
    pass
log = utilities.get_cloudscheduler_logger()

class AzureCluster(cluster_tools.ICluster):
    ERROR = 1
    DEFAULT_INSTANCE_TYPE = config.default_VMInstanceType if config.default_VMInstanceType else "m1.small"
    DEFAULT_INSTANCE_TYPE_LIST = _attr_list_to_dict(config.default_VMInstanceTypeList)
    VM_STATES = {
            "Unknown": "Error",
            "CreatingVM": "Starting",
            "StartingVM": "Starting",
            "CreatingRole": "Starting",
            "StartingRole": "Starting",
            "RoleStateUnknown": "Starting",
            "ReadyRole": "Running",
            "BusyRole": "Starting",
            "Preparing": "Starting",
            "Provisioning": "Starting",
            "ProvisioningFailed": "Error",
            "StoppingRole": "StopRole",
            "StoppingVM": "Shutdown",
            "DeletingVM": "Shutdown",
            "StoppedVM": "Stopped",
            "RestartingRole": "Restarting",
            "StoppedDeallocated": "Stopped",
            "Active": "Running",
            "Running": "Running",
            "ERROR": "Error",
    }
    def __init__(self, name="Dummy Cluster", cloud_type="Dummy",
                 memory=[], max_vm_mem= -1, networks=[], vm_slots=0,
                 cpu_cores=0, storage=0, security_group=None,
                 username=None, password=None, tenant_name=None, auth_url=None,
                 hypervisor='xen', key_name=None, boot_timeout=None, secure_connection="",
                 regions=[], vm_domain_name="", reverse_dns_lookup=False,placement_zone=None, 
                 enabled=True, priority=0, keycert=None,keep_alive=0, blob_url=""):

        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name, host=auth_url, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, hypervisor=hypervisor, boot_timeout=boot_timeout, enabled=enabled,
                         priority=priority, keep_alive=keep_alive,)
        try:
            import azure
            import azure.servicemanagement
        except:
                print "Unable to import azure-mgmt, unable to use Azure cloudtypes"
                sys.exit(1)
        if not security_group:
            security_group = ["default"]
        self.security_groups = security_group
        self.username = username if username else ""
        self.password = password if password else ""
        self.tenant_name = tenant_name if tenant_name else ""
        self.auth_url = auth_url if auth_url else ""
        self.key_name = key_name if key_name else ""
        self.secure_connection = secure_connection in ['True', 'true', 'TRUE']
        self.total_cpu_cores = -1
        self.regions = regions
        self.vm_domain_name = vm_domain_name if vm_domain_name != None else ""
        self.placement_zone = placement_zone
        self.keycert = keycert
        self.blob_url = blob_url
    
    def __getstate__(self):
        """Override to work with pickle module."""
        state = cluster_tools.ICluster.__getstate__(self)
        return state

    def __setstate__(self, state):
        """Override to work with pickle module."""
        cluster_tools.ICluster.__setstate__(self, state)
    
    def vm_create(self, vm_name, vm_type, vm_user,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  vm_keepalive=0, instance_type="", job_per_core=False, 
                  pre_customization=None, extra_userdata=[]):
        """ Create a VM on Azure."""

        use_cloud_init = True

        if customization:
            user_data = cloud_init_util.build_write_files_cloud_init(customization)
        else:
            user_data = ""
        if pre_customization:
            user_data = cloud_init_util.inject_customizations(pre_customization, user_data)
        elif use_cloud_init:
            user_data = cloud_init_util.inject_customizations([], user_data)
        if len(extra_userdata) > 0:
            # need to use the multi-mime type functions
            user_data = cloud_init_util.build_multi_mime_message([(user_data, 'cloud-config', 'cloud_conf.yaml')], extra_userdata)

        # Compress the user data to try and get under the limit
        user_data = utilities.gzip_userdata(user_data)
        
        try:
            if self.name in vm_image.keys():
                image = vm_image[self.name]
            elif self.network_address in vm_image.keys():
                image = vm_image[self.network_address]
            else:
                image = vm_image['default']
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

        try:
            if self.name in instance_type.keys():
                i_type = instance_type[self.name]
            elif self.network_address in instance_type.keys():
                i_type = instance_type[self.network_address]
            else:
                i_type = instance_type['default']
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

        # create the hosted service
        # create the configurationset and network set

        # Need to get the rotating hostname from the google code to use for here.  
        name = self._generate_next_name()
        instance = None

        if name:
            try:
                sms = self._get_service_connection()
                req = sms.create_hosted_service(name,name,name, self.regions[0])
                sms.wait_for_operation_status(req.request_id)
                conf_set = azure.servicemanagement.LinuxConfigurationSet(host_name=name, user_name=self.username,
                                                                        user_password=self.password, disable_ssh_password_authentication=False,
                                                                         custom_data=user_data)
                net_set = azure.servicemanagement.ConfigurationSet()
                net_set.input_endpoints.input_endpoints.append(azure.servicemanagement.ConfigurationSetInputEndpoint(name='SSH',
                                                                                                                     protocol='TCP',
                                                                                                                     port=22,
                                                                                                                     local_port=22))
                os_hd = azure.servicemanagement.OSVirtualHardDisk(image, self.blob_url+name)
                req = sms.create_virtual_machine_deployment(name,name, 'production', name,name, conf_set,network_config=net_set,
                                                            os_virtual_hard_disk=os_hd, role_size=i_type)

            except Exception as e:
                #print e
                log.error("Unhandled exception while creating vm on %s: %s" %(self.name, e))
                try:
                    sms.delete_hosted_service(name, True)
                except:
                    log.error("Problem cleaning up the failed service deployment: %s" % name)
            if req:
                if not vm_keepalive and self.keep_alive: #if job didn't set a keep_alive use the clouds default
                    vm_keepalive = self.keep_alive

                new_vm = cluster_tools.VM(name = vm_name, id = name, vmtype = vm_type, user = vm_user,
                            clusteraddr = self.network_address, hostname = ''.join([name, self.vm_domain_name]),
                            cloudtype = self.cloud_type, network = None,
                            image= vm_image, flavor=i_type,
                            memory = vm_mem, cpucores = vm_cores, storage = vm_storage, 
                            keep_alive = vm_keepalive, job_per_core = job_per_core)
    
                try:
                    self.resource_checkout(new_vm)
                    log.info("Launching 1 VM: %s on %s" % (name, self.name))
                except:
                    log.error("Unexpected Error checking out resources when creating a VM. Programming error?")
                    self.vm_destroy(new_vm, reason="Failed Resource checkout")
                    return self.ERROR
        
                self.vms.append(new_vm)
            else:
                log.debug("Failed to create instance on %s" % self.name)
                return self.ERROR
        else:
            log.debug("Unable to generate name for %s" % self.name)
            return self.ERROR

        return 0

    def vm_destroy(self, vm, return_resources=True, reason=""):
        """ Destroy a VM on Azure."""
        log.info("Destroying VM: %s Name: %s on %s tenant: %s Reason: %s" % (vm.id, vm.hostname, self.name, self.tenant_name, reason))
        try:
            azure_conn = self._get_service_connection()
            azure_conn.delete_hosted_service(vm.id, True)
        except Exception as e:
            try:
                if "hosted service name is invalid" in e.message:
                    log.error("Invalid service name on %s : %s, dropping from CS" % (self.name,e))
                else:
                    log.error("Unhandled exception while destroying VM on %s : %s" % (self.name,e))
                    return 1
            except:
                print e
                log.error("Failed to log exception properly?")
                return 1

        # Delete references to this VM
        try:
            if return_resources and vm.return_resources:
                self.resource_return(vm)
            with self.vms_lock:
                self.vms.remove(vm)
        except Exception as e:
            log.error("Error removing vm from list: %s" % e)
            return 1

        return 0
    def vm_poll(self, vm):
        """ Query Azure for status information of VMs."""
        instance = None
        try:
            azure_conn = self._get_service_connection()
            service_list = azure_conn.list_hosted_services()
        except Exception as e:
            try:
                log.error("Unexpected exception occurred polling vm %s: %s" % (vm.id, e))
            except:
                log.error("Failed to log exception properly: %s" % vm.id)
        for service in service_list:
            try:
                vm_info = azure_conn.get_hosted_service_properties(service.service_name, True)
            except Exception as e:
                log.error("Unable to find service with name: %s on Azure. %s" % (service.service_name, e))
            if vm_info and len(vm_info.deployments) == 0:
                log.debug("No VMs running on service: %s, skipping." % vm_info.service_name)
                continue
            if vm_info.service_name+self.vm_domain_name == vm.hostname:
                instance = vm_info
                break
        else:
            log.debug("Unable to find VM %s on Azure." % vm.hostname)
            instance = None
        with self.vms_lock:
            if instance and instance.deployments and instance.deployments[0].role_instance_list and vm.status != self.VM_STATES.get(instance.deployments[0].role_instance_list[0].instance_status, "Starting"):

                vm.last_state_change = int(time.time())
                log.debug("VM: %s on %s. Changed from %s to %s." % (vm.id, self.name, vm.status, self.VM_STATES.get(instance.deployments[0].role_instance_list[0].instance_status, "Starting")))

            if instance and instance.deployments and instance.deployments[0].role_instance_list and instance.deployments[0].role_instance_list[0].instance_status in self.VM_STATES.keys():
                vm.status = self.VM_STATES[instance.deployments[0].role_instance_list[0].instance_status]
            elif instance and instance.deployments and instance.deployments[0].role_instance_list:
                vm.status = instance.deployments[0].role_instance_list[0].instance_status
            else:
                vm.status = self.VM_STATES['ERROR']
        return vm.status

    def _get_service_connection(self):
        return azure.servicemanagement.ServiceManagementService(self.tenant_name, self.keycert)
