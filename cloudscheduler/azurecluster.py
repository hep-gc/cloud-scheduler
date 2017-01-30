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
    AZURE_SERVICE_NAME = "CloudSchedulerService"
    VM_STATES = {
        "Active": "Starting",
        "BusyRole": "Starting",
        "CreatingRole": "Starting",
        "CreatingVM": "Starting",
        "Deleting": "Shutdown",
        "DeletingVM": "Shutdown",
        "Deploying": "Starting",
        "Error": "Error",
        "Preparing": "Starting",
        "Provisioning": "Starting",
        "ProvisioningFailed": "Error",
        "ProvisioningTimeout": "Error",
        "ReadyRole": "Running",
        "RestartingRole": "Restarting",
        "RoleStateUnknown": "Starting",
        "Running": "Running",
        "StartingVM": "Starting",
        "StartingRole": "Starting",
        "StoppedDeallocated": "Stopped",
        "StoppedVM": "Stopped",
        "StoppingRole": "StopRole",
        "StoppingVM": "Shutdown",
        "Suspended": "Suspended",
        "Suspending": "Suspending",
        "Unknown": "Error",
    }

    def __init__(self, name="Dummy Cluster", cloud_type="Dummy",
                 memory=[], max_vm_mem=-1, networks=[], vm_slots=0,
                 cpu_cores=0, storage=0, security_group=None,
                 username=None, password=None, tenant_name=None, auth_url=None,
                 key_name=None, boot_timeout=None, secure_connection="",
                 regions="", reverse_dns_lookup=False, placement_zone=None,
                 enabled=True, priority=0, keycert=None, keep_alive=0, blob_url="", service_name=None):

        # Call super class's init
        cluster_tools.ICluster.__init__(self, name=name, host="azure.microsoft.com", cloud_type=cloud_type,
                                        memory=memory, max_vm_mem=max_vm_mem, networks=networks,
                                        vm_slots=vm_slots, cpu_cores=cpu_cores,
                                        storage=storage, boot_timeout=boot_timeout,
                                        enabled=enabled,
                                        priority=priority, keep_alive=keep_alive, )
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
        self.placement_zone = placement_zone
        self.keycert = keycert
        self.blob_url = blob_url
        self.count = 0
        self.azure_service_name = service_name if service_name else self.AZURE_SERVICE_NAME

    def __getstate__(self):
        """Override to work with pickle module."""
        state = cluster_tools.ICluster.__getstate__(self)
        return state

    def __setstate__(self, state):
        """Override to work with pickle module."""
        cluster_tools.ICluster.__setstate__(self, state)

    def vm_create(self, vm_name, vm_type, vm_user,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  vm_keepalive=0, instance_type={}, job_per_core=False,
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
            user_data = cloud_init_util.build_multi_mime_message([(user_data, 'cloud-config', 'cloud_conf.yaml')],
                                                                 extra_userdata)
            if not user_data:
                log.error("Problem building cloud-config user data.")
                return self.ERROR

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

        name = self._generate_next_name()
        instance = None
        req = None
        if name:
            sms = self._get_service_connection()
            try:
                conf_set = azure.servicemanagement.LinuxConfigurationSet(host_name=name, user_name=self.username,
                                                                         user_password=self.password,
                                                                         disable_ssh_password_authentication=False,
                                                                         custom_data=user_data)
                net_set = azure.servicemanagement.ConfigurationSet()
                vm_ssh_port = 20000+self.count
                net_set.input_endpoints.input_endpoints.append(
                    azure.servicemanagement.ConfigurationSetInputEndpoint(name='SSH',
                                                                          protocol='TCP',
                                                                          port=vm_ssh_port,
                                                                          local_port=22))
                self.count += 1
                if self.count > 15000:
                    self.count = 0
                os_hd = azure.servicemanagement.OSVirtualHardDisk(image, self.blob_url + name)

                res = sms.check_hosted_service_name_availability(self.azure_service_name)
                if res.result:
                    req = sms.create_hosted_service(self.azure_service_name, self.azure_service_name, location=self.regions[0])
                    sms.wait_for_operation_status(req.request_id)
                if len(self.vms) == 0:
                    req = sms.create_virtual_machine_deployment(service_name=self.azure_service_name,
                                                                deployment_name=self.azure_service_name,
                                                                deployment_slot='production',
                                                                role_name=name, label=name,
                                                                system_config=conf_set, network_config=net_set,
                                                                os_virtual_hard_disk=os_hd, role_size=i_type)
                    try:
                        op_status = sms.wait_for_operation_status(req.request_id)
                    except Exception as e:
                        log.error("Problem creating VM on Azure: %s" % e.result.error.message)
                        return 1
                else:
                    req = sms.add_role(service_name=self.azure_service_name, deployment_name=self.azure_service_name,
                                       role_name=name, system_config=conf_set, network_config=net_set,
                                       os_virtual_hard_disk=os_hd, role_size=i_type)
                    try:
                        op_status = sms.wait_for_operation_status(req.request_id)
                    except Exception as e:
                        log.error("Problem creating VM on Azure: %s" % e.result.error.message)
                        return 1
            except Exception as e:
                log.error("Unhandled exception while creating vm on %s: %s" % (self.name, e))
                return self.ERROR
            if req:
                if not vm_keepalive and self.keep_alive:  # if job didn't set a keep_alive use the clouds default
                    vm_keepalive = self.keep_alive

                new_vm = cluster_tools.VM(name=vm_name, id=name, vmtype=vm_type, user=vm_user,
                                          clusteraddr=self.network_address,
                                          hostname=name,
                                          cloudtype=self.cloud_type, network=None,
                                          image=vm_image, flavor=i_type,
                                          memory=vm_mem, cpucores=vm_cores, storage=vm_storage,
                                          keep_alive=vm_keepalive, job_per_core=job_per_core, ssh_port=vm_ssh_port)

                try:
                    self.resource_checkout(new_vm)
                    log.info("Launching 1 VM: %s on %s" % (name, self.name))
                except:
                    log.error("Unexpected Error checking out resources when creating a VM. Programming error?")
                    self.vm_destroy(new_vm, reason="Failed Resource checkout", return_resources=False)
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
        log.info("Destroying VM: %s Name: %s on %s tenant: %s Reason: %s" % (
        vm.id, vm.hostname, self.name, self.tenant_name, reason))
        try:
            azure_conn = self._get_service_connection()
            req = azure_conn.delete_role(self.azure_service_name, self.azure_service_name, vm.id, True)
            azure_conn.wait_for_operation_status(req.request_id)
        except Exception as e:
            log.debug("Problem destroying VM on Azure: %s" % e)
            try:
                if "only role present" in e.message:
                    try:
                        azure_conn = self._get_service_connection()
                        req = azure_conn.delete_hosted_service(self.azure_service_name, True)
                        azure_conn.wait_for_operation_status(req.request_id)
                    except Exception as e:
                        log.error("Problem deleteing the CS Azure service: %s" % e)
                        return 1
                elif "hosted service name is invalid" in e.message or 'does not exist' in e.message or \
                     "not found in the currently deployed service" in e.message:
                    log.error("Invalid service name on %s : %s, dropping from CS" % (self.name, e))
                elif "Not Found" in e.message:
                    log.error("VM %s not found on azure, may already be destroyed, dropping from CS" % vm.id)
                else:
                    log.error("Unhandled exception while destroying VM on %s : %s" % (self.name, e))
                    return 1
            except:
                log.error("Failed to log exception properly?")
                return 1

        # Delete references to this VM
        try:
            if return_resources and vm.return_resources:
                self.resource_return(vm)
            with self.vms_lock:
                self.vms.remove(vm)
                log.info("VM %s removed from %s list" % (vm.id, self.name))
        except Exception as e:
            log.error("Error removing vm from list: %s" % e)
            return 1

        return 0

    def vm_poll(self, vm):
        """ Query Azure for status information of VMs."""

        instance = None
        azure_conn = self._get_service_connection()
        vm_info = None
        try:
            vm_info = azure_conn.get_hosted_service_properties(self.azure_service_name, True)
        except Exception as e:
            log.error("Unable to find service with name: %s on Azure. %s" % (self.azure_service_name, e))
            vm.status = self.VM_STATES['Error']
            return vm.status
        if vm_info and len(vm_info.deployments) == 0:
            log.debug("No VMs running on service: %s, skipping." % vm_info.service_name)
            vm.status = self.VM_STATES['Error']
            return vm.status
        if vm_info:
            for vm_instance in vm_info.deployments:
                for role in vm_instance.role_instance_list.role_instances:
                    if role.role_name == vm.id:
                        instance = role
                        break
                else:
                    log.debug("Unable to find VM: %s on Azure" % vm.id)

        with self.vms_lock:
            if instance and vm.status != self.VM_STATES.get(
                    instance.instance_status, "Starting"):
                vm.last_state_change = int(time.time())
                log.debug("VM: %s on %s. Changed from %s to %s." % (vm.id, self.name, vm.status, self.VM_STATES.get(
                    instance.instance_status, "Starting")))

            if instance and instance.instance_status in self.VM_STATES.keys():
                vm.status = self.VM_STATES[instance.instance_status]
            elif instance:
                vm.status = instance.instance_status
            else:
                vm.status = self.VM_STATES['Error']
        return vm.status

    def _get_service_connection(self):
        return azure.servicemanagement.ServiceManagementService(self.tenant_name, self.keycert)
