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
from cStringIO import StringIO
import gzip

log = utilities.get_cloudscheduler_logger()

class OpenStackCluster(cluster_tools.ICluster):
    ERROR = 1
    DEFAULT_INSTANCE_TYPE = config.default_VMInstanceType if config.default_VMInstanceType else "m1.small"
    DEFAULT_INSTANCE_TYPE_LIST = _attr_list_to_dict(config.default_VMInstanceTypeList)
    VM_STATES = {
            "BUILD" : "Starting",
            "ACTIVE" : "Running",
            "SHUTOFF" : "Shutdown",
            "SUSPENDED": "Suspended",
            "STOPPED": "Stopped",
            "PAUSED": "Paused",
            "ERROR" : "Error",
            "VERIFY_RESIZE": "Error",
    }
    def __init__(self, name="Dummy Cluster", cloud_type="Dummy",
                 memory=[], max_vm_mem= -1, networks=[], vm_slots=0,
                 cpu_cores=0, storage=0, security_group=None,
                 username=None, password=None, tenant_name=None, auth_url=None,
                 key_name=None, boot_timeout=None, secure_connection="",
                 regions="", reverse_dns_lookup=False,placement_zone=None,
                 enabled=True, priority=0, cacert=None,keep_alive=0,):

        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name, host=auth_url, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, boot_timeout=boot_timeout, enabled=enabled,
                         priority=priority, keep_alive=keep_alive,)
        try:
            import novaclient.v2.client as nvclient
            import novaclient.exceptions
            #import keystoneclient.v2_0.client as ksclient
        except:
                print "Unable to import novaclient - cannot use native openstack cloudtypes"
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
        self.reverse_dns_lookup = reverse_dns_lookup in ['True', 'true', 'TRUE']
        self.placement_zone = placement_zone
        self.flavor_set = set()
        self.cacert = cacert
        self.session = self._get_keystone_session()
    
    def __getstate__(self):
        """Override to work with pickle module."""
        state = cluster_tools.ICluster.__getstate__(self)
        del state['flavor_set']
        del state['session']
        return state

    def __setstate__(self, state):
        """Override to work with pickle module."""
        cluster_tools.ICluster.__setstate__(self, state)
        self.flavor_set = set()
        self.session = self._get_keystone_session()
    
    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  vm_keepalive=0, instance_type="", job_per_core=False, 
                  securitygroup=[],key_name="", pre_customization=None, use_cloud_init=False, extra_userdata=[]):
        """ Create a VM on OpenStack."""

        import novaclient.exceptions
        use_cloud_init = use_cloud_init or config.use_cloud_init
        nova = self._get_creds_nova()
        if len(securitygroup) != 0:
            sec_group = []
            for group in securitygroup:
                if group in self.security_groups:
                    sec_group.append(group)
            if len(sec_group) == 0:
                log.debug("No defined security groups for job - trying default value from cloud_resources.conf")
                sec_group = self.security_groups
        else:
            sec_group = self.security_groups
        log.debug("Using security group: %s" % str(sec_group))
        if key_name and len(key_name) > 0:
            if not nova.keypairs.findall(name=key_name):
                key_name = ""
        else:
            key_name = self.key_name if self.key_name else ""
        if customization:
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
            user_data = cloud_init_util.inject_customizations([], user_data)
        if len(extra_userdata) > 0:
            # need to use the multi-mime type functions
            user_data = cloud_init_util.build_multi_mime_message([(user_data, 'cloud-config', 'cloud_conf.yaml')], extra_userdata)
            if not user_data:
                log.error("Problem building cloud-config user data.")
                return self.ERROR
        #with open('/tmp/userdata.yaml', 'w') as f:
            #f.write(user_data)
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
            imageobj = nova.images.find(name=image)
        except novaclient.exceptions.EndpointNotFound:
            log.error("Endpoint not found, are your region settings correct for %s" % self.name)
            return -4
        except Exception as e:
            log.warning("Exception occurred while trying to fetch image via name: %s %s" % (image, e))
            try:
                imageobj = nova.images.get(image)
                log.debug("Got image via uuid: %s" % image)
            except novaclient.exceptions.EndpointNotFound:
                log.error("Endpoint not found, are your region settings correct for %s" % self.name)
                return -4
            except Exception as e:
                log.exception("Unable to fetch image via uuid: %s %s" % (image, e))
                self.failed_image_set.add(image)
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
        try:   
            flavor = nova.flavors.find(name=i_type)
        except Exception as e:
            log.warning("Exception occurred while trying to get flavor by name: %s - will attempt to use name value as a uuid." % e)
            try:
                flavor = nova.flavors.get(i_type)
                log.debug("Got flavor via uuid: %s" % i_type)
            except Exception as ex:
                log.error("Exception occurred trying to get flavor by uuid: %s" % ex)
                return
        self.flavor_set.add(flavor)
        # find the network id to use if more than one network
        if vm_networkassoc:
            network = self._find_network(vm_networkassoc)
            if network:
                netid = [{'net-id': network.id}]
            else:
                log.debug("Unable to find network named: %s on %s" % (vm_networkassoc, self.name))
                netid = []
        elif self.network_pools and len(self.network_pools) > 0:
            network = self._find_network(self.network_pools[0])
            if network:
                netid = [{'net-id': network.id}]
            else:
                log.debug("Unable to find network named: %s on %s" % (self.network_pools[0], self.name))
                netid = []
        else:
            netid = []
        # Need to get the rotating hostname from the google code to use for here.  
        name = self._generate_next_name()
        instance = None

        if name:
            try:
                instance = nova.servers.create(name=name, image=imageobj, flavor=flavor, key_name=key_name, 
                                               availability_zone=self.placement_zone, nics =netid, userdata=user_data, security_groups=sec_group)
                #print instance.__dict__
            except novaclient.exceptions.OverLimit as e:
                log.info("Unable to create VM without exceeded quota on %s: %s" % (self.name, e.message))
            except Exception as e:
                #print e
                log.error("Unhandled exception while creating vm on %s: %s" %(self.name, e))
            if instance:
                instance_id = instance.id
                if not vm_keepalive and self.keep_alive: #if job didn't set a keep_alive use the clouds default
                    vm_keepalive = self.keep_alive

                new_vm = cluster_tools.VM(name = vm_name, id = instance_id, vmtype = vm_type, user = vm_user,
                            clusteraddr = self.network_address, hostname = name,
                            cloudtype = self.cloud_type, network = vm_networkassoc,
                            image= vm_image, flavor=flavor.name,
                            memory = vm_mem, cpucores = vm_cores, storage = vm_storage, 
                            keep_alive = vm_keepalive, job_per_core = job_per_core)
    
                try:
                    self.resource_checkout(new_vm)
                    log.info("Launching 1 VM: %s on %s under tenant: %s" % (instance_id, self.name, self.tenant_name))
                except:
                    log.error("Unexpected Error checking out resources when creating a VM. Programming error?")
                    self.vm_destroy(new_vm, reason="Failed Resource checkout", return_resources=False)
                    return self.ERROR
        
                self.vms.append(new_vm)
            else:
                log.debug("Failed to create instance on %s" % self.name)
                return self.ERROR
        else:
            log.debug("Unable to generate name for %" % self.name)
            return self.ERROR

        return 0

    def vm_destroy(self, vm, return_resources=True, reason=""):
        """ Destroy a VM on OpenStack."""
        nova = self._get_creds_nova()
        import novaclient.exceptions
        log.info("Destroying VM: %s Name: %s on %s tenant: %s Reason: %s" % (vm.id, vm.hostname, self.name, self.tenant_name, reason))
        try:
            instance = nova.servers.get(vm.id)
            instance.delete()
        except novaclient.exceptions.NotFound as e:
            log.error("VM %s not found on %s: removing from CS" % (vm.id, self.name))
        except Exception as e:
            try:
                log.error("Unhandled exception while destroying VM on %s : %s" % (self.name,e))
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
        except Exception as e:
            log.error("Error removing vm from list: %s" % e)
            return 1

        return 0
    def vm_poll(self, vm):
        """ Query OpenStack for status information of VMs."""
        import novaclient.exceptions
        nova = self._get_creds_nova()
        instance = None
        try:
            instance = nova.servers.get(vm.id)
        except novaclient.exceptions.NotFound as e:
            log.exception("VM %s not found on %s: %s" % (vm.id, self.name, e))
            vm.status = self.VM_STATES['ERROR']
        except Exception as e:
            try:
                log.error("Unexpected exception occurred polling vm %s: %s" % (vm.id, e))
            except:
                log.error("Failed to log exception properly: %s" % vm.id)
        with self.vms_lock:
            #print instance.status
            if instance and vm.status != self.VM_STATES.get(instance.status, "Starting"):

                vm.last_state_change = int(time.time())
                log.debug("VM: %s on %s. Changed from %s to %s." % (vm.id, self.name, vm.status, self.VM_STATES.get(instance.status, "Starting")))
            if instance and instance.status in self.VM_STATES.keys():
                vm.status = self.VM_STATES[instance.status]
            elif instance:
                vm.status = instance.status
            else:
                vm.status = self.VM_STATES['ERROR']
        return vm.status

    def _get_creds_nova(self):
        """Get an auth token to Nova."""
        try:
            import novaclient.v2.client as nvclient
        except Exception as e:
                print "Unable to import novaclient - cannot use native openstack cloudtypes"
                print e
                sys.exit(1)
        try:
            #client = nvclient.Client(username=self.username, api_key=self.password, auth_url=self.auth_url, project_id=self.tenant_name,
            #                        region_name=self.regions[0], cacert=self.cacert)
            client = nvclient.Client(session=self.session, region_name=self.regions[0], timeout=10)
        except Exception as e:
            log.error("Unable to create connection to %s: Reason: %s" % (self.name, e))
        return client

    def _get_keystone_session(self):
        try:
            from keystoneclient.auth.identity import v2
            from keystoneclient import session
            auth = v2.Password(auth_url=self.auth_url, username=self.username, password=self.password, tenant_name=self.tenant_name)
            sess = session.Session(auth=auth, verify=self.cacert)
        except Exception as e:
            log.error("Problem importing keystone modules, and getting session: %s" % e)
        log.debug("Session object for %s created" % self.name)
        return sess
    
    def _find_network(self, name):
        nova = self._get_creds_nova()
        network = None
        try:
            networks = nova.networks.list()
            for net in networks:
                if net.label == name:
                    network = net
        except Exception as e:
            log.error("Unable to list networks on %s Exception: %s" % (self.name, e))
        return network

