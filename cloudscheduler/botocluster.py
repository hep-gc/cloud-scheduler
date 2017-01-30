import os
import sys
import gzip
import json
import time
import boto3
import string
import shutil
import logging
import botocore
import subprocess
import cluster_tools
import cloud_init_util
import cloudscheduler.config as config
import cloudscheduler.utilities as utilities
import datetime as dt
from cloudscheduler.job_management import _attr_list_to_dict
from httplib import BadStatusLine
from subprocess import Popen
from urlparse import urlparse
from cStringIO import StringIO

log = utilities.get_cloudscheduler_logger()

class BotoCluster(cluster_tools.ICluster):

    VM_STATES = {
            "running" : "Running",
            "pending" : "Starting",
            "shutting-down" : "Shutdown",
            "terminated" : "Shutdown",
            "error" : "Error",
    }

    ERROR = 1
    DEFAULT_INSTANCE_TYPE = config.default_VMInstanceType if config.default_VMInstanceType else "m1.small"
    DEFAULT_INSTANCE_TYPE_LIST = _attr_list_to_dict(config.default_VMInstanceTypeList)

    def __init__(self, name="Dummy Cluster", host="localhost", cloud_type="Dummy",
                 memory=[], max_vm_mem= -1, networks=[], vm_slots=0,
                 cpu_cores=0, storage=0, access_key_id=None, secret_access_key=None,
                 security_group=None, key_name="",
                 boot_timeout=None, secure_connection="", regions="",
                 reverse_dns_lookup=False,placement_zone=None, enabled=True, priority=0,
                 keep_alive=0, port=8773):

        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, boot_timeout=boot_timeout, enabled=enabled,
                        priority=priority,keep_alive=keep_alive,)

        if not security_group:
            security_group = ["default"]
        self.security_groups = security_group

        if not access_key_id or not secret_access_key:
            log.error("Cannot connect to cluster %s "
                      "because you haven't specified an access_key_id or "
                      "a secret_access_key" % self.name)

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.key_name = key_name
        self.secure_connection = secure_connection in ['True', 'true', 'TRUE']
        self.total_cpu_cores = -1
        self.regions = regions
        self.reverse_dns_lookup = reverse_dns_lookup in ['True', 'true', 'TRUE']
        self.placement_zone = placement_zone
        self.port = port


    def _get_connection(self):
        """
            _get_connection - get a boto connection object to this cluster

            returns a boto connection object, or none in the case of an error
        """
        connection = None
        if len(self.regions) > 0:
            region_name = self.regions[0]
        else:
            region_name = self.name

        if self.cloud_type == "AmazonEC2":
            try:
                log.debug("Not Implemented use the boto 2 version for AmazonEC2.")
                return None

            except Exception, e:
                log.error("Couldn't connect to Amazon EC2 because: %s" %
                                                                e.error_message)

        elif self.cloud_type == "Eucalyptus":
            try:
                log.verbose("Created a connection to Eucalyptus (%s)" % self.name)

            except boto.exception.EC2ResponseError, e:
                log.error("Couldn't connect to Eucalyptus EC2 because: %s" %
                                                               e.error_message)

        elif self.cloud_type.lower() == "opennebula":
            try:
                connection = boto3.client('ec2', region_name=self.regions, endpoint_url=self.host,
                                          aws_access_key_id=self.access_key_id, aws_secret_access_key=self.secret_access_key,
                                          config=botocore.client.Config(signature_version='v2'))
                log.verbose("Created a connection to OpenNebula.")
            except Exception as e:
                log.error("Couldn't connect to OpenNebula: %s" % e.error_message)

        elif self.cloud_type == "OpenStack":
            try:
                log.debug("Use the boto2  interface for OpenStack ec2 accounts.")
                #log.verbose("Created a connection to OpenStack (%s)" % self.name)
                return None

            except boto.exception.EC2ResponseError, e:
                log.error("Couldn't connect to OpenStack because: %s" %
                            e.error_message)
        else:
            log.error("BotoCluster don't know how to handle a %s cluster." %
                                                               self.cloud_type)

        return connection

    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  pre_customization=None, vm_keepalive=0, instance_type="",
                  maximum_price=0, job_per_core=False, securitygroup=[],
                  key_name="",use_cloud_init=False, extra_userdata=[]):
        """Attempt to boot a new VM on the cluster."""

        use_cloud_init = use_cloud_init or config.use_cloud_init
        log.verbose("Trying to boot %s on %s" % (vm_type, self.network_address))
        if len(securitygroup) != 0:
            sec_group = []
            for group in securitygroup:
                if group in self.security_groups:
                    sec_group.append(group)
            if len(sec_group) == 0:
                log.debug("No matching security groups - trying default config")
                sec_group = self.security_groups
                #sec_group.append("default") - don't just append default use what is in cloud_resources.conf for this cloud
        else:
            sec_group = self.security_groups

        try:
            if self.name in vm_image.keys():
                vm_ami = vm_image[self.name]
            else:
                vm_ami = vm_image[self.network_address]
        except:
            log.debug("No AMI for %s, trying default" % self.network_address)
            #try:
            #    vm_ami = vm_image["default"]
            #except:
                #log.debug("No given default - trying global defaults")
            try:
                vm_default_ami = _attr_list_to_dict(config.default_VMAMI)
                if self.name in vm_default_ami.keys():
                    vm_ami = vm_default_ami[self.name]
                else:
                    vm_ami = vm_default_ami[self.network_address]
            except:
                try:
                    vm_ami = vm_default_ami["default"]
                except:
                    log.exception("Can't find a suitable AMI")
                    self.failed_image_set.add(vm_ami)
                    return

        try:
            if self.name in instance_type.keys():
                i_type = instance_type[self.name]
            elif self.network_address in instance_type.keys():
                i_type = instance_type[self.network_address]
            else:
                i_type = instance_type["default"]
        except:
            log.debug("No instance type for %s, trying default" % self.network_address)
            #try:
            #    i_type = instance_type["default"]
            #except:
            #    if isinstance(instance_type, str):
            #        i_type = instance_type
            #    else:
            try:
                if self.name in self.DEFAULT_INSTANCE_TYPE_LIST.keys():
                    i_type = self.DEFAULT_INSTANCE_TYPE_LIST[self.name]
                else:
                    i_type = self.DEFAULT_INSTANCE_TYPE_LIST[self.network_address]
            except:
                log.debug("No default instance type found for %s, trying single default" % self.network_address)
                i_type = self.DEFAULT_INSTANCE_TYPE
        instance_type = i_type

        if key_name == "" or key_name == None:
            key_name = self.key_name if self.key_name else ""

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
            user_data = cloud_init_util.build_multi_mime_message([(user_data, 'cloud-config', 'cloud_conf.yaml')], extra_userdata)

        if "AmazonEC2" == self.cloud_type and vm_networkassoc != "public":
            log.debug("You requested '%s' networking, but EC2 only supports 'public'" % vm_networkassoc)
            addressing_type = "public"
        else:
            addressing_type = vm_networkassoc

        user_data = utilities.gzip_userdata(user_data)
        try:
            client = self._get_connection()
            #Uncomment for debugging boto calls
            #boto3.set_stream_logger('botocore')
            resp = client.run_instances(ImageId=vm_ami, MinCount=1, MaxCount=1, InstanceType=instance_type, UserData=user_data, KeyName=key_name, SecurityGroups=sec_group)
            # will need to figure out how PlacementGroups will work still will probably just be Placement={"AvailabilityZone':placement_zone}
        except Exception as e:
            #print e
            #print e.__dict__

            log.error("Problem creating instance %s" % e)
            return self.ERROR

        if not vm_keepalive and self.keep_alive: #if job didn't set a keep_alive use the clouds default
            vm_keepalive = self.keep_alive
        if 'Instances' in resp.keys():
            new_vm_id =resp['Instances'][0]['InstanceId']
        else:
            #print resp.keys()
            new_vm_id = "unable to get id"
            return self.ERROR
        new_vm = cluster_tools.VM(name = vm_name, id = new_vm_id, vmtype = vm_type, user = vm_user,
                    clusteraddr = self.network_address,
                    cloudtype = self.cloud_type, network = vm_networkassoc,
                    image= vm_ami, flavor=instance_type,
                    memory = vm_mem, cpucores = vm_cores, storage = vm_storage,
                    keep_alive = vm_keepalive, job_per_core = job_per_core)

        #try:
        #    new_vm.spot_id = spot_id
        #except:
        #    log.verbose("No spot ID to add to VM %s" % instance_id)

        try:
            self.resource_checkout(new_vm)
        except:
            log.exception("Unexpected Error checking out resources when creating a VM. Programming error?")
            self.vm_destroy(new_vm, reason="Failed Resource checkout", return_resources=False)
            return self.ERROR

        self.vms.append(new_vm)

        return 0


    def vm_poll(self, vm):
        """Query the cloud service for information regarding a VM."""
        client = self._get_connection()
        response = client.describe_instances(InstanceIds=[vm.id])
        if response:
            for instance in response['Reservations'][0]['Instances']:
                if instance['InstanceId'] == vm.id:
                    if vm.status != instance['State']['Name']:
                        vm.last_state_change = int(time.time())
                    vm.status = instance['State']['Name']
                    if not vm.hostname:
                        vm.hostname = instance['PublicDnsName']
                    break
            else:
                log.debug("Unable to find vm: %s" % vm.id)
                return 'Error'

        return vm.status

    def vm_destroy(self, vm, return_resources=True, reason=""):
        """
        Shutdown, destroy and return resources of a VM to it's cluster

        Parameters:
        vm -- vm to shutdown and destroy
        return_resources -- if set to false, do not return resources from VM to cluster
        """
        log.info("Destroying VM: %s Name: %s on %s Reason: %s" % (vm.id, vm.hostname, self.name, reason))
        client = self._get_connection()
        try:
            client.terminate_instances(InstanceIds=[vm.id])
        except Exception as e:
            log.error("Problem destroying VM %s: %s" %(vm.hostname, e))
            # will need to detect correct exception / message for no longer existing VM and clean it from CS by falling
            # through and continuing with the resource return, if other exception return error
            return self.ERROR

        if return_resources and vm.return_resources:
            self.resource_return(vm)
        with self.vms_lock:
            try:
                self.vms.remove(vm)
            except Exception as e:
                log.error("Unable to remove VM %s on %s: %s" % (vm.id, self.name, e))

        return 0
