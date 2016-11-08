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
                 security_group=None, key_name=None,
                 boot_timeout=None, secure_connection="", regions=[],
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
                pass
                log.debug("Not Implemented use the boto 2 version for AmazonEC2.")
                return None
                #log.verbose("Created a connection to Amazon EC2")

            except Exception, e:
                log.error("Couldn't connect to Amazon EC2 because: %s" %
                                                                e.error_message)

        elif self.cloud_type == "Eucalyptus":
            try:
                pass
                log.verbose("Created a connection to Eucalyptus (%s)" % self.name)

            except boto.exception.EC2ResponseError, e:
                log.error("Couldn't connect to Eucalyptus EC2 because: %s" %
                                                               e.error_message)

        elif self.cloud_type == "OpenNebula":
            try:
                pass
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
            log.error("EC2Cluster don't know how to handle a %s cluster." %
                                                               self.cloud_type)

        return connection

    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  pre_customization=None, vm_keepalive=0, instance_type="",
                  maximum_price=0, job_per_core=False, securitygroup=[],
                  key_name="",use_cloud_init=False, extra_userdata=[]):
        """Attempt to boot a new VM on the cluster."""

        client = self._get_connection()
        client.run_instances(ImageId='ami', MinCount=1, MaxCount=1, InstanceType='instance_type', UserData='userdata')
        pass

    def vm_poll(self, vm):
        """Query the cloud service for information regarding a VM."""
        client = self._get_connection()
        response = client.describe_instance_status()
        if response and 'InstanceStatuses' in response.keys():
            for instance in response['InstanceStatuses']:
                if instance['InstanceId'] == vm.id:
                    if vm.status != instance['InstanceState']['Name']:
                        vm.last_state_change = int(time.time())
                    vm.status = instance['InstanceState']['Name']
                    break
                else:
                    continue
                #instance['InstanceId'] # vm id
                #instance['InstanceState']['Name'] # pending / running / terminated / etc
                pass
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
        pass



