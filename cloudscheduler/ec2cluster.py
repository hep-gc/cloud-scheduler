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
try:
    import boto.ec2
    import boto
except ImportError:
    log.error("To use EC2-style clouds, you need to have boto " \
            "installed. You can install it from your package manager, " \
            "or get it from http://code.google.com/p/boto/")

from subprocess import Popen
from urlparse import urlparse


class EC2Cluster(cluster_tools.ICluster):

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
                region = boto.ec2.regioninfo.RegionInfo(name=region_name,
                                                 endpoint=self.network_address)
                connection = boto.connect_ec2(
                                   aws_access_key_id=self.access_key_id,
                                   aws_secret_access_key=self.secret_access_key,
                                   )
                log.verbose("Created a connection to Amazon EC2")

            except boto.exception.EC2ResponseError, e:
                log.error("Couldn't connect to Amazon EC2 because: %s" %
                                                                e.error_message)

        elif self.cloud_type == "Eucalyptus":
            try:
                region = boto.ec2.regioninfo.RegionInfo(name=region_name,
                                                 endpoint=self.network_address)
                connection = boto.connect_ec2(
                                   aws_access_key_id=self.access_key_id,
                                   aws_secret_access_key=self.secret_access_key,
                                   is_secure=self.secure_connection,
                                   region=region,
                                   port=8773,
                                   path="/services/Eucalyptus",
                                   )
                log.verbose("Created a connection to Eucalyptus (%s)" % self.name)

            except boto.exception.EC2ResponseError, e:
                log.error("Couldn't connect to Eucalyptus EC2 because: %s" %
                                                               e.error_message)

        elif self.cloud_type == "OpenNebula":

            log.error("OpenNebula support isn't ready yet.")
            raise NotImplementedError

        elif self.cloud_type == "OpenStack":
            try:
                region = boto.ec2.regioninfo.RegionInfo(name=region_name,
                                                 endpoint=self.network_address)
                connection = boto.connect_ec2(
                                   aws_access_key_id=self.access_key_id,
                                   aws_secret_access_key=self.secret_access_key,
                                   is_secure=self.secure_connection,
                                   region=region,
                                   port=8773,
                                   path="/services/Cloud",
                                   )
                log.verbose("Created a connection to OpenStack (%s)" % self.name)

            except boto.exception.EC2ResponseError, e:
                log.error("Couldn't connect to OpenStack because: %s" %
                            e.error_message)
        else:
            log.error("EC2Cluster don't know how to handle a %s cluster." %
                                                               self.cloud_type)

        return connection

    def __init__(self, name="Dummy Cluster", host="localhost", cloud_type="Dummy",
                 memory=[], max_vm_mem= -1, cpu_archs=[], networks=[], vm_slots=0,
                 cpu_cores=0, storage=0,
                 access_key_id=None, secret_access_key=None, security_group=None,
                 hypervisor='xen', key_name=None, boot_timeout=None, secure_connection="",
                 regions=[], vm_domain_name="", reverse_dns_lookup=False,placement_zone=None):

        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, cpu_archs=cpu_archs, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, hypervisor=hypervisor, boot_timeout=boot_timeout)

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
        self.vm_domain_name = vm_domain_name if vm_domain_name != None else ""
        self.reverse_dns_lookup = reverse_dns_lookup in ['True', 'true', 'TRUE']
        self.placement_zone = placement_zone

    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc, vm_cpuarch,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  vm_keepalive=0, instance_type="", maximum_price=0,
                  job_per_core=False, securitygroup=[],key_name=""):
        """Attempt to boot a new VM on the cluster."""
        #print vm_image
        #print instance_type
        #print securitygroup
        log.verbose("Trying to boot %s on %s" % (vm_type, self.network_address))
        if len(securitygroup) != 0:
            sec_group = []
            for group in securitygroup:
                if group in self.security_groups:
                    sec_group.append(group)
            if len(sec_group) == 0:
                log.warning("No matching security groups - trying default")
                sec_group.append("default")
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
                    return

        try:
            if self.name in instance_type.keys():
                i_type = instance_type[self.name]
            else:
                i_type = instance_type[self.network_address]
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
        if key_name == None:
            key_name = self.key_name
        if customization:
            user_data = nimbus_xml.ws_optional(customization)
        else:
            user_data = ""

        if "AmazonEC2" == self.cloud_type and vm_networkassoc != "public":
            log.debug("You requested '%s' networking, but EC2 only supports 'public'" % vm_networkassoc)
            addressing_type = "public"
        else:
            addressing_type = vm_networkassoc

        try:
            connection = self._get_connection()
            image = None
            if not "Eucalyptus" == self.cloud_type:
                image = connection.get_image(vm_ami)

            else: #HACK: for some reason Eucalyptus won't respond properly to
                  #      get_image("whateverimg"). Use a linear search until
                  #      this is fixed
                  # This is Eucalyptus bug #495670
                  # https://bugs.launchpad.net/eucalyptus/+bug/495670
                images = connection.get_all_images()
                for potential_match in images:
                    if potential_match.id == vm_ami:
                        image = potential_match
                        break

            if image:
                if maximum_price is 0: # don't request a spot instance
                    try:
                        reservation = image.run(1,1, key_name=key_name,
                                                addressing_type=addressing_type,
                                                user_data=user_data,
                                                placement=self.placement_zone,
                                                security_groups=sec_group,
                                                instance_type=instance_type)
                        instance_id = reservation.instances[0].id
                        log.debug("Booted VM %s" % instance_id)
                    except:
                        log.exception("There was a problem creating an EC2 instance...")
                        return self.ERROR

                else: # get a spot instance of no more than maximum_price
                    try:
                        price_in_dollars = str(float(maximum_price) / 100)
                        reservation = connection.request_spot_instances(
                                                  price_in_dollars,
                                                  image.id,
                                                  key_name=key_name,
                                                  user_data=user_data,
                                                  placement=self.placement_zone,
                                                  addressing_type=addressing_type,
                                                  security_groups=self.sec_group,
                                                  instance_type=instance_type)
                        spot_id = str(reservation[0].id)
                        instance_id = ""
                        log.debug("Reserved instance %s at no more than %s" % (spot_id, price_in_dollars))
                    except AttributeError:
                        log.exception("Your version of boto doesn't seem to support "\
                                  "spot instances. You need at least 1.9")
                        return self.ERROR
                    except:
                        log.exception("Problem creating an EC2 spot instance...")
                        return self.ERROR


            else:
                log.error("Couldn't find image %s on %s" % (vm_image, self.name))
                return self.ERROR

        except:
            log.exception("Problem creating EC2 instance on on %s" % self.name)
            return self.ERROR

        vm_mementry = self.find_mementry(vm_mem)
        if (vm_mementry < 0):
            #TODO: this is kind of pointless with EC2...
            log.debug("Cluster memory list has no sufficient memory " +\
                      "entries (Not supposed to happen). Returning error.")
            return self.ERROR
        log.verbose("vm_create - Memory entry found in given cluster: %d" %
                                                                    vm_mementry)
        new_vm = cluster_tools.VM(name = vm_name, id = instance_id, vmtype = vm_type, user = vm_user,
                    clusteraddr = self.network_address,
                    cloudtype = self.cloud_type, network = vm_networkassoc,
                    cpuarch = vm_cpuarch, image= vm_image,
                    memory = vm_mem, mementry = vm_mementry,
                    cpucores = vm_cores, storage = vm_storage, 
                    keep_alive = vm_keepalive, job_per_core = job_per_core)

        try:
            new_vm.spot_id = spot_id
        except:
            log.verbose("No spot ID to add to VM %s" % instance_id)

        try:
            self.resource_checkout(new_vm)
        except:
            log.exception("Unexpected Error checking out resources when creating a VM. Programming error?")
            self.vm_destroy(new_vm, reason="Failed Resource checkout")
            return self.ERROR

        self.vms.append(new_vm)

        return 0


    def vm_poll(self, vm):
        """Query the cloud service for information regarding a VM."""
        try:
            log.verbose("Polling vm with instance id %s" % vm.id)
            connection = self._get_connection()

            if vm.spot_id:
                try:
                    spot_reservation = connection.get_all_spot_instance_requests(vm.spot_id)[0]
                    if spot_reservation.instance_id == None:
                        log.debug("Spot reservation %s doesn't have a VM id yet." % vm.spot_id)
                        return vm.status
                    vm.id = str(spot_reservation.instance_id)
                except AttributeError:
                    log.exception("Problem getting spot VM info. Do you have boto 2.0+?")
                    return vm.status
                except:
                    log.exception("Problem getting information for spot vm %s" % vm.spot_id)
                    return vm.status

            instance = None
            try:
                reservations = connection.get_all_instances([vm.id])
                instance = reservations[0].instances[0]
            except IndexError:
                log.error("%s on %s doesn't seem to exist anymore, setting status to Error" % (vm.id, self.network_address))
                vm.status = self.VM_STATES['error']
                vm.last_state_change = int(time.time())
                return vm.status
            except Exception, e:
                log.exception("Unexpected error polling %s: %s" % (vm.id, e))
                if e.status == 400 and e.error_code == 'InstanceNotFound':
                    vm.status = self.VM_STATES['error']
                return vm.status

        except boto.exception.EC2ResponseError, e:
            log.error("Couldn't update status because: %s" % e.error_message)
            return vm.status

        with self.vms_lock:
            if vm.status != self.VM_STATES.get(instance.state, "Starting"):

                vm.last_state_change = int(time.time())
                log.debug("VM: %s on %s. Changed from %s to %s." % (vm.id, self.name, vm.status, self.VM_STATES.get(instance.state, "Starting")))
            vm.status = self.VM_STATES.get(instance.state, "Starting")
            if self.reverse_dns_lookup:
                # run a dig -x on the ip address
                dig_cmd = ['dig', '-x', instance.ip_address]
                (_, dig_out, _) = self.vm_execwait(dig_cmd, env=vm.get_env())
                # extract the hostname from dig -x output
                vm.hostname = self._extract_host_from_dig(dig_out)
            elif self.cloud_type == "OpenStack":
                vm.hostname = ''.join([instance.public_dns_name, self.vm_domain_name])
            else:
                vm.hostname = instance.public_dns_name
            vm.lastpoll = int(time.time())
        return vm.status


    def vm_destroy(self, vm, return_resources=True, reason=""):
        """
        Shutdown, destroy and return resources of a VM to it's cluster

        Parameters:
        vm -- vm to shutdown and destroy
        return_resources -- if set to false, do not return resources from VM to cluster
        """
        log.info("Destroying VM: %s Name: %s Reason: %s" % (vm.id, vm.hostname, reason))

        try:
            connection = self._get_connection()

            if vm.spot_id:
                connection.cancel_spot_instance_requests([vm.spot_id])

            if vm.id:
                reservations = connection.get_all_instances([vm.id])
                instance = reservations[0].instances[0]
                instance.terminate()

        except IndexError:
            log.warning("%s already seem to be gone... removing anyway." % vm.id)
        except boto.exception.EC2ResponseError, e:
            returnError = True
            log.exception("Couldn't connect to cloud to destroy VM: %s !" % vm.id)
            if e.status == 400 and e.error_code == 'InstanceNotFound':
                log.exception("VM %s no longer exists... removing from system")
                returnError = False
            if returnError:
                return self.ERROR
        except:
            log.exception("Unexpected error destroying VM: %s!" % vm.id)

        # Delete references to this VM
        if return_resources:
            self.resource_return(vm)
        with self.vms_lock:
            self.vms.remove(vm)

        return 0

    def _extract_host_from_dig(self, dig_out):
        at_answer_line = False
        hostname = ""
        for line in dig_out.split('\n'):
            parts = line.split()
            if at_answer_line:
                hostname = parts[-1][:-1]
                break
            elif 'ANSWER' in parts and 'SECTION:' in parts:
                at_answer_line = True
                continue
        return hostname

    def vm_execwait(self, cmd, env=None):
        """As above, a function to encapsulate command execution via Popen.
        vm_execwait executes the given cmd list, waits for the process to finish,
        and returns the return code of the process. STDOUT and STDERR are stored
        in given parameters.
        Parameters:
        (cmd as above)
        Returns:
        ret - The return value of the executed command
        out - The STDOUT of the executed command
        err - The STDERR of the executed command
        The return of this function is a 3-tuple
        """
        out = ""
        err = ""
        try:
            sp = Popen(cmd, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            if not utilities.check_popen_timeout(sp):
                (out, err) = sp.communicate(input=None)
            else:
                log.warning("Process %s timed out! cmd was %" % (sp.pid, " ".join(cmd)))
            return (sp.returncode, out, err)
        except OSError, e:
            try:
                log.error("Problem running %s, got errno %d \"%s\"" % (string.join(cmd, " "), e.errno, e.strerror))
            except:
                log.error("Problem running command, OSError.")
            return (-1, "", "")
        except:
            try:
                log.error("Problem running %s, unexpected error: %s" % (string.join(cmd, " "), err))
            except:
                log.error("Problem running command, unexpected error.")
            return (-1, "", "")
