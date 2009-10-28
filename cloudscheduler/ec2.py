#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

# Copyright (C) 2009 University of Victoria
# You may distribute under the terms of either the GNU General Public
# License or the Apache v2 License, as specified in the README file.

from cloud_management import Cluster
import logging

try:
    import boto.ec2
except ImportError:
    log.error("To use EC2-style clouds, you need to have boto installed. "
              "You can install it from your package manager, or get it from "
              "http://code.google.com/p/boto/")

log = logging.getLogger("CloudLogger")

class EC2Cluster(Cluster):
    

    def __init__(self, name="Dummy Cluster", host="localhost", type="Dummy",
                 memory=[], cpu_archs=[], networks=[], vm_slots=0,
                 cpu_cores=0, storage=0,
                 access_key_id=None, secret_access_key=None):

        # Call super class's init
        Cluster.__init__(self,name=name, host=host, type=type, memory=memory,
                         cpu_archs=cpu_archs, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage,)

        if not access_key_id or not secret_access_key:
            log.error("Cannot connect to cluster %s "
                      "because you haven't specified an access_key_id or "
                      "a secret_access_key" % self.name)
            #TODO: Handle this better

        self.access_key_id =  access_key_id
        self.secret_access_key = secret_access_key

        if self.type == "Eucalyptus":
            region = boto.ec2.regioninfo.RegionInfo(name=self.name,
                                                 endpoint=self.network_address)
            self.connection = boto.connect_ec2(
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  is_secure=False,
                                  region=region,
                                  port=8773,
                                  path="/services/Eucalyptus")

        elif self.type == "OpenNebula":

            log.error("OpenNebula support isn't ready yet.")
            raise NotImplementedError
        else:
            log.error("EC2Cluster don't know how to handle a %s cluster." %
                      self.type)

    def vm_create(self, vm_name, vm_networkassoc, vm_cpuarch, vm_imagelocation, vm_mem, vm_scratchSpace):
        raise NotImplementedError

    def vm_poll(self, vm):
        raise NotImplementedError

        
    def vm_destroy(self, vm):
        raise NotImplementedError
