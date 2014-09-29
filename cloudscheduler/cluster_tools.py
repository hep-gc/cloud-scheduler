#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

# Copyright (C) 2009 University of Victoria
# You may distribute under the terms of either the GNU General Public
# License or the Apache v2 License, as specified in the README file.


##
## This file contains the VM class, ICluster interface, as well as the
## implementations of this interface.

from __future__ import with_statement

import os
import re
import sys
import time
import string
import shutil
import logging
import datetime
import tempfile
import subprocess
import threading

from subprocess import Popen
from urlparse import urlparse

import nimbus_xml
import config
import cloudscheduler.utilities as utilities
from cloudscheduler.utilities import get_cert_expiry_time

log = utilities.get_cloudscheduler_logger()


class VM:
    """
    A class for storing created VM information. Used to populate Cluster classes
    'vms' lists.

    Instance Variables

    The global VM states are:
       Starting - The VM is being created in the cloud
       Running  - The VM is running somewhere on the cloud (fully functional)
       Error    - The VM has been corrupted or is in the process of being destroyed

    States are defined in each Cluster subclass, in which a VM_STATES dictionary
    maps specific cloud software state to these global states.
    """

    def __init__(self, name="", id="", vmtype="", user="",
            hostname="", ipaddress="", clusteraddr="", clusterport="",
            cloudtype="", network="public", cpuarch="x86",
            image="", memory=0, mementry=0,
            cpucores=0, storage=0, keep_alive=0, spot_id="",
            proxy_file=None, myproxy_creds_name=None, myproxy_server=None, myproxy_server_port=None, 
            myproxy_renew_time="12", job_per_core=False):
        """
        Constructor

        name         - (str) The name of the vm (arbitrary)
        id           - (str) The id tag for the VM. Whatever is used to access the vm
                       by cloud software (Nimbus: epr file. OpenNebula: id number, etc.)
        vmtype       - (str) The condor VMType attribute for the VM
        user         - (str) The user who 'owns' this VM
        uservmtype   - (str) Aggregate type in form 'user:vmtype'
        hostname     - (str) The first part of hostname given to VM
        ipaddress    - (str) The IP Address of the VM
        condorname   - (str) The name of the VM as it's registered with Condor
        condoraddr   - (str) The Address of the VM as it's registered with Condor
        clusteraddr  - (str) The address of the cluster hosting the VM
        clusterport  - (str) The port of the cluster hosting the VM
        cloudtype    - (str) The cloud type of the VM (Nimbus, OpenNebula, etc)
        network      - (str) The network association the VM uses
        cpuarch      - (str) The required CPU architecture of the VM
        image        - (str) The location of the image from which the VM was created
        memory       - (int) The memory used by the VM
        mementry     - (int) The index of the entry in the host cluster's memory list
                       from which this VM is taking memory
        proxy_file   - the proxy that was used to authenticate this VM's creation
        myproxy_creds_name - (str) The name of the credentials to retreive from the myproxy server
        myproxy_server - (str) The hostname of the myproxy server to retreive user creds from
        myproxy_server_port - (str) The port of the myproxy server to retreive user creds from
        errorcount   - (int) Number of Polling Errors VM has had
        force_retire - (bool) Flag to prevent a retiring VM from being turned back on
        """
        self.name = name
        self.id = id
        self.vmtype = vmtype
        self.user = user
        self.uservmtype = ':'.join([user,vmtype])
        self.hostname = hostname
        self.alt_hostname = None
        self.ipaddress = ipaddress
        self.condorname = None
        self.condoraddr = None
        self.condormasteraddr = None
        self.clusteraddr = clusteraddr
        self.clusterport = clusterport
        self.cloudtype = cloudtype
        self.network = network
        self.image = image
        self.memory = memory
        self.mementry = mementry
        self.cpucores = cpucores
        self.storage = storage
        self.errorcount = 0
        self.errorconnect = None
        self.lastpoll = None
        self.last_state_change = None
        self.initialize_time = int(time.time())
        self.startup_time = None
        self.keep_alive = keep_alive
        self.idle_start = None
        self.spot_id = spot_id
        self.proxy_file = proxy_file
        self.myproxy_creds_name = myproxy_creds_name
        self.myproxy_server = myproxy_server
        self.myproxy_server_port = myproxy_server_port
        self.myproxy_renew_time = myproxy_renew_time
        self.override_status = None
        self.job_per_core = job_per_core
        self.force_retire = False
        self.failed_retire = False
        self.job_run_times = utilities.JobRunTrackQueue('Run_Times')
        self.x509userproxy_expiry_time = None
        
        # Set a status variable on new creation
        self.status = "Starting"

        global log
        log = logging.getLogger("cloudscheduler")
        log.verbose("New VM Object - Name: %s, id: %s, host: %s, image: %s, memory: %d" \
          % (name, id, clusteraddr, image, memory))
        log.info("Created VM cluster address %s name %s"%(clusteraddr,name))

    def log(self):
        """Log the VM to the info level."""
        log.info("VM Name: %s, ID: %s, Type: %s, User: %s, Status: %s on %s" % (self.name, self.id, self.vmtype,  self.user, self.status, self.clusteraddr))
    def log_dbg(self):
        """Log the VM to the debug level."""
        log.debug("VM Name: %s, ID: %s, Type: %s, User: %s, Status: %s on %s" % (self.name, self.id, self.vmtype, self.user, self.status, self.clusteraddr))

    def get_vm_info(self):
        """Formatted VM information for use with cloud_status."""
        nameout = self.hostname
        if self.hostname == "":
            nameout = self.ipaddress
        output = "%-6s %-25s %-20s %-10s %-12s\n" % (self.id, nameout, self.vmtype, self.user, self.status)
        if self.override_status != None:
            output = "%-6s %-25s %-20s %-10s %-12s\n" % (self.id, nameout, self.vmtype, self.user, self.override_status)
        return output

    @staticmethod
    def get_vm_info_header():
        """Formatted header for use with cloud_status vm info output."""
        return "%-6s %-25s %-20s %-10s %-12s %-23s\n" % ("ID", "HOSTNAME", "VMTYPE", "USER", "STATUS", "CLUSTER")

    def get_vm_info_pretty(self):
        """Header + VM info formatted output."""
        output = self.get_vm_info_header()
        output += self.get_vm_info()
        return output

    def get_proxy_file(self):
        """Return the proxy file associated with the VM or None."""
        if hasattr(self, "proxy_file"):
            return self.proxy_file
        else:
            return None

    def get_myproxy_creds_name(self):
        """Return the MyProxy credentials name associated with the VM or None."""
        if hasattr(self, "myproxy_creds_name"):
            return self.myproxy_creds_name
        else:
            return None

    def get_myproxy_server(self):
        """Return the MyProxy server associated with the VM or None."""
        if hasattr(self, "myproxy_server"):
            return self.myproxy_server
        else:
            return None

    def get_myproxy_server_port(self):
        """Return the MyProxy server port associated with the VM or None."""
        if hasattr(self, "myproxy_server_port"):
            return self.myproxy_server_port
        else:
            return None

    def get_renew_time(self):
        """Return the MyProxy proxy renewal time associated with the VM."""
        return self.myproxy_renew_time

    def get_x509userproxy_expiry_time(self):
        """Use this method to get the expiry time of the VM's user proxy, if any.
        Note that lazy initialization is done;  the expiry time will be extracted
        from the user proxy the first time the method is called and then it will
        be cached in the instance variable.

        Returns the expiry time as a datetime.datetime instance (UTC), or None
        if there is no user proxy associated with this VM.
        """
        if (self.x509userproxy_expiry_time == None) and (self.get_proxy_file() != None):
            self.x509userproxy_expiry_time = get_cert_expiry_time(self.get_proxy_file())
        return self.x509userproxy_expiry_time

    def reset_x509userproxy_expiry_time(self):
        """Use this method to trigger an update of the proxy expiry time next
        time it is checked. For example, this must be called right after the
        proxy has been renewed. See get_x509userproxy_expiry_time for more info
        about how the proxy expiry time is cached in memory.
        """
        self.x509userproxy_expiry_time = None


    def is_proxy_expired(self):
        """Test if a VM's user proxy is expired.

        Returns True if the proxy is expired, False otherwise.
        """
        expiry_time = self.get_x509userproxy_expiry_time()
        if expiry_time == None:
            return False
        return expiry_time <= datetime.datetime.utcnow()


    def needs_proxy_renewal(self):
        """Test if a VM's user proxy needs to be refreshed, according
        the VM proxy refresh threshold found in the cloud scheduler configuration.

        Returns: True if the proxy needs to be refreshed, or 
                 False otherwise (or if the VM has no user proxy associated with it).
        """
        expiry_time = self.get_x509userproxy_expiry_time()
        if expiry_time == None:
            return False
        td = expiry_time - datetime.datetime.utcnow()
        td_in_seconds = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
        log.verbose("needs_proxy_renewal td: %d, threshold: %d" % (td_in_seconds, config.vm_proxy_renewal_threshold))
        return td_in_seconds < config.vm_proxy_renewal_threshold

    def needs_proxy_shutdown(self):
        """This method will test if a VM needs to be shutdown before proxy expiry, according
        the VM proxy shutdown threshold found in the cloud scheduler configuration.

        Returns: True if the VM needs to be shutdown
                 False otherwise (or if the VM has no user proxy associated with it).
        """
        expiry_time = self.get_x509userproxy_expiry_time()
        if expiry_time == None:
            return False
        td = expiry_time - datetime.datetime.utcnow()
        td_in_seconds = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
        log.verbose("needs_proxy_renewal td: %d, threshold: %d" % (td_in_seconds, config.vm_proxy_shutdown_threshold))
        return td_in_seconds < config.vm_proxy_shutdown_threshold

    def get_env(self):
        """The following method will return the environment that should
        be used when executing subprocesses.  This is needed for setting
        the user's x509 proxy for example.
        """
        env = None
        if self.get_proxy_file() != None:
            env = {'X509_USER_PROXY':self.get_proxy_file()}
        return env

class NoResourcesError(Exception):
    """Exception raised for errors where not enough resources are available

    Attributes:
        resource -- name of resource that is insufficient

    """

    def __init__(self, resource):
        self.resource = resource


class ICluster:
    """
    The ICluster interface is the framework for implementing support for
    a specific IaaS cloud implementation. In general, you'll need to
    override __init__ (be sure to call super's init), vm_create, vm_poll,
    and vm_destroy
    """

    def __init__(self, name="Dummy Cluster", host="localhost",
                 cloud_type="Dummy", memory=[], max_vm_mem= -1, networks=[],
                 vm_slots=0, cpu_cores=0, storage=0, hypervisor='xen', boot_timeout=None, enabled=True, priority=0):
        self.name = name
        self.network_address = host
        self.cloud_type = cloud_type
        self.memory = memory
        self.max_mem = tuple(memory)
        self.max_vm_mem = max_vm_mem
        self.network_pools = networks
        self.vm_slots = vm_slots
        self.max_slots = vm_slots
        self.cpu_cores = cpu_cores
        self.storageGB = storage
        self.max_storageGB = storage
        self.vms = [] # List of running VMs
        self.vms_lock = threading.RLock()
        self.res_lock = threading.RLock()
        self.enabled = enabled
        self.hypervisor = hypervisor
        self.boot_timeout = int(boot_timeout) if boot_timeout != None else config.vm_start_running_timeout
        self.connection_fail_disable_time = config.connection_fail_disable_time
        self.connection_problem = False
        self.errorconnect = None
        self.priority = priority

        self.setup_logging()
        log.debug("New cluster %s created" % self.name)

    def __getstate__(self):
        """Override to work with pickle module."""
        state = self.__dict__.copy()
        del state['vms_lock']
        del state['res_lock']
        return state

    def __setstate__(self, state):
        """Override to work with pickle module."""
        self.__dict__ = state
        self.vms_lock = threading.RLock()
        self.res_lock = threading.RLock()

    def __repr__(self):
        return self.name

    def setup_logging(self):
        """Fetch the global log object."""
        global log
        log = logging.getLogger("cloudscheduler")


    def log_cluster(self):
        """Print cluster information to the log."""
        log.info("-" * 30 +
            "Name:\t\t%s\n"        % self.name +
            "Address:\t%s\n"       % self.network_address +
            "Type:\t\t%s\n"        % self.cloud_type +
            "VM Slots:\t%s\n"      % self.vm_slots +
            "CPU Cores:\t%s\n"     % self.cpu_cores +
            "Storage:\t%s\n"       % self.storageGB +
            "Memory:\t\t%s\n"      % self.memory +
            "Network Pools:\t%s\n" % string.join(self.network_pools, ", ") +
            "-" * 30)

    def log(self):
        """Print a short form of cluster information to the log."""
        log.debug("CLUSTER Name: %s, Address: %s, Type: %s, VM slots: %d, Mem: %s" \
          % (self.name, self.network_address, self.cloud_type, self.vm_slots, \
          self.memory))

    def log_vms(self):
        """Print the cluster 'vms' list (via VM print)."""
        if len(self.vms) == 0:
            log.info("CLUSTER %s has no running VMs..." % (self.name))
        else:
            log.info("CLUSTER %s running VMs:" % (self.name))
            for vm in self.vms:
                vm.log_short("\t")


    ## Support methods

    def num_vms(self):
        """Returns the number of VMs running on the cluster (in accordance
        to the vms[] list)
        """
        return len(self.vms)

    def slot_fill_ratio(self):
        """Return a ratio of how 'full' the cluster is based on used slots / total slots."""
        return (self.max_slots - self.vm_slots) / float(self.max_slots)

    def get_cluster_info_short(self):
        """Return a short form of cluster information."""
        output = "Cluster: %s \n" % self.name
        output += "%-25s  %-15s  %-10s  %-10s %-10s %-10s %-10s\n" % ("ADDRESS", "CLOUD TYPE", "VM SLOTS", "MEMORY", "STORAGE", "HYPERVISOR", "ENABLED")
        if self.cloud_type == 'Nimbus':
            output += "%-25s  %-15s  %-10s  %-10s %-10s %-10s %-10s\n" % (self.network_address, self.cloud_type, self.vm_slots, self.memory, self.storageGB, self.hypervisor, self.enabled)
        else:
            output += "%-25s  %-15s  %-10s  %-10s %-10s %-10s %-10s\n" % (self.network_address, self.cloud_type, self.vm_slots, self.memory, self.storageGB, " ", self.enabled)
        return output

    def get_cluster_vms_info(self):
        """Return information about running VMs on Cluster as a string."""
        if len(self.vms) == 0:
            return ""
        else:
            output = ""
            for vm in self.vms:
                output += "%s %-15s\n" % (vm.get_vm_info()[:-1], self.name)
            return output

    def get_vm(self, vm_id):
        """Get VM object with id value."""
        for vm in self.vms:
            if vm_id == vm.id:
                return vm
        return None


    # VM manipulation methods
    #-!------------------------------------------------------------------------
    # NOTE: In implementing subclasses of Cluster, the following method prototypes
    #       should be used (standardize on these parameters)
    #-!------------------------------------------------------------------------

    # Note: vm_id is the identifier for a VM, used to query or change an already
    #       created VM. vm_id will be a different entity based on the subclass's
    #       cloud software. EG:
    #       - Nimbus vm_ids are epr files
    #       - OpenNebula (and Eucalyptus?) vm_ids are names/numbers

    def vm_create(self, **args):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_create'

    def vm_destroy(self, vm, return_resources=True, reason=""):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_destroy'

    def vm_poll(self, vm):
        log.debug('This method should be defined by all subclasses of Cluster\n')
        assert 0, 'Must define workspace_poll'


    ## Private VM methods

    def find_mementry(self, memory):
        """Finds a memory entry in the Cluster's 'memory' list which supports the
        requested amount of memory for the VM. If multiple memory entries fit
        the request, returns the first suitable entry. Returns an exact fit if
        one exists.
        Parameters: memory - the memory required for VM creation
        Return: The index of the first fitting entry in the Cluster's 'memory'
        list.
        If no fitting memory entries are found, returns -1 (error!)
        """
        # Check for exact fit
        if (memory in self.memory):
            return self.memory.index(memory)

        # Scan for any fit
        for i in range(len(self.memory)):
            if self.memory[i] >= memory:
                return i

        # If no entries found, return error code.
        return(-1)

    def find_potential_mementry(self, memory):
        """Check if a cluster contains a memory entry with adequate space for given memory value.
        Returns: True if a valid memory entry is found
                 False otherwise
        """
        potential_fit = False
        for i in range(len(self.max_mem)):
            if self.max_mem[i] >= memory:
                potential_fit = True
                break
        return potential_fit

    def resource_checkout(self, vm):
        """
        Checks out resources taken by a VM in creation from the internal rep-
        resentation of the Cluster

        Parameters:
        vm   - the VM object used to check out resources from the Cluster.

        Raises NoResourcesError if there are not enough available resources
        to check out.
        """
        #log.debug("Checking out resources for VM %s from Cluster %s" % (vm.name, self.name))
        with self.res_lock:

            remaining_vm_slots = self.vm_slots - 1
            if remaining_vm_slots < 0:
                raise NoResourcesError("vm_slots")

            remaining_storage = self.storageGB - vm.storage
            if remaining_storage < 0:
                raise NoResourcesError("storage")

            remaining_memory = self.memory[vm.mementry] - vm.memory
            if remaining_memory < 0:
                raise NoResourcesError("memory")

            # Otherwise, we can check out these resources
            self.vm_slots = remaining_vm_slots
            self.storageGB = remaining_storage
            self.memory[vm.mementry] = remaining_memory

    def resource_return(self, vm):
        """Returns the resources taken by the passed in VM to the Cluster's internal
        storage.
        Parameters: (as for checkout() )
        Notes: (as for checkout)
        """
        #log.debug("Returning resources used by VM %s to Cluster %s" % (vm.name, self.name))
        with self.res_lock:
            self.vm_slots += 1
            self.storageGB += vm.storage
            # ISSUE: No way to know what mementry a VM is running on
            try:
                self.memory[vm.mementry] += vm.memory
            except:
                log.warning("Couldn't return memory because I don't know about that mem entry anymore...")

