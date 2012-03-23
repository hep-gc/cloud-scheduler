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

try:
    import boto.ec2
    import boto


except ImportError:
    log.error("To use EC2-style clouds, you need to have boto " \
            "installed. You can install it from your package manager, " \
            "or get it from http://code.google.com/p/boto/")


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
            hostname="", ipaddress="", clusteraddr="",
            cloudtype="", network="public", cpuarch="x86",
            image="", memory=0, mementry=0,
            cpucores=0, storage=0, keep_alive=0, spot_id="",
            proxy_file=None, myproxy_creds_name=None, myproxy_server=None, myproxy_server_port=None, job_per_core=False):
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
        self.ipaddress = ipaddress
        self.condorname = None
        self.condoraddr = None
        self.clusteraddr = clusteraddr
        self.cloudtype = cloudtype
        self.network = network
        self.cpuarch = cpuarch
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
        log.debug("New VM object created:")
        log.debug("VM - Name: %s, id: %s, host: %s, image: %s, memory: %d" \
          % (name, id, clusteraddr, image, memory))

    def log(self):
        """Log the VM to the info level."""
        log.info("VM Name: %s, ID: %s, Type: %s, User: %s, Status: %s on %s" % (self.name, self.id, self.vmtype,  self.user, self.status, self.clusteraddr))
    def log_dbg(self):
        """Log the VM to the debug level."""
        log.debug("VM Name: %s, ID: %s, Type: %s, User: %s, Status: %s on %s" % (self.name, self.id, self.vmtype, self.user, self.status, self.clusteraddr))

    def get_vm_info(self):
        """Formatted VM information for use with cloud_status."""
        output = "%-11s %-23s %-20s %-10s %-12s\n" % (self.id[-11:], self.hostname[-23:], self.vmtype[-10:], self.user[-10:], self.status[-8:])
        if self.override_status != None:
            output = "%-11s %-23s %-20s %-10s %-12s\n" % (self.id[-11:], self.hostname[-23:], self.vmtype[-10:], self.user[-10:], self.override_status[-12:])
        return output

    @staticmethod
    def get_vm_info_header():
        """Formatted header for use with cloud_status vm info output."""
        return "%-11s %-23s %-20s %-10s %-12s %-23s\n" % ("ID", "HOSTNAME", "VMTYPE", "USER", "STATUS", "CLUSTER")

    def get_vm_info_pretty(self):
        """Header + VM info formatted output."""
        output = get_vm_info_header()
        output += get_vm_info()
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
        log.debug("needs_proxy_renewal td: %d, threshold: %d" % (td_in_seconds, config.vm_proxy_shutdown_threshold))
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
                 cloud_type="Dummy", memory=[], max_vm_mem= -1, cpu_archs=[], networks=[],
                 vm_slots=0, cpu_cores=0, storage=0, hypervisor='xen'):
        self.name = name
        self.network_address = host
        self.cloud_type = cloud_type
        self.memory = memory
        self.max_mem = tuple(memory)
        self.max_vm_mem = max_vm_mem
        self.cpu_archs = cpu_archs
        self.network_pools = networks
        self.vm_slots = vm_slots
        self.max_slots = vm_slots
        self.cpu_cores = cpu_cores
        self.storageGB = storage
        self.max_storageGB = storage
        self.vms = [] # List of running VMs
        self.vms_lock = threading.RLock()
        self.res_lock = threading.RLock()
        self.enabled = True
        self.hypervisor = hypervisor

        self.setup_logging()
        log.info("New cluster %s created" % self.name)

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
            "CPU Archs:\t%s\n"     % string.join(self.cpu_archs, ", ") +
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
        output += "%-25s  %-15s  %-10s  %-10s %-10s %-10s\n" % ("ADDRESS", "CLOUD TYPE", "VM SLOTS", "MEMORY", "STORAGE", "ENABLED")
        output += "%-25s  %-15s  %-10s  %-10s %-10s %-10s\n" % (self.network_address, self.cloud_type, self.vm_slots, self.memory, self.storageGB, self.enabled)
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
        log.debug("Checking out resources for VM %s from Cluster %s" % (vm.name, self.name))
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
        log.info("Returning resources used by VM %s to Cluster %s" % (vm.id, self.name))
        with self.res_lock:
            self.vm_slots += 1
            self.storageGB += vm.storage
            # ISSUE: No way to know what mementry a VM is running on
            try:
                self.memory[vm.mementry] += vm.memory
            except:
                log.warning("Couldn't return memory because I don't know about that mem entry anymore...")


class NimbusCluster(ICluster):
    """
    Implements cloud management functionality with the Nimbus service as part of
    the Globus Toolkit.
    """

    # Global Nimbus command variables
    VM_TARGETSTATE = "Running"
    VM_NODES = "1"

    # Number of seconds to wait between executing a shutdown and a destroy.
    # (Used in vm_destroy method)
    VM_SHUTDOWN = 8

    ERROR = 1

    # A dictionary mapping Nimbus states to global states (see VM class comments
    # for the global state information)
    # Nimbus VM states: Unstaged, Unpropagated, Propagated, Running, Paused,
    # TransportReady, StagedOut, Corrupted, Cancelled.
    VM_STATES = {
         "Unstaged"       : "Starting",
         "Unpropagated"   : "Starting",
         "Propagated"     : "Starting",
         "Running"        : "Running",
         "Paused"         : "Running",
         "TransportReady" : "Running",
         "StagedOut"      : "Running",
         "Corrupted"      : "Error",
         "Cancelled"      : "Error",
    }

    def __init__(self, name="Dummy Cluster", host="localhost", port="8443",
                 cloud_type="Dummy", memory=[], max_vm_mem= -1, cpu_archs=[], networks=[],
                 vm_slots=0, cpu_cores=0, storage=0,
                 access_key_id=None, secret_access_key=None, security_group=None,
                 netslots={}, hypervisor='xen'):

        # Call super class's init
        ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, cpu_archs=cpu_archs, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, hypervisor=hypervisor)
        # typical cluster setup uses the get_or_none - if init called with port=None default not used
        self.port = port if port != None else "8443"
        self.net_slots = netslots
        total_pool_slots = 0
        for pool in self.net_slots.keys():
            total_pool_slots += self.net_slots[pool]
        self.max_slots = total_pool_slots

    def get_cluster_info_short(self):
        """Returns formatted cluster information for use by cloud_status, Overloaded from baseclass to use net_slots."""
        output = "Cluster: %s \n" % self.name
        output += "%-25s  %-15s  %-10s  %-10s %-10s\n" % ("ADDRESS", "CLOUD TYPE", "VM SLOTS", "MEMORY", "STORAGE")
        output += "%-25s  %-15s  %-10s  %-10s %-10s\n" % (self.network_address, self.cloud_type, self.net_slots, self.memory, self.storageGB)
        return output

    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc, vm_cpuarch,
            vm_image, vm_mem, vm_cores, vm_storage, customization=None, vm_keepalive=0,
            job_proxy_file_path=None, myproxy_creds_name=None, myproxy_server=None, 
            myproxy_server_port=None, job_per_core=False, proxy_non_boot=False):
        """Attempt to boot up a new VM on the cluster."""
        def _remove_files(files):
            """Private function to clean up temporary files created during the create process."""
            for file in files:
                try:
                    if file:
                        log.debug("Deleting %s" % file)
                        os.remove(file)
                except:
                    log.exception("Couldn't delete %s" % file)

        log.debug("Nimbus cloud create command")

        if vm_networkassoc == "":
            # No network specified, so just pick the first available one
            try:
                for netpool in self.net_slots.keys():
                    if self.net_slots[netpool] > 0:
                        vm_networkassoc = netpool
                        break
                if vm_networkassoc == "":
                    vm_networkassoc = self.network_pools[0]
            except:
                log.exception("No network pool available? Aborting vm creation.")
                return self.ERROR

        # Create a workspace metadata xml file
        vm_metadata = nimbus_xml.ws_metadata_factory(vm_name, vm_networkassoc, \
                vm_cpuarch, vm_image, vm_storage > 0)

        # Create a deployment request file
        vm_deploymentrequest = nimbus_xml.ws_deployment_factory(config.vm_lifetime, \
                self.VM_TARGETSTATE, vm_mem, vm_storage, self.VM_NODES, vm_cores=vm_cores)

        job_proxy = None
        try:
            with open(job_proxy_file_path) as proxy:
                job_proxy = proxy.read()
        except:
            if job_proxy_file_path:
                log.exception("Couldn't open '%s', continuing without user's proxy" % (job_proxy_file_path))
            job_proxy = None


        if customization or job_proxy:
            image_scheme = urlparse(vm_image).scheme
            if image_scheme == "https":
                _job_proxy = job_proxy
            else:
                _job_proxy = None
            vm_optional = nimbus_xml.ws_optional_factory(custom_tasks=customization, credential=_job_proxy)
        else:
            vm_optional = None


        # Set a timestamp for VM creation
        now = datetime.datetime.now()

        # Create an EPR file name (unique with timestamp)
        (epr_handle, vm_epr) = tempfile.mkstemp(suffix=".vm_epr")
        os.close(epr_handle)

        nimbus_files = [vm_epr, vm_metadata, vm_deploymentrequest, vm_optional]

        # Create cached copy of job proxy to be used by VM for startup and shutdown.
        vm_proxy_file_path = None
        if job_proxy_file_path and not proxy_non_boot:
            try:
                vm_proxy_file_path = self._cache_proxy(job_proxy_file_path)
                log.debug("Cached proxy to '%s'" % vm_proxy_file_path)
            except:
                log.exception("Problem caching proxy.")
                _remove_files(nimbus_files)
                return -1

        # Create the workspace command as a list (private method)
        ws_cmd = self.vmcreate_factory(vm_epr, vm_metadata, vm_deploymentrequest, optional_file=vm_optional)
        

        log.debug("Command: " + string.join(ws_cmd, " "))

        # Execute the workspace create command: returns immediately.
        env = None;
        if vm_proxy_file_path != None and not proxy_non_boot:
            env = {'X509_USER_PROXY':vm_proxy_file_path}
            log.debug("VM creation environment will contain:\n\tX509_USER_PROXY = %s" % (vm_proxy_file_path))

        (create_return, create_out, create_err) = self.vm_execwait(ws_cmd, env)
        if (create_return != 0):
            if create_out == "" or create_out == None:
                create_out = "No Output returned."
            if create_err == "" or create_err == None:
                create_err = "No Error output returned."
            log.warning("Error creating VM %s: %s %s" % (vm_name, create_out, create_err))
            _remove_files(nimbus_files + [vm_proxy_file_path])
            err_type = self._extract_create_error(create_err)
            ## TODO Figure out some error codes to return then handle the codes in the scheduler vm creation code
            if err_type == 'NoProxy' or err_type == 'ExpiredProxy':
                create_return = -1
            elif err_type == 'NoSlotsInNetwork':
                with self.res_lock:
                    if vm_networkassoc in self.net_slots.keys():
                        self.net_slots[vm_networkassoc] = 0 # no slots remaining
                create_return = -2
            elif err_type =='NotEnoughMemory':
                with self.res_lock:
                    index = self.find_mementry(vm_mem)
                    self.memory[index] = vm_mem - 1 # may still be memory, but just not enough for this vm
                create_return = -2

            return create_return

        log.debug("Nimbus create command executed.")

        log.debug("Deleting temporary Nimbus Metadata files")
        _remove_files(nimbus_files)

        # Find the memory entry in the Cluster 'memory' list which _create will be
        # subtracted from
        vm_mementry = self.find_mementry(vm_mem)
        if (vm_mementry < 0):
            # At this point, there should always be a valid mementry, as the ResourcePool
            # get_resource methods have selected this cluster based on having an open
            # memory entry that fits VM requirements.
            log.error("Cluster memory list has no sufficient memory " +\
              "entries (Not supposed to happen). Returning error.")
        log.debug("Memory entry found in given cluster: %d" % vm_mementry)

        # Get the id of the VM from the output of workspace.sh
        try:
            vm_id = re.search("Workspace created: id (\d*)", create_out).group(1)
        except:
            log.error("Couldn't find workspace id for new VM")
            create_return = -3
            return create_return
        try:
            vm_ip = re.search("IP address: (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", create_out).group(1)
        except:
            log.error("Couldn't find the ip address for new VM")
            create_return = -3
            return create_return

        # Get the first part of the hostname given to the VM
        vm_hostname = self._extract_hostname(create_out)
        if vm_hostname:
            log.debug("Hostname for vm_id %s is %s" % (vm_id, vm_hostname))
        else:
            log.warning("Unable to get the VM hostname, for vm_id %s" % vm_id)


        # Create a VM object to represent the newly created VM
        new_vm = VM(name = vm_name, id = vm_id, vmtype = vm_type, user = vm_user,
            hostname = vm_hostname, ipaddress = vm_ip, 
            clusteraddr = self.network_address,
            cloudtype = self.cloud_type,network = vm_networkassoc,
            cpuarch = vm_cpuarch, image = vm_image,
            memory = vm_mem, mementry = vm_mementry, cpucores = vm_cores,
            storage = vm_storage, keep_alive = vm_keepalive, 
            proxy_file = vm_proxy_file_path, 
            myproxy_creds_name = myproxy_creds_name, myproxy_server = myproxy_server, 
            myproxy_server_port = myproxy_server_port, job_per_core = job_per_core)

        # Add the new VM object to the cluster's vms list And check out required resources
        self.vms.append(new_vm)
        try:
            self.resource_checkout(new_vm)
        except:
            log.exception("Unexpected error checking out resources when creating a VM. Programming error?")
            return self.ERROR

        log.info("Started vm %s on %s using image at %s" % (new_vm.id, new_vm.clusteraddr, new_vm.image))
        return create_return


    def vm_destroy(self, vm, return_resources=True, reason="", shutdown_first=True):
        """
        Shutdown, destroy and return resources of a VM to it's cluster

        Parameters:
        vm -- vm to shutdown and destroy
        return_resources -- if set to false, do not return resources from VM to cluster
        shutdown_first -- if set to false, will first call a shutdown before destroying
        """

        # Create an epr for workspace.sh
        vm_epr = nimbus_xml.ws_epr_factory(vm.id, vm.clusteraddr)

        if shutdown_first:
            # Create the workspace command with shutdown option
            shutdown_cmd = self.vmshutdown_factory(vm_epr)
            log.verbose("Shutting down VM with command: " + string.join(shutdown_cmd, " "))

            # Execute the workspace shutdown command.
            shutdown_return = self.vm_exec_silent(shutdown_cmd, env=vm.get_env())
            if (shutdown_return != 0):
                log.debug("(vm_destroy) - VM shutdown request failed, moving directly to destroy.")
            else:
                log.debug("(vm_destroy) - workspace shutdown command executed successfully.")
                # Sleep for a few seconds to allow for proper shutdown
                log.debug("Waiting %ss for VM to shut down..." % self.VM_SHUTDOWN)
                time.sleep(self.VM_SHUTDOWN)


        # Create the workspace command with destroy option as a list (priv.)
        destroy_cmd = self.vmdestroy_factory(vm_epr)
        log.verbose("Destroying VM with command: " + string.join(destroy_cmd, " "))

        # Execute the workspace destroy command: wait for return, stdout to log.
        (destroy_return, destroy_out, destroy_error) = self.vm_execwait(destroy_cmd, env=vm.get_env())
        destroy_out = destroy_out + destroy_error


        # Check destroy return code. If successful, continue. Otherwise, set VM to
        # error state (wait, and the polling thread will attempt a destroy later)
        if (destroy_return != 0):

            if "Destroyed" == self._extract_state(destroy_error):
                log.debug("VM %s seems to have already been destroyed." % vm.id)
            else:
                if destroy_out == "" or destroy_out == None:
                    destroy_out = "No Output returned."
                if destroy_error == "" or destroy_error == None:
                    destroy_error = "No Error output returned."
                log.warning("VM %s was not correctly destroyed: %s %s" % (vm.id, destroy_out, destroy_error))
                vm.status = "Error"
                os.remove(vm_epr)
                return destroy_return

        # Delete VM proxy
        if (vm.get_proxy_file()) :
            log.verbose("Cleaning up proxy for VM %s (%s)" % (vm.id, vm.get_proxy_file()))
            try:
                os.remove(vm.get_proxy_file())
            except:
                log.exception("Problem removing VM proxy file")

        # Return checked out resources And remove VM from the Cluster's 'vms' list
        with self.vms_lock:
            try:
                self.vms.remove(vm)
            except ValueError:
                log.error("Attempted to remove vm from list that was already removed.")
                return_resources = False
        if return_resources:
            self.resource_return(vm)

        # Delete EPR
        os.remove(vm_epr)


        log.info("Destroyed VM: %s Name: %s Reason: %s" % (vm.id, vm.hostname, reason))

        return destroy_return


    def vm_poll(self, vm):
        """
        vm_poll -- Polls a running VM, updates its status, and returns its state

        Parameters:
            vm -- vm to poll

        Note: If VM does not appear to be running any longer, it will be destroyed.
        """
        # Retire not actually bad, just don't want that state overwritten
        bad_status = ("Destroyed", "NoProxy", "ExpiredProxy")
        special_status = ("Retiring", "TempBanned", "HeldBadReqs", "HTTPFail")
        # Create an epr for our poll command
        vm_epr = nimbus_xml.ws_epr_factory(vm.id, vm.clusteraddr)

        # Create workspace poll command
        ws_cmd = self.vmpoll_factory(vm_epr)
        log.verbose("Polling Nimbus with:\n%s" % string.join(ws_cmd, " "))

        # Execute the workspace poll (wait, retrieve return code, stdout, and stderr)
        (poll_return, poll_out, poll_err) = self.vm_execwait(ws_cmd, env=vm.get_env())
        poll_out = poll_out + poll_err

        with self.vms_lock:

            # Print output, and parse the VM status from it
            #vm.hostname = self._extract_hostname(poll_out)
            new_status = self._extract_state(poll_out)
            if new_status == "Destroyed":
                self.vm_destroy(vm, shutdown_first=False, reason="Nimbus has already destroyed VM")
                vm.status = new_status

            elif new_status == "NoProxy":
                vm.override_status = new_status
                log.error("Problem polling VM %s. You don't have a valid proxy." % vm.id)

            elif new_status == "ExpiredProxy":
                vm.override_status = new_status
                log.error("Problem polling VM %s. Your proxy expired." % vm.id)

            elif new_status == "ConnectionRefused":
                vm.override_status = new_status
                log.error("Unable to connect to nimbus service on %s" % vm.clusteraddr)

            elif vm.status != new_status:
                vm.last_state_change = int(time.time())
                vm.status = new_status

            elif vm.override_status != None and new_status not in bad_status and vm.override_status not in special_status:
                vm.override_status = None
                vm.errorconnect = None

            # If there was some other error we're not aware of (temporary network problem, etc...)
            elif (poll_return != 0):
                if poll_out == "" or poll_out == None:
                    poll_out = "No Output returned."
                if poll_err == "" or poll_err == None:
                    poll_err = "No Error output returned."
                log.warning("There was a problem polling VM %s: %s %s" % (vm.id, poll_out, poll_err))

        # Tidy up and return
        os.remove(vm_epr)
        vm.lastpoll = int(time.time())
        return vm.status



    ## NimbusCluster private methods

    def vm_execwait(self, cmd, env=None):
        """As above, a function to encapsulate command execution via Popen.
        vm_execwait executes the given cmd list, waits for the process to finish,
        and returns the return code of the process. STDOUT and STDERR are stored
        in given parameters.
        Parameters:
        (cmd as above)
        Returns:
            ret   - The return value of the executed command
            out   - The STDOUT of the executed command
            err   - The STDERR of the executed command
        The return of this function is a 3-tuple
        """
        out = ""
        err = ""
        try:
            sp = Popen(cmd, executable=config.workspace_path, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            if not utilities.check_popen_timeout(sp):
                (out, err) = sp.communicate(input=None)
            else:
                log.warning("Process timed out!")
            return (sp.returncode, out, err)
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (string.join(cmd, " "), e.errno, e.strerror))
            return (-1, "", "")
        except:
            log.error("Problem running %s, unexpected error: %s" % (string.join(cmd, " "), err))
            return (-1, "", "")

    def vm_exec_silent(self, cmd, env=None):
        """
        vm_exec_silent executes a given command list, and discards the output

        parameter: cmd -- a list of a command and arguments

        returns: the return value of the command that was run
        """
        out = ""
        err = ""
        try:
            sp = Popen(cmd, executable=config.workspace_path, shell=False,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            if not utilities.check_popen_timeout(sp):
                (out, err) = sp.communicate(input=None)
            else:
                log.warning("Process timed out!")
            return sp.returncode
        except OSError, e:
            log.error("Problem running %s, got errno %d \"%s\"" % (string.join(cmd, " "), e.errno, e.strerror))
            return -1
        except:
            log.error("Problem running %s, unexpected error: %s" % (string.join(cmd, " "), err))
            return -1

    def vmcreate_factory(self, epr_file, metadata_file, request_file, optional_file=None):
        """Takes the given paraments and creates a list representing a workspace command
        used by Nimbus.
        
        Return: list
        """

        ws_list = [config.workspace_path,
           "-z", "none",
           "--poll-delay", "200",
           "--deploy",
           "--file", epr_file,
           "--metadata", metadata_file,
           "--request", request_file,
           "-s", "https://" + self.network_address + ":" + self.port + "/wsrf/services/WorkspaceFactoryService",
           "--nosubscriptions",              # Causes the command to start workspace and return immediately
          ]
        if optional_file:
            ws_list.append("--optional")
            ws_list.append(optional_file)

        # Return the workspace command list
        return ws_list

    def vmdestroy_factory(self, epr_file):
        """Create a workspace destroy command formatted list of arguments."""
        ws_list = [config.workspace_path, "-e", epr_file, "--destroy"]
        return ws_list

    def vmshutdown_factory(self, epr_file):
        """Create a workspace shutdown command formatted list of arguments."""
        ws_list = [config.workspace_path, "-e", epr_file, "--shutdown"]
        return ws_list

    def vmpoll_factory(self, epr_file):
        """Create a workspace poll(rpquery) command formatted list of arguments."""
        ws_list = [config.workspace_path, "-e", epr_file, "--rpquery"]
        return ws_list

    @staticmethod
    def _extract_hostname(create_response):
        """
        _extract_hostname -- extracts the hostname from a Nimbus create call

        returns short hostname of VM as string
        """

        try:
            matches = re.search("Hostname:\s(.*)[\.\s]", create_response)
            hostname = matches.group(1)
        except:
            return ""

        return hostname

    @staticmethod
    def _extract_state(output):
        """
        _extract_state -- extract the state from a Nimbus workspace command

        Parameters:
            output -- expects the output from a workspace.sh command
        """

        STATE_RE = "State:\s(\w*)"

        # Check if VM has a regular state
        match = re.search(STATE_RE, output)
        if match:
            status = match.group(1)
            if (status in NimbusCluster.VM_STATES):
                if status == 'Corrupted':
                    http_fail = re.search("Problem: TRANSFER FAILED Problem propagating :UnexpectedError :HTTP error Not Found", output)
                    if http_fail:
                        return "HttpError"
                return NimbusCluster.VM_STATES[status]
            else:
                return "Error"

        # Check if VM does not exist on server
        non_existant = re.search("This workspace is unknown to the service", output)
        if non_existant:
            return "Destroyed"

        # Check if you have no proxy
        no_proxy = re.search("Defective credential detected.*not found", output)
        if no_proxy:
            return "NoProxy"

        # Check if your proxy is expired
        expired_proxy = re.search("Expired credentials detected", output)
        if expired_proxy:
            return "ExpiredProxy"
        
        connect_refused = re.search("Connection refused", output)
        if connect_refused:
            return "ConnectionRefused"

        return "Error"

    @staticmethod
    def _extract_create_error(output):
        """
        _extract_create_error -- extract the state from a Nimbus workspace command
    
        Parameters:
            output -- expects the error output from a workspace.sh deploy command
        """

        # Check if you have no proxy
        no_proxy = re.search("Defective credential detected.", output)
        if no_proxy:
            return "NoProxy"

        # Check if your proxy is expired
        expired_proxy = re.search("Expired credentials detected", output)
        if expired_proxy:
            return "ExpiredProxy"

        # Check if out of network slots
        out_of_slots = re.search("Resource request denied: Error creating workspace.s.. network", output)
        if out_of_slots:
            return "NoSlotsInNetwork"

        # Check if out of memory
        out_of_memory = re.search("Resource request denied: Error creating workspace.s.. based on memory", output)
        if out_of_memory:
            return "NotEnoughMemory"

        return "Error"
    def _cache_proxy(self, proxy_file_path):
        """
        Creates a copy of the user's credential to use in case the user removes
        his Condor job early.
        Note that the location of the cached proxies is controled via the
        proxy_cache_dir config attribute.

        Raises an exception if there was a problem creating the cached proxy

        Returns a path to the cached proxy
        """
        (tmp_proxy_file, tmp_proxy_file_path) = tempfile.mkstemp(suffix='.pem', dir=config.proxy_cache_dir)
        os.close(tmp_proxy_file)

        shutil.copy2(proxy_file_path, tmp_proxy_file_path)

        return tmp_proxy_file_path

    def resource_checkout(self, vm):
        """
        Checks out resources taken by a VM in creation from the internal rep-
        resentation of the Cluster
    
        Parameters:
        vm   - the VM object used to check out resources from the Cluster.
    
        Raises NoResourcesError if there are not enough available resources
        to check out.
        """
        with self.res_lock:
            remaining_net_slots = self.net_slots[vm.network] - 1
            if remaining_net_slots < 0:
                raise NoResourcesError("net_slots: " + vm.network)
            ICluster.resource_checkout(self, vm)
            self.net_slots[vm.network] = remaining_net_slots

    def resource_return(self, vm):
        """Returns the resources taken by the passed in VM to the Cluster's internal
        storage.
        Parameters: (as for checkout() )
        Notes: (as for checkout)
        """
        with self.res_lock:
            self.net_slots[vm.network] += 1
            ICluster.resource_return(self, vm)

    def slot_fill_ratio(self):
        """Return a ratio of how 'full' the cluster is based on used slots / total slots."""
        remaining_total_slots = 0
        for pool in self.net_slots.keys():
            remaining_total_slots += self.net_slots[pool]
        return (self.max_slots - remaining_total_slots) / float(self.max_slots)

class EC2Cluster(ICluster):

    VM_STATES = {
            "running" : "Running",
            "pending" : "Starting",
            "shutting-down" : "Shutdown",
            "termimated" : "Shutdown",
            "error" : "Error",
    }

    ERROR = 1
    DEFAULT_INSTANCE_TYPE = "m1.small"

    def _get_connection(self):
        """
            _get_connection - get a boto connection object to this cluster

            returns a boto connection object, or none in the case of an error
        """

        connection = None

        if self.cloud_type == "AmazonEC2":
            try:
                region = boto.ec2.regioninfo.RegionInfo(name=self.name,
                                                 endpoint=self.network_address)
                connection = boto.connect_ec2(
                                   aws_access_key_id=self.access_key_id,
                                   aws_secret_access_key=self.secret_access_key,
                                   )
                log.debug("Created a connection to Amazon EC2")

            except boto.exception.EC2ResponseError, e:
                log.error("Couldn't connect to Amazon EC2 because: %s" %
                                                                e.error_message)

        elif self.cloud_type == "Eucalyptus":
            try:
                region = boto.ec2.regioninfo.RegionInfo(name=self.name,
                                                 endpoint=self.network_address)
                connection = boto.connect_ec2(
                                   aws_access_key_id=self.access_key_id,
                                   aws_secret_access_key=self.secret_access_key,
                                   is_secure=False,
                                   region=region,
                                   port=8773,
                                   path="/services/Eucalyptus",
                                   )
                log.debug("Created a connection to Eucalyptus (%s)" % self.name)

            except boto.exception.EC2ResponseError, e:
                log.error("Couldn't connect to Eucalyptus EC2 because: %s" %
                                                               e.error_message)

        elif self.cloud_type == "OpenNebula":

            log.error("OpenNebula support isn't ready yet.")
            raise NotImplementedError

        elif self.cloud_type == "OpenStack":
            try:
                region = boto.ec2.regioninfo.RegionInfo(name=self.name,
                                                 endpoint=self.network_address)
                connection = boto.connect_ec2(
                                   aws_access_key_id=self.access_key_id,
                                   aws_secret_access_key=self.secret_access_key,
                                   is_secure=False,
                                   region=region,
                                   port=8773,
                                   path="/services/Cloud",
                                   )
                log.debug("Created a connection to OpenStack (%s)" % self.name)

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
                 hypervisor='xen'):

        # Call super class's init
        ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, cpu_archs=cpu_archs, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, hypervisor=hypervisor)

        if not security_group:
            security_group = "default"
        self.security_groups = [security_group]

        if not access_key_id or not secret_access_key:
            log.error("Cannot connect to cluster %s "
                      "because you haven't specified an access_key_id or "
                      "a secret_access_key" % self.name)

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

        connection = self._get_connection()


    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc, vm_cpuarch,
                  vm_image, vm_mem, vm_cores, vm_storage, customization=None,
                  vm_keepalive=0, instance_type="", maximum_price=0,
                  job_per_core=False):
        """Attempt to boot a new VM on the cluster."""

        log.debug("Trying to boot %s on %s" % (vm_type, self.network_address))

        try:
            vm_ami = vm_image[self.network_address]
        except:
            log.debug("No AMI for %s, trying default" % self.network_address)
            try:
                vm_ami = vm_image["default"]
            except:
                log.exception("Can't find a suitable AMI")
                return

        try:
            i_type = instance_type[self.network_address]
        except:
            log.debug("No instance type for %s, trying default" % self.network_address)
            try:
                i_type = instance_type["default"]
            except:
                i_type = self.DEFAULT_INSTANCE_TYPE
        instance_type = i_type

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
                        reservation = image.run(1,1,
                                                addressing_type=addressing_type,
                                                user_data=user_data,
                                                security_groups=self.security_groups,
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
                                                  user_data=user_data,
                                                  addressing_type=addressing_type,
                                                  security_groups=self.security_groups,
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
        log.debug("vm_create - Memory entry found in given cluster: %d" %
                                                                    vm_mementry)
        new_vm = VM(name = vm_name, id = instance_id, vmtype = vm_type, user = vm_user,
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
            except:
                log.exception("Unexpected error polling %s" % vm.id)
                return vm.status

        except boto.exception.EC2ResponseError, e:
            log.error("Couldn't update status because: %s" % e.error_message)
            return vm.status

        with self.vms_lock:
            if vm.status != self.VM_STATES.get(instance.state, "Starting"):

                vm.last_state_change = int(time.time())
            vm.status = self.VM_STATES.get(instance.state, "Starting")
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
            log.exception("Couldn't connect to cloud to destroy VM: %s !" % vm.id)
            return self.ERROR
        except:
            log.exception("Unexpected error destroying VM: !" % vm.id)

        # Delete references to this VM
        if return_resources:
            self.resource_return(vm)
        with self.vms_lock:
            self.vms.remove(vm)

        return 0

