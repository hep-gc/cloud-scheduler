from __future__ import with_statement

import os
import re
import sys
import time
import shutil
import cluster_tools
import nimbus_xml
import config
import logging
import string
#import datetime
import tempfile
import subprocess
from subprocess import Popen
from urlparse import urlparse
import cloudscheduler.utilities as utilities
from cloudscheduler.utilities import get_cert_expiry_time

log = utilities.get_cloudscheduler_logger()

class NimbusCluster(cluster_tools.ICluster):
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
         "Unstaged"       : "Unstaged",
         "Unpropagated"   : "Unpropagated",
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
                 vm_slots=0, cpu_cores=0, storage=0, max_vm_storage=-1,
                 access_key_id=None, secret_access_key=None, security_group=None,
                 netslots={}, hypervisor='xen', vm_lifetime=config.vm_lifetime,
                 image_attach_device=config.image_attach_device,
                 scratch_attach_device=config.scratch_attach_device, boot_timeout=None, total_cpu_cores=-1,
                 temp_lease_storage=False, enabled=True, priority=0):

        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, hypervisor=hypervisor, boot_timeout= boot_timeout, enabled=enabled, priority=priority)
        # typical cluster setup uses the get_or_none - if init called with port=None default not used
        self.port = port if port != None else "8443"
        self.net_slots = netslots
        self.cpu_archs = cpu_archs
        total_pool_slots = 0
        for pool in self.net_slots.keys():
            total_pool_slots += self.net_slots[pool]
        self.max_slots = total_pool_slots
        self.max_vm_storage = max_vm_storage
        self.total_cpu_cores = total_cpu_cores
        self.vm_lifetime = int(vm_lifetime) if vm_lifetime != None else config.vm_lifetime
        self.scratch_attach_device = scratch_attach_device if scratch_attach_device != None else config.scratch_attach_device
        self.image_attach_device = image_attach_device if image_attach_device != None else config.image_attach_device
        self.temp_lease_storage = temp_lease_storage if temp_lease_storage != None else False

    def get_cluster_info_short(self):
        """Returns formatted cluster information for use by cloud_status, Overloaded from baseclass to use net_slots."""
        output = "Cluster: %s \n" % self.name
        if self.total_cpu_cores == -1:
            output += "%-25s  %-15s  %-10s  %-10s %-10s %-10s %-10s\n" % ("ADDRESS", "CLOUD TYPE", "VM SLOTS", "MEMORY", "STORAGE", "HYPERVISOR", "ENABLED")
            output += "%-25s  %-15s  %-10s  %-10s %-10s %-10s %-10s\n" % (self.network_address, self.cloud_type, self.net_slots, self.memory, self.storageGB, self.hypervisor, self.enabled)
        else:
            output += "%-25s  %-15s  %-10s  %-10s %-10s %-10s %-10s %-10s\n" % ("ADDRESS", "CLOUD TYPE", "VM SLOTS", "MEMORY", "STORAGE", "CPU_CORES", "HYPERVISOR", "ENABLED")
            output += "%-25s  %-15s  %-10s  %-10s %-10s %-10s %-10s %-10s\n" % (self.network_address, self.cloud_type, self.net_slots, self.memory, self.storageGB, self.total_cpu_cores, self.hypervisor, self.enabled)
        return output

    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc, vm_cpuarch,
            vm_image, vm_mem, vm_cores, vm_storage, customization=None, vm_keepalive=0,
            job_proxy_file_path=None, myproxy_creds_name=None, myproxy_server=None, 
            myproxy_server_port=None, job_per_core=False, proxy_non_boot=False,
            vmimage_proxy_file=None, vmimage_proxy_file_path=None):
        """Attempt to boot up a new VM on the cluster."""
        def _remove_files(files):
            """Private function to clean up temporary files created during the create process."""
            for file in files:
                try:
                    if file:
                        log.verbose("Deleting %s" % file)
                        os.remove(file)
                except:
                    log.exception("Couldn't delete %s" % file)

        log.verbose("Nimbus cloud create command")

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
        if not self.temp_lease_storage:
            vm_metadata = nimbus_xml.ws_metadata_factory(vm_name, vm_networkassoc, \
                self.cpu_archs[0], vm_image, vm_storage > 0, self.image_attach_device,
                self.scratch_attach_device,)
        else:
            vm_metadata = nimbus_xml.ws_metadata_factory(vm_name, vm_networkassoc, \
                self.cpu_archs[0], vm_image, False, self.image_attach_device,
                self.scratch_attach_device,)


        # Create a deployment request file
        if not self.temp_lease_storage:
            vm_deploymentrequest = nimbus_xml.ws_deployment_factory(vm_duration = self.vm_lifetime, \
                vm_targetstate = self.VM_TARGETSTATE, vm_mem = vm_mem, vm_storage = vm_storage, vm_nodes = self.VM_NODES, vm_cores=vm_cores)
        else:
            vm_deploymentrequest = nimbus_xml.ws_deployment_factory(vm_duration = self.vm_lifetime, \
                vm_targetstate = self.VM_TARGETSTATE, vm_mem = vm_mem, vm_storage = None, vm_nodes = self.VM_NODES, vm_cores=vm_cores)

        job_proxy = None
        try:
            with open(job_proxy_file_path) as proxy:
                job_proxy = proxy.read()
        except:
            if job_proxy_file_path:
                log.exception("Couldn't open '%s', Backing out of VM Creation." % (job_proxy_file_path))
                return -1 # Temp Ban job

        if customization or job_proxy or vmimage_proxy_file:
            image_scheme = urlparse(vm_image).scheme
            if image_scheme == "https":
                if vmimage_proxy_file:
                    try:
                        with open(vmimage_proxy_file_path) as proxy:
                            vmimage_proxy = proxy.read()
                    except:
                        if vmimage_proxy_file:
                            log.exception("Couldn't open '%s' path for %s, Backing out of VM Creation." % (vmimage_proxy_file_path, vmimage_proxy_file))
                            return -1 # Temp Ban job
                    _job_proxy = vmimage_proxy
                else:
                    _job_proxy = job_proxy
            else:
                _job_proxy = None
            vm_optional = nimbus_xml.ws_optional_factory(custom_tasks=customization, credential=_job_proxy)
        else:
            vm_optional = None


        # Set a timestamp for VM creation
        #now = datetime.datetime.now()

        # Create an EPR file name (unique with timestamp)
        (epr_handle, vm_epr) = tempfile.mkstemp(suffix=".vm_epr")
        os.close(epr_handle)

        nimbus_files = [vm_epr, vm_metadata, vm_deploymentrequest, vm_optional]

        # Create cached copy of job proxy to be used by VM for startup and shutdown.
        vm_proxy_file_path = None
        if job_proxy_file_path and not proxy_non_boot:
            try:
                vm_proxy_file_path = self._cache_proxy(job_proxy_file_path)
                log.verbose("Cached proxy to '%s'" % vm_proxy_file_path)
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
            log.warning("Error creating VM %s: %s %s %s" % (vm_name, create_out, create_err, create_return))
            _remove_files(nimbus_files + [vm_proxy_file_path])
            err_type = self._extract_create_error(create_err)
            ## TODO Figure out some error codes to return then handle the codes in the scheduler vm creation code
            if err_type == 'NoProxy' or err_type == 'ExpiredProxy':
                create_return = -1
            elif err_type == 'NoSlotsInNetwork' and config.adjust_insufficient_resources:
                with self.res_lock:
                    if vm_networkassoc in self.net_slots.keys():
                        self.vm_slots -= self.net_slots[vm_networkassoc]
                        self.net_slots[vm_networkassoc] = 0 # no slots remaining
                create_return = -2
            elif err_type =='NotEnoughMemory' and config.adjust_insufficient_resources:
                with self.res_lock:
                    index = self.find_mementry(vm_mem)
                    self.memory[index] = vm_mem - 1 # may still be memory, but just not enough for this vm
                create_return = -2
            elif err_type == 'ExceedMaximumWorkspaces' or err_type == 'NotAuthorized':
                create_return = -3

            return create_return

        log.verbose("Nimbus create command executed.")

        log.verbose("Deleting temporary Nimbus Metadata files")
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
        log.verbose("Memory entry found in given cluster: %d" % vm_mementry)

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
            log.verbose("Hostname for vm_id %s is %s" % (vm_id, vm_hostname))
        else:
            log.warning("Unable to get the VM hostname, for vm_id %s" % vm_id)


        # Create a VM object to represent the newly created VM
        new_vm = cluster_tools.VM(name = vm_name, id = vm_id, vmtype = vm_type, user = vm_user,
            hostname = vm_hostname, ipaddress = vm_ip, 
            clusteraddr = self.network_address, clusterport = self.port,
            cloudtype = self.cloud_type,network = vm_networkassoc,
            cpuarch = vm_cpuarch, image = vm_image,
            memory = vm_mem, mementry = vm_mementry, cpucores = vm_cores,
            storage = vm_storage, keep_alive = vm_keepalive, 
            proxy_file = vm_proxy_file_path, 
            myproxy_creds_name = myproxy_creds_name, myproxy_server = myproxy_server, 
            myproxy_server_port = myproxy_server_port, job_per_core = job_per_core)

        # Add the new VM object to the cluster's vms list And check out required resources
        try:
            self.resource_checkout(new_vm)
        except:
            log.exception("Unexpected error checking out resources when creating a VM. Programming error?")
            return self.ERROR
        self.vms.append(new_vm)
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
        vm_epr = nimbus_xml.ws_epr_factory(vm.id, vm.clusteraddr, vm.clusterport)
        if vm.clusteraddr != self.network_address:
            log.error("Attempting to destroy a VM on wrong cluster - vm belongs to %s, but this is %s. Abort" % (vm.clusteraddr, self.networ_address))
            return -1

        if shutdown_first:
            # Create the workspace command with shutdown option
            shutdown_cmd = self.vmshutdown_factory(vm_epr)
            log.verbose("Shutting down VM with command: " + string.join(shutdown_cmd, " "))

            # Execute the workspace shutdown command.
            shutdown_return = self.vm_exec_silent(shutdown_cmd, env=vm.get_env())
            if (shutdown_return != 0):
                log.debug("(vm_destroy) - VM shutdown request failed, moving directly to destroy.")
            else:
                log.verbose("(vm_destroy) - workspace shutdown command executed successfully.")
                # Sleep for a few seconds to allow for proper shutdown
                log.verbose("Waiting %ss for VM to shut down..." % self.VM_SHUTDOWN)
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
                log.warning("VM %s was not correctly destroyed: %s %s %s" % (vm.id, destroy_out, destroy_error, destroy_return))
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
        special_status = ("Retiring", "TempBanned", "HeldBadReqs", "HTTPFail, BrokenPipe")
        # Create an epr for our poll command
        vm_epr = nimbus_xml.ws_epr_factory(vm.id, vm.clusteraddr, vm.clusterport)

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
                log.error("Problem polling VM %s. Your proxy expired. Proxy File: %s" % (vm.id, vm.proxy_file))

            elif new_status == "ConnectionRefused":
                vm.override_status = new_status
                log.error("Unable to connect to nimbus service on %s" % vm.clusteraddr)

            elif new_status == "BrokenPipe":
                vm.override_status = new_status
                log.error("Broken Pipe error on %s. Check max_clients in libvirtd.conf on nodes." % vm.clusteraddr)

            elif vm.status != new_status:
                vm.last_state_change = int(time.time())
                log.debug("VM: %s on %s. Changed from %s to %s." % (vm.id, self.name, vm.status, new_status))
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
                log.warning("There was a problem polling VM %s: %s %s %s" % (vm.id, poll_out, poll_err, poll_return))

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
                log.warning("Process %s timed out! cmd was %" % (sp.pid, " ".join(cmd)))
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
                log.warning("Process %s timed out! cmd was %" % (sp.pid, " ".join(cmd)))
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
                    broken_pipe = re.search("Problem with connection to the VMM: cannot send data: Broken pipe", output)
                    if broken_pipe:
                        return "BrokenPipe"
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

        not_authorized = re.search("not authorized to use operation", output)
        if not_authorized:
            return "NotAuthorized"

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
        out_of_slots = re.search("Resource request denied: Error creating workspace.s..+network", output)
        if out_of_slots:
            return "NoSlotsInNetwork"

        # Check if out of memory
        out_of_memory = re.search("Resource request denied: Error creating workspace.s..+based on memory", output)
        if out_of_memory:
            return "NotEnoughMemory"
        
        # Check if exceeded maximum allowed VMs
        exceed = re.search("Denied: Request for 1 workspaces, together with number of currently..concurrently running workspaces.", output)
        if exceed:
            return "ExceedMaximumWorkspaces"

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
                raise NoResourcesError("net_slots: %s" % vm.network)
            if self.total_cpu_cores != -1:
                remaining_cores = self.total_cpu_cores - vm.cpucores
                if remaining_cores < 0:
                    raise NoResourcesError("Not Enough Cores to allocate %i" % vm.cpucores)
            cluster_tools.ICluster.resource_checkout(self, vm)
            self.net_slots[vm.network] = remaining_net_slots
            if self.total_cpu_cores != -1:
                self.total_cpu_cores = remaining_cores

    def resource_return(self, vm):
        """Returns the resources taken by the passed in VM to the Cluster's internal
        storage.
        Parameters: (as for checkout() )
        Notes: (as for checkout)
        """
        with self.res_lock:
            self.net_slots[vm.network] += 1
            if self.total_cpu_cores != -1:
                self.total_cpu_cores += vm.cpucores
            cluster_tools.ICluster.resource_return(self, vm)

    def slot_fill_ratio(self):
        """Return a ratio of how 'full' the cluster is based on used slots / total slots."""
        remaining_total_slots = 0
        for pool in self.net_slots.keys():
            remaining_total_slots += self.net_slots[pool]
        return (self.max_slots - remaining_total_slots) / float(self.max_slots)

