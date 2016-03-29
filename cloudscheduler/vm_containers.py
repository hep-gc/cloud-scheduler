from abc import ABCMeta, abstractmethod
from collections import defaultdict
import time
import threading
import logging
import cloudscheduler.config as config

# Use this global variable for logging.
log = None

#
# This is an abstract base class; do not instantiate directly.
#
# API documentation should go in here, as opposed to writing specific
# documentation for each concrete subclasses.
#
class VMContainer():
    __metclass__ = ABCMeta

    # Use this lock if you require to threadsafe an operation.
    lock = None
    ## VM States
    vm_status_list = ['Starting', 'Running', 'Retiring', 'Error']
    def __init__(self):
        self.lock = threading.RLock()
        global log
        log = logging.getLogger("cloudscheduler")
        pass

    @abstractmethod
    def get_vm(self, vmid):
        """
        Return the requested VM
        :param vmid: id of VM to get
        :return: vm object
        """
        pass


    # Tests if the container has a specific VM, by id.
    # Returns True if the container has the given VM, returns False otherwise.
    @abstractmethod
    def has_vm(self, vmid):
        pass

    # Add a VM to the container.
    # If the VM already exist, it will be replaced.
    @abstractmethod
    def add_vm(self, vm):
        pass

    # Add a set of VMs (in a list) to the container.
    # If a VM already exist, it will be replaced.
    @abstractmethod
    def add_vms(self, vms):
        pass

    # Remove all VMs from the container.
    # After calling this method, the container is completely empty.
    @abstractmethod
    def clear(self):
        pass

    # Remove a single VM form the container.
    # If the VM does not exist in the container, then nothing is done.
    @abstractmethod
    def remove_vm(self, vm):
        pass

    # Remove a set of VMs (in a list) from the container.
    # If a VM does not exist in the container, then it is ignored.
    @abstractmethod
    def remove_vms(self, vms):
        pass

    # Remove a VM (by VM id) from the container.
    # If the VM does not exist in the container, then nothing is done.
    @abstractmethod
    def remove_vm_by_id(self, vmid):
        pass

    # Remove a set of VMs (by VM ids, in a list) from the container.
    # If a VM does not exist in the container, then it is ignored.
    @abstractmethod
    def remove_vms_by_id(self, vmids):
        pass

    # Remove all VMs in the container that do not appear in a given set
    # of jobs (in a list).
    @abstractmethod
    def remove_all_not_in(self, vms_to_keep):
        pass


    # Returns True if the container has no jobs, returns False otherwise.
    @abstractmethod
    def is_empty(self):
        pass

    # Returns a string containing human-readable information about this container.
    @abstractmethod
    def __str__(self):
        pass




class HashTableJobContainer(VMContainer):


    def __init__(self):
        VMContainer.__init__(self)
        self.vms = {} # keyed on id

        # might want another hashed with vm type
        # condorname, condoraddr, user
        log.verbose('HashTableVMContainer instance created.')


    def __str__(self):
        return self.vms.__str__()


    def get_vm(self, vmid):
        """
        Return the requested VM
        :param vmid: id of VM to get
        :return: vm object, or None
        """
        try:
            return self.vms[vmid]
        except KeyError:
            return None


    def has_vm(self, vmid):
        return vmid in self.vms


    def add_vm(self, vm):
        self.vms[vm.id] = vm


    def add_vms(self, vms):
        for vm in vms:
            self.add_vm(vm)


    def clear(self):
        self.vms.clear()


    def remove_vm(self, vm):
        with self.lock:
            del self.vms[vm.id]


    def remove_vms(self, vms):
        with self.lock:
            for vm in vms:
                self.remove_vm(vm)


    def remove_vm_by_id(self, vmid):
        with self.lock:
            del self.vms[vmid]


    def remove_vms_by_id(self, vmids):
        with self.lock:
            for vmid in vmids:
                self.remove_vm_by_id(vmid)


    def remove_all_not_in(self, vms_to_keep):
        with self.lock:
            vms_to_keep_dict = {}
            removed_vms = []
            for vm in vms_to_keep:
                vms_to_keep_dict[vm.id] = vm
            for vm in self.vms.values():
                if vm.id not in vms_to_keep_dict:
                    self.remove_vm(vm)
                    removed_vms.append(vm)
        return removed_vms


    def is_empty(self):
        return len(self.vms) == 0
