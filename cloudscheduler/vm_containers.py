"""
 This is an abstract base class; do not instantiate directly.

 API documentation should go in here, as opposed to writing specific
 documentation for each concrete subclasses.
"""

from abc import ABCMeta, abstractmethod
import threading
import logging

# Use this global variable for logging.
log = None

class VMContainer(object):
    __metclass__ = ABCMeta

    # Use this lock if you require to threadsafe an operation.
    lock = None
    ## VM States
    vm_status_list = ['Starting', 'Running', 'Retiring', 'Error']
    def __init__(self):
        self.lock = threading.RLock()
        global log
        log = logging.getLogger("cloudscheduler")

    @abstractmethod
    def get_vm(self, vmid):
        """
        Return the requested VM
        :param vmid: id of VM to get
        :return: vm object
        """
        pass

    @abstractmethod
    def has_vm(self, vmid):
        """
        Tests if the container has a specific VM, by id.
        Returns True if the container has the given VM, returns False otherwise.
        :param vmid:
        """
        pass

    @abstractmethod
    def add_vm(self, vm):
        """
        Add a VM to the container.
        If the VM already exist, it will be replaced.
        :param vm:
        """
        pass

    @abstractmethod
    def add_vms(self, vms):
        """
        Add a set of VMs (in a list) to the container.
        If a VM already exist, it will be replaced.
        :param vms:
        """
        pass

    @abstractmethod
    def clear(self):
        """
        Remove all VMs from the container.
        After calling this method, the container is completely empty.
        """
        pass

    @abstractmethod
    def remove_vm(self, vm):
        """
        Remove a single VM form the container.
        If the VM does not exist in the container, then nothing is done.
        :param vm:
        """
        pass

    @abstractmethod
    def remove_vms(self, vms):
        """
        Remove a set of VMs (in a list) from the container.
        If a VM does not exist in the container, then it is ignored.
        :param vms:
        """
        pass

    @abstractmethod
    def remove_vm_by_id(self, vmid):
        """
        Remove a VM (by VM id) from the container.
        If the VM does not exist in the container, then nothing is done.
        :param vmid:
        """
        pass

    @abstractmethod
    def remove_vms_by_id(self, vmids):
        """
        Remove a set of VMs (by VM ids, in a list) from the container.
        If a VM does not exist in the container, then it is ignored.
        :param vmids:
        """
        pass

    @abstractmethod
    def remove_all_not_in(self, vms_to_keep):
        """
        Remove all VMs in the container that do not appear in a given set
        of jobs (in a list).
        :param vms_to_keep:
        """
        pass

    @abstractmethod
    def is_empty(self):
        """
        Returns True if the container has no jobs, returns False otherwise.
        """
        pass

    @abstractmethod
    def __str__(self):
        """
        Returns a string containing human-readable information about this container.
        """
        pass


class HashTableJobContainer(VMContainer):

    """
    Implementation of VMContainer base class using HashTables/Dictionaries.
    """
    def __init__(self):
        """
        Constructor.
        """
        VMContainer.__init__(self)
        self.vms = {} # keyed on id

        # might want another hashed with vm type
        # condorname, condoraddr, user
        log.verbose('HashTableVMContainer instance created.')


    def __str__(self):
        """
        Printable string of the VMs in the container.
        :return:
        """
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
