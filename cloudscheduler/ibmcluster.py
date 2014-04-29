import cluster_tools
import logging
import threading

class IBMCluster(cluster_tools.ICluster):

    VM_STATES = ['Running', 'Rebooting', 'Terminated', 'Pending', 'Unknown']
    VM_STATES_DICT = {'Running': 0, 'Rebooting': 1, 'Terminated': 2, 
                      'Pending': 3, 'Unknown': 4}
    VM_COMPUTE_SIZE_MAP = { 'brz32': 0, 'bronze32': 0,
                            'brz64': 1, 'bronze64': 1,
                            'cop32': 2, 'copper32': 2,
                            'cop64': 3, 'copper64': 3, 
                            'slv32': 4, 'silver32': 4,
                            'slv64': 5, 'silver64': 5,
                            'gld32': 6, 'gold32': 6,
                            'gld64': 7, 'gold64': 7,
                        'plt64': 8, 'platinum64': 8 }

    CLOUD_LOCATION_ID_MAP = {'raleigh': '41', 'ehningen': '61', 'boulder2': '81',
                             'boulder1': '82', 'markham': '101', 'makuhari': '121',
                             'singapore': '141',}

    def __init__(self, name="Dummy Cluster", host="localhost", cloud_type="Dummy",
                 memory=[], max_vm_mem= -1, networks=[], vm_slots=0,
                 cpu_cores=0, storage=0, hypervisor='xen', username="", password="",enabled=True, priority = 0):

        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name, host=host, cloud_type=cloud_type,
                         memory=memory, max_vm_mem=max_vm_mem, networks=networks,
                         vm_slots=vm_slots, cpu_cores=cpu_cores,
                         storage=storage, hypervisor=hypervisor, enabled=enabled, priority=priority)
        from libcloud.compute.types import Provider
        from libcloud.compute.providers import get_driver
        global log
        log = logging.getLogger("cloudscheduler")
        self.username = username
        self.password = password
        self.driver = get_driver(Provider.IBM)
        self.total_cpu_cores = -1
        #self.connection = self.driver(self.username, self.password)
        #self.locations = self.connection.list_locations()
        #self.locations_dict = {loc.id: loc for loc in self.locations}
        #self.compute_sizes = self.connection.list_sizes()
        #self.images = self.connection.list_images()

    def __getstate__(self):
        """Override to work with pickle module."""
        state = self.__dict__.copy()
        del state['vms_lock']
        del state['res_lock']
        del state['driver']
        return state

    def __setstate__(self, state):
        """Override to work with pickle module."""
        self.__dict__ = state
        self.vms_lock = threading.RLock()
        self.res_lock = threading.RLock()
        self.driver = get_driver(Provider.IBM)

    def _get_connection(self, username, password):
        self.connection = self.driver(username, password)
        try:
            self.locations = self.connection.list_locations()
            self.locations_dict = {}
            for loc in self.locations:
                self.locations_dict[loc.id] = loc
            #self.locations_dict = {loc.id: loc for loc in self.locations}
            self.compute_sizes = self.connection.list_sizes()
            self.images = self.connection.list_images()
        except:
            return None
        return self.connection

    def vm_create(self, vm_name, vm_type, vm_user, vm_networkassoc,
                  vm_image, vm_mem, vm_cores, vm_storage, customization, 
                  vm_keepalive, instance_type, location, job_per_core,
                  vm_keyname, username="", password=""):
        # will need to use self.driver.deploy_node(...) as this seems to allow for contextualization whereas create_node() does not
        from libcloud.compute.base import NodeImage, NodeSize, NodeLocation, NodeAuthSSHKey
        if not username:
            username = self.username
        if not password:
            password = self.password
        conn = self._get_connection(username, password)
        if instance_type and instance_type.lower() in self.VM_COMPUTE_SIZE_MAP.keys():
            vm_size = self.compute_sizes[self.VM_COMPUTE_SIZE_MAP[instance_type.lower()]]
        else:
            log.debug("%s not a valid instance type." % instance_type)
            return
        if location.lower() in self.CLOUD_LOCATION_ID_MAP.keys():
            try:
                vm_location = self.locations_dict[self.CLOUD_LOCATION_ID_MAP[location.lower()]]
            except KeyError:
                log.debug("Bad Dict Key mapping")
                return
        else:
            log.debug("%s is not a valid location" % location)
            print 'location invalid'
            return

        image = NodeImage(vm_image, '','')
        # 20035253 image id is a RHL 5.7 that should boot on bronze 32bit and is on markham location
        instance = None
        vm_key = NodeAuthSSHKey(vm_keyname)

        instance = conn.create_node(name=vm_name, image=image, size=vm_size, location=vm_location, auth=vm_key)
        if instance:
            new_vm = VM(name = vm_name, id = instance.uuid, vmtype = vm_type, user = vm_user,
                        clusteraddr = self.network_address,
                        cloudtype = self.cloud_type, network = vm_networkassoc,
                        image= vm_image,
                        memory = vm_mem,
                        cpucores = vm_cores, storage = vm_storage, 
                        keep_alive = vm_keepalive, job_per_core = job_per_core)
            try:
                self.resource_checkout(new_vm)
            except:
                log.exception("Unexpected Error checking out resources when creating a VM. Programming error?")
                self.vm_destroy(new_vm, reason="Failed Resource checkout")
                return self.ERROR
    
            self.vms.append(new_vm)
        else:
            log.debug("No return from create_node()")

        return 0

    def vm_destroy(self, vm, return_resources=True, reason=""):
        nodes = self.connection.list_nodes()
        for node in nodes:
            if node.uuid == vm.id:
                self.connection.destroy_node(node)
                log.info("VM %s Destroyed: Reason: %s" % (vm.id, reason))
                # return resources
                if return_resources:
                    self.resource_return(vm)
                with self.vms_lock:
                    self.vms.remove(vm)
                return 0
        

    def vm_poll(self, vm):
        # libcloud does not seem to support polling individual VMs you simply list off what you have
        # ineffecient this way but is in line with other clouds
        nodes = self.connection.list_nodes()
        for node in nodes:
            if node.uuid == vm.id:
                if not vm.ipaddress:
                    if node.public_ip:
                        vm.ipaddress = node.public_ip[0]
                if self.VM_STATES[node.state] == vm.status:
                    continue
                # Both startup and shutdown enter the Pending state
                elif vm.status == 'Running' and node.state == 3:
                    vm.status = self.VM_STATES[node.state]
                    vm.override_status = 'Stopping'
                    vm.last_state_change = int(time.time())
                    log.debug("VM: %s on %s. Changed from %s to Stopping." % (vm.id, self.name, vm.status))
                elif vm.status == 'Starting' and node.state == 3:
                    vm.status = self.VM_STATES[node.state]
                    vm.last_state_change = int(time.time())
                    log.debug("VM: %s on %s. Changed from %s to %s." % (vm.id, self.name, vm.status, self.VM_STATES[node.state]))
                else:
                    vm.status = self.VM_STATES[node.state]
                    vm.last_state_change = int(time.time())
                    log.debug("VM: %s on %s. Changed from %s to %s." % (vm.id, self.name, vm.status, self.VM_STATES[node.state]))
            else:
                continue
        pass
