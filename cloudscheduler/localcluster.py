"""
Creates, Destroys and Polls VM's for the localhost
"""
import os
import sys
import time
import tempfile
import subprocess
import yaml
from pathlib2 import Path
from cloudscheduler import cluster_tools
from cloudscheduler import cloud_init_util
import cloudscheduler.config as config
import cloudscheduler.utilities as utilities

log = utilities.get_cloudscheduler_logger()
config_val = config.config_options

class LocalCluster(cluster_tools.ICluster):
    """
    LocalCluster simulates a cloud using libvirt on the local machine
    """
    ERROR = 1
    VM_STATES = {
        "VIR_DOMAIN_NOSTATE": 'Error',
        "VIR_DOMAIN_RUNNING": 'Running',
        "VIR_DOMAIN_BLOCKED": 'Error',
        "VIR_DOMAIN_PAUSED": 'Paused',
        "VIR_DOMAIN_SHUTDOWN": 'Shutdown',
        "VIR_DOMAIN_SHUTOFF": 'Stopped',
        "VIR_DOMAIN_CRASHED": 'Error',
        "VIR_DOMAIN_PMSUSPENDED": 'Suspended',
    }
    def __init__(self, name="Dummy Cluster", cloud_type="Dummy",
                 memory=[], max_vm_mem=-1, networks=[], vm_slots=0,
                 cpu_cores=0, key_name=None, enabled=True, priority=0,
                 keep_alive=0):

        # Call super class's init
        cluster_tools.ICluster.__init__(self, name=name, cloud_type=cloud_type,
                                        memory=memory, max_vm_mem=max_vm_mem, networks=networks,
                                        cpu_cores=cpu_cores, vm_slots=vm_slots,
                                        enabled=enabled,
                                        priority=priority, keep_alive=keep_alive,)

        self.key_name = key_name if key_name else ""
        self.session = None

        try:
            import libvirt
        except ImportError:
            log.debug("Unable to import libvirt - cannot use local cloud type")
            sys.exit(1)

    def __getstate__(self):
        state = cluster_tools.ICluster.__getstate__(self)
        return state

    def __setstate__(self, state):
        cluster_tools.ICluster.__setstate(self, state)

    def vm_create(self, vm_name, vm_image, vm_mem, vm_cores, vm_type, vm_user,
                  vm_keepalive=0, network='default', customizations=None,
                  pre_customization=None, extra_userdata="", key_name=""):

        import libvirt
        ###Create a VM on LocalHost.###

        conn = libvirt.open(None)
        if conn is None:
            log.debug("Failed to open connection to hypervisior")
            sys.exit(1)

        #get VM name and create a working directory
        name = self._generate_next_name()

        config_tmp = tempfile.mkdtemp(suffix="-"+name)
        
        log.debug(config_val.get('global', 'condor_host_on_vm'))
        log.debug(config_val.get('global', 'condor_context_file'))
        if customizations:
            user_data = cloud_init_util.build_write_files_cloud_init(customizations)
        else:
            user_data = ""

        if pre_customization:
            user_data = cloud_init_util.inject_customizations(pre_customization, user_data)

        if os.path.exists('/etc/cloudscheduler/auth-key.yaml'):
            extra_userdata = ['/etc/cloudscheduler/auth-key.yaml']+extra_userdata

        if len(extra_userdata) > 0:
            # need to use the multi-mime type functions
            user_data = cloud_init_util.build_multi_mime_message([(user_data, 'cloud-config', 'cloud_conf.yaml')],
                                                                 extra_userdata)
            if not user_data:
                log.error("Problem building cloud-config user data.")
                return self.ERROR

        raw_user = user_data
        user_data = utilities.gzip_userdata(user_data)

        try:
            if self.name in vm_image.keys():
                image = vm_image[self.name]
            elif self.network_address in vm_image.keys():
                image = vm_image[self.network_address]
            else:
                image = vm_image['default']
        except Exception as error:
            log.error("Could not determine image: %s", error)

	###INSIDE A CONTAINER: The default image repo is /jobs/repo, will check there that the image exists
	#to-do: add optional specification for image repo location

	#check image exists in repo
        if os.path.exists('/jobs/instances/base/'+image):
            path = '/jobs/instances/base/'+image
        elif os.path.exists(image):
            path = image
            image = os.path.basename(path)
        else:
            log.error('Could not find image %s: Does not exists in image repository', image)
            return 1

	#check image format
        if image.endswith('.img'):
            image_copy = image.rstrip('.img')
            image_copy = image_copy+'-'+name+'.qcow2'
            subprocess.call('qemu-img create -f qcow2 -b '+path+' /jobs/instances/'+image_copy, shell=True)
            image = image_copy
            path = '/jobs/instances/'+image
        elif image.endswith('.qcow2'):
            image_copy = image.rstrip('.qcow2')
            base = image_copy+'.img'
            if not os.path.exists('/jobs/instances/base/'+base):
                subprocess.call('qemu-img convert -f qcow2 -O raw '+path+' /jobs/instances/base/'+base, shell=True)
            image_copy = image_copy+'-'+name+'.qcow2'
            subprocess.call('qemu-img create -f qcow2 -b /jobs/instances/base/'+base+' /jobs/instances/'+image_copy, shell=True)
            image = image_copy
            path = '/jobs/instances/'+image

        #Create the config directory for metadata server
        metapath = self._generate_meta(name)

        subprocess.call('mv '+metapath+' '+config_tmp+'/meta-data', shell=True)
        subprocess.call('rm -f '+metapath, shell=True)

        with open(config_tmp+"/user-data", 'w') as ufile:
            ufile.write(user_data)
        with open(config_tmp+'/raw-user', 'w') as raw:
            raw.write(raw_user)
        try:
            subprocess.call("mkisofs -o "+config_tmp+"/config.iso -V cidata -r -J --quiet "
                            +config_tmp+"/meta-data "+config_tmp+"/user-data", shell=True)
        except Exception as error:
            log.debug("Could not create config dir")


        virt_call = "virt-install --name "+name+" --network "+network+ \
                    " --print-xml --dry-run -r "+str(vm_mem)+" --disk path="+path+ \
                    ",sparse=true --disk path="+config_tmp+ \
                    "/config.iso,device=cdrom --import --serial file,path="+config_tmp+"/boot-log"

        image_xml = subprocess.check_output(virt_call, shell=True)
        dom = conn.createXML(image_xml, 0)
        if dom is None:
            log.debug("Failed to create domain from xml definiton")
            sys.exit(1)
        else:
            if not vm_keepalive and self.keep_alive:
                vm_keepalive = self.keep_alive

            new_vm = cluster_tools.VM(name=name, id=dom.ID(), vmtype=vm_type, hostname=self.name,
                                      user=vm_user, cloudtype=self.cloud_type, network=network,
                                      image=image, memory=vm_mem, cpucores=vm_cores,
                                      keep_alive=vm_keepalive)
            try:
                self.resource_checkout(new_vm)
                log.info("Launching 1 VM: %s on %s ", dom.ID(), self.name)
                self.vms.append(new_vm)
            except Exception as error:
                log.error("Unexpected Error checking out resources when creating a VM. Programming error?: %s", error)
                self.vm_destroy(new_vm, reason="Failed Resource checkout", return_resources=False)
                return self.ERROR
        conn.close()

        return 0

    def vm_destroy(self, vm, return_resources=True, reason=""):

        import libvirt
        """ Destroy a VM on OpenStack."""
        log.info("Destroying VM: %s Name: %s on %s Reason: %s", vm.id, vm.hostname, self.name, reason)
        conn = libvirt.open(None)
        if conn is None:
            print "Failed to open connection with hypervisor"
            sys.exit(1)
        dom = conn.lookupByName(vm.name)
        if dom is None:
            log.error("VM %s not found on %s: removing from CS", vm.id, self.name)
        dom.destroy()

        conn.close()

        # Delete references to this VM
        try:
            if return_resources and vm.return_resources:
                self.resource_return(vm)
            with self.vms_lock:
                self.vms.remove(vm)
            if config_val.get('global', 'monitor_url'):
                self._report_monitor(vm)
        except Exception as e:
            log.error("Error removing vm from list: %s", e)
            return 1

	#clean up tmp directory and image copy
        try:
            subprocess.call('rm -f /jobs/instaces/'+vm.image, shell=True)
        except Exception as e:
            log.error("Error in deleting VM %s image %s", vm.name, vm.image)
            log.error(e)

        try:
            pipe = subprocess.Popen(['ls', '/tmp'], stdout=subprocess.PIPE)
            tmp = pipe.communicate()[0]
            for tmp_file in tmp.split():
                tmp_file = tmp_file.rstrip()
                if tmp_file.endswith(vm.name):
                    tmp_dir = '/tmp/'+tmp_file
                    log.error('Found tmp directory %s', tmp_dir)
                    subprocess.call('rm -rf '+tmp_dir, shell=True)
        except Exception as e:
            log.error("Could not remove the tmp directory for VM %s: %s", vm.name, e)

        return 0
    def vm_poll(self, vm):
        """ Polling VM's using libvirt"""
        import libvirt
        conn = libvirt.open(None)
        if conn is None:
            print "Failed to establish connection with hypervisor"
            sys.exit(1)
        dom = conn.lookupByName(vm.name)
        if dom is None:
            vm.status = self.VM_STATES['ERROR']
            log.error("VM %s not found on %s: removing from CS", vm.id, self.name)
        (state, reason) = dom.state()
        vm.last_state_change = int(time.time())

        if state == libvirt.VIR_DOMAIN_NOSTATE:
            vm.status = self.VM_STATES['VIR_DOMAIN_NOSTATE']
        elif state == libvirt.VIR_DOMAIN_RUNNING:
            vm.status = self.VM_STATES['VIR_DOMAIN_RUNNING']
        elif state == libvirt.VIR_DOMAIN_BLOCKED:
            vm.status = self.VM_STATES['VIR_DOMAIN_BLOCKED']
        elif state == libvirt.VIR_DOMAIN_PAUSED:
            vm.status = self.VM_STATES['VIR_DOMAIN_PAUSED']
        elif state == libvirt.VIR_DOMAIN_SHUTDOWN:
            vm.status = self.VM_STATES['VIR_DOMAIN_SHUTDOWN']
        elif state == libvirt.VIR_DOMAIN_SHUTOFF:
            vm.status = self.VM_STATES['VIR_DOMAIN_SHUTOFF']
        elif state == libvirt.VIR_DOMAIN_CRASHED:
            vm.status = self.VM_STATES['VIR_DOMAIN_CRASHED']
        elif state == libvirt.VIR_DOMAIN_PMSUSPENDED:
            vm.status = self.VM_STATES['VIR_DOMAIN_PMSUSPENDED']
        else:
            vm.status = 'unknown'
        return vm.status

    def _generate_meta(self, name):
        instance_id = name
        host = name
        (fd, file_path) = tempfile.mkstemp(text=True)

        meta_info = {'instance-id':instance_id, 'local-hostname':host}

        with open(file_path, 'w') as yaml_file:
            yaml.dump(meta_info, yaml_file, default_flow_style=False)
        return file_path

