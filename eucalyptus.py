#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

# Copyright 2009 University of Victoria
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.


import cloud_management
import logging
import subprocess
import re

log = logging.getLogger("CloudLogger")

class EucalyptusCluster(cloud_management.Cluster):
    
    configScript = '' # this script sets the enviroment varables eucalyptus needs.
    instanceTypes = [] # list


    def populate(self, attr_list):
        cloud_management.Cluster.populate(self, attr_list)
        self.configScript = getConfigScriptName(self.network_address)
        command = 'ec2-describe-availability-zones verbose'
        log.info(self.name + ' Identifing instance types')
        response = self.callEC2(command)
        if response:
            for line in response:
                results = re.findall('AVAILABILITYZONE\s+\|-\s+(\S+)\s+\d{4} / (\d{4})\s+(\d+)\s+(\d+)\s+(\d+)', line)
                if len(results) == 1 and int(results[0][1]):
                    self.instanceTypes.append({'type':results[0][0], 'cpus':int(results[0][2]), 'ram':int(results[0][3]), 'disc':int(results[0][4])})
        else:
            raise Exception('Can not get instance types ' + response)
    

    def callEC2(self, command):
        return subprocess.Popen('bash -c "source ' + self.configScript + '; ' + command + '"', shell=True, stdout=subprocess.PIPE).communicate()[0].split('\n')

    def vm_create(self, vm_name, vm_networkassoc, vm_cpuarch, vm_imagelocation, vm_mem, vm_scratchSpace):
        imageType = ''

        for type in self.instanceTypes:
            if type['ram'] >= vm_mem and type['disc'] >= vm_scratchSpace: # and type['cpus'] >= vm_cpus
                imageType = type['type']
                break

        if not imageType:
            raise Exception('No Instance Types match requirements')

        command = 'ec2-run-instances ' + getImageId(vm_imagelocation) + ' -t ' + imageType
        log.info(self.name + ' Creating VM \n' + command)
        response = self.callEC2(command)
        
        results = re.findall('INSTANCE\s+(\S+)\s+\S+\s+\d+.\d+.\d+.\d+\s+\d+.\d+.\d+.\d+\s+(\S+)\s+\S+\s+\S+\s+\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d[-+]\d{4}',response[1])
        if results:
            id = results[0][0]
            memoryEntry = self.find_mementry(vm_mem)
            if memoryEntry < 0:
                raise Exception('Cluster ' + self.name + ' out of memory')
            vm = cloud_management.VM(vm_name, str(id), self.network_address, self.cloud_type, vm_networkassoc, vm_cpuarch, vm_imagelocation, vm_mem, memoryEntry)   
            self.vms.append(vm)
            self.vm_slots -= 1
            self.memory[memoryEntry] -= vm_mem
            log.info(self.name + ' VM ' + str(id) + ' Created')
        else:
            raise Exception('Error Creating VM ' + response)

    def vm_poll(self, vm):
        command = 'ec2-describe-instances ' + vm.id
        log.info(self.name + ' Polling VM ' + str(vm.name))
        response = self.callEC2(command)
        status = 'Error'
        # deal with non existant VM
        if response == ['']:
            log.info(self.name + ' VM ' + str(vm.name) + ' Not found VM likely shutdown')
            return status
        results = re.findall('INSTANCE\s+(\S+)\s+\S+\s+\d+.\d+.\d+.\d+\s+\d+.\d+.\d+.\d+\s+(\S+)\s+(?:\S+\s+)?\S+\s+\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d[-+]\d{4}',response[1])
        if results and results[0][0] == vm.id:
            state = results[0][1]
            if state == 'running':
                status = 'Running'
            elif state == 'pending':
                status = 'Starting'
            log.info(self.name + ' VM ' + str(vm.name) + ' STATUS ' + status + ' (EUCA ' + state + ')')
            return status
        else:
            raise Exception('Error getting VM state ' + response)

        
    def vm_destroy(self, vm):
        command = 'ec2-terminate-instances ' + vm.id
        log.info(self.name + ' Destroying VM ' + str(vm.name))
        response = self.callEC2(command)
        result = re.findall('INSTANCE\t+(\S+)\trunning\tshutting-down',response[0]) 

        if result and result[0] == vm.id:
            log.info(self.name + ' VM ' + str(vm.name) + ' Shutting Down')
            self.vm_slots += 1
            self.memory[vm.mementry] += vm.memory
            self.vms.remove(vm)

        else:
            raise Exception('Error destroying VM ' + response)
    


def getConfigScriptName(networkAddress):
    map = {'http://142.104.60.60:8773/services/Eucalyptus':'/home/chrusher/seed.sh'} # this should be read from a file
    return map[networkAddress]

def getImageId(imageName):
    map = {'ttylinux':'emi-F4651175'}
    return map[imageName]
