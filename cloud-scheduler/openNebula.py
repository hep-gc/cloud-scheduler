#! /usr/bin/python


import xmlrpclib
import re
import cloud_management
import logging

oneLog = 'oneCloud.log'
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s", filename=oneLog, filemode='a')

class OpenNebulaCloud(cloud_management.Cluster):
	
	def vm_create(self, vm_name, vm_networkassoc, vm_cpuarch, vm_imagelocation, vm_mem, vm_scratchSpace):

		template = 'NAME = ' + vm_name + '\nCPU = .1\nMEMORY = ' + str(vm_mem) + '\nOS = [bootloader = "/usr/bin/pygrub"]\nDISK = [source = "' + vm_imagelocation + '", target = "sda", readonly = "no"]\nNIC = [NETWORK = "test"]\nREQUIREMENTS = "ARCH = ' + vm_cpuarch + '"'
		logging.info(self.name + ' Creating VM \n' + template)
		response = self.getProxy().one.vmallocate('', template)
		if response[0]:
			id = response[1]
			memoryEntry = self.find_mementry(vm_mem)
			if memoryEntry < 0:
				raise Exception('Cluster ' + self.name + ' out of memory')
			vm = cloud_management.VM(vm_name, str(id), self.network_address, self.cloud_type, vm_networkassoc, vm_cpuarch, vm_imagelocation, vm_mem, memoryEntry)	
			self.vms.append(vm)
			self.vm_slots -= 1
			self.memory[memoryEntry] -= vm_mem
			logging.info(self.name + ' VM ' + str(id) + ' Created')
		else:
			raise Exception(response[1])
	
	def vm_poll(self, vm):
		logging.info(self.name + ' Polling VM ' + str(vm.name))
		response = self.getProxy().one.vmget_info('', int(vm.id))
		if response[0]:
			results = re.findall('STATE\s+: (\S+)\s+LCM STATE\s+: (\S+)', response[1])
			if len(results) == 1 and len(results[0]) == 2:
				state = results[0][0]
				lcmstate = results[0][1]
				logging.info(self.name + ' VM ' + str(vm.name) + ' status ' +  status(state, lcmstate) + ' (ONE ' + oneStatus(state, lcmstate) + ')') 
				return status(state, lcmstate)
					
			raise Exception('Error Parsing VM state')
		else:
			raise Exception(response[1])

	def vm_destroy(self, vm):
		logging.info(self.name + ' Destroying VM ' + str(vm.name)) 
		pollResponse = self.vm_poll(vm)
		if pollResponse == 'RUNNING':
			destroyResponse = self.getProxy().one.vmaction('', 'shutdown', int(vm.id))
			if destroyResponse[0]:
				logging.info(self.name + ' VM ' + str(vm.name) + ' Shutting Down') 
				self.vm_slots += 1
				self.memory[vm.mementry] += vm.memory
				self.vms.remove(vm)
			else:
				raise Exception(destroyResponse[1])
		else:
			raise Exception('Wrong VM state to shutdown')

	def getProxy(self):
		return xmlrpclib.ServerProxy(self.network_address)

def oneStatus(state, lcmstate):
	if state == '0':
		return 'INIT'
	elif state == '1':
		return 'PENDING'
	elif state == '2':
		return 'HOLD'
	elif state == '3':
		if lcmstate == '1':
			return 'PROLOG'
		elif lcmstate == '2':
			return 'BOOT'
		elif lcmstate == '3':
			return 'RUNNING'
		elif lcmstate == '4':
			return 'MIGRATE'
		elif lcmstate == '5':
			return 'SAVE_STOP'
		elif lcmstate == '6':
			return 'SAVE_SUSPEND'
		elif lcmstate == '7':
			return 'SAVE_MIGRATE'
		elif lcmstate == '8':
			return 'PROLOG_MIGRATE'
		elif lcmstate == '9':
			return 'PROLOG_RESUME'
		elif lcmstate == '10':
			return 'EPILOG_STOP' 
		elif lcmstate == '11':
			return 'EPILOG'
		elif lcmstate == '12':
			return 'SHUTDOWN'
		elif lcmstate == '13':
			return 'CANCEL'
	elif state == '4':
		return 'STOPPED'
	elif state == '5':
		return 'SUSPENDED'
	elif state == '6':
		return 'DONE'
	elif state == '7':
		return 'FAILED'
	else:
		raise Exception('Bad VM state')

def status(state, lcmstate):
	if state == '3' and lcmstate == '3':
		return 'RUNNING'
	elif state == '0' or state == '1' or (state == '3' and (lcmstate == '1' or lcmstate == '2')):
		return 'CREATING'
	elif state == '6' or (state == '3' and (lcmstate == '11' or lcmstate == '12' or lcmstate == '13')):
		return 'SHUTDOWN'
	else:
		return 'ERROR'
