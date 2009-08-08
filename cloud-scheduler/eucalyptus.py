#! /usr/bin/python

import cloud_management
import logging

class EucalyptusCluster(cloud_management.Cluster):

	def vm_create(self, vm_name, vm_networkassoc, vm_cpuarch, vm_imagelocation, vm_mem, vm_scratchSpace):
		None

	def vm_poll(self, vm):
		None

	def vm_destroy(self, vm):
		None
	
