#! /usr/bin/python

from oneCloud import *
from cloud_management import *

cluster = OneCloud()
cluster.populate(['onetest', 'http://ugdev05.phys.uvic.ca:2633/RPC2', 'OpenNebula', 1, 1, 0, 512, 'yes', 'no', 'no', 'no'])
vm = VM('test', '27', '', 'OpenNebula', '', '', '', 0) 
#cluster.print_cluster()
print cluster.vm_poll(vm)
#print cluster.vm_destroy(26)
#print cluster.vm_create('test', '/usr/local/one/images/sl53iraf.img', 'i686', 512, 'test', None)


