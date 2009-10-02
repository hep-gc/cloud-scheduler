#! /usr/bin/python

# This program determines the free space in the filesystem mounted as / and overwrites this space with zeros before deleting it. The removal of the non uniform bytes from deleted files can drastically reduce the size and compression time of an image. 

# Chris Usher May 2009

import re
import subprocess

#figure out how much free space exists
discInfo = subprocess.Popen('df -P', shell=True, stdout=subprocess.PIPE).communicate()[0]
lines = discInfo.split('\n')
free = 0
for line in lines:
	match = re.findall('^\S+\s+\d+\s+\d+\s+(\d+)\s+\S+\s+/$', line)
	if len(match):
		free = int(match[0]) - 10240 #small off set of 10 MB so not to completely fill the file system 

print 'Cleaning', free, 'KB of free space. This may take a while.'

# overwrite and remove
if free > 0:
	subprocess.Popen('dd if=/dev/zero bs=1K count=' + str(free) + ' >> blank', shell=True, stdout=subprocess.PIPE).wait()
	subprocess.Popen('rm -f blank', shell=True).wait()
print 'Done'
