# setup.py - standard distutils setup file for Cloud Scheduler
import os
import os.path
import sys
try:
    from setuptools import setup
except:
    try:
        from distutils.core import setup
    except:
        print "Couldn't use either setuputils or distutils. Install one of those. :)"
        sys.exit(1)
import cloudscheduler.__version__ as version

if not os.geteuid() == 0:
    config_files_dir = os.path.expanduser("~/.cloudscheduler/")
else:
    config_files_dir = "/etc/cloudscheduler/"
config_files = ["cloud_scheduler.conf", "cloud_resources.conf"]

# check for preexisting config files
data_files = okay_files = []
for config_file in config_files:
    if not os.path.isfile(config_files_dir + os.path.basename(config_file)):
        okay_files.append(config_file)
if okay_files:
    data_files = [(config_files_dir, okay_files)]

setup(name = "cloud-scheduler",
    version = version.version,
    license="'GPL3' or 'Apache 2'",
    install_requires=[
       "boto>=2.0b3",
        ],
    description = "A cloud-enabled distributed resource manager",
    author = "Duncan Penfold-Brown, Chris Usher, Patrick Armstrong, Ian Gable, Michael Paterson, Andre Charbonneau",
    author_email = "mhp@uvic.ca",
    url = "http://github.com/hep-gc/cloud-scheduler",
    packages = ['cloudscheduler'],
    package_data = {'cloudscheduler' : ["wsdl/*"] },
    data_files = data_files,
    scripts = ["cloud_scheduler", "cloud_status", "cloud_admin"],
) 
