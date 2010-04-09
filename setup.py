# setup.py - standard distutils setup file for Cloud Scheduler
import os.path
import sys
from distutils.core import setup
import cloudscheduler.__version__ as version

config_files_dir = "/etc/cloudscheduler/"
config_files = ["cloud_scheduler.conf", "cloud_resources.conf"]

# check for preexisting config files
data_files = okay_files = []
for config_file in config_files:
    if not os.path.isfile(config_files_dir + os.path.basename(config_file)):
        okay_files.append(config_file)
if okay_files:
    data_files = [(config_files_dir, okay_files)]

setup(name = "Cloud Scheduler",
    version = version.version,
    description = "Cloud Scheduler is a cloud-enabled Condor backend. It boots VMs to suit the jobs you submit to your Condor pool",
    author = "Duncan Penfold-Brown, Chris Usher, Patrick Armstrong, Ian Gable at University of Victoria",
    author_email = "dpb@uvic.ca",
    url = "http://github.com/hep-gc/cloud-scheduler",
    packages = ['cloudscheduler'],
    package_data = {'cloudscheduler' : ["wsdl/*"] },
    data_files = data_files,
    scripts = ["cloud_scheduler", "cloud_status"],
) 
