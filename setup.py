# setup.py - standard distutils setup file for Cloud Scheduler
import sys
from distutils.core import setup
import cloudscheduler.__version__ as version

setup(name = "Cloud Scheduler",
    version = version.version,
    description = "Cloud Scheduler is a cloud-enabled Condor backend. It boots VMs to suit the jobs you submit to your Condor pool",
    author = "Duncan Penfold-Brown, Chris Usher, Patrick Armstrong, Ian Gable at University of Victoria",
    author_email = "dpb@uvic.ca",
    url = "http://github.com/hep-gc/cloud-scheduler",
    packages = ['cloudscheduler'],
    package_data = {'cloudscheduler' : ["wsdl/*"] },
    data_files = [('/etc/cloudscheduler', ["cloud_scheduler.conf", "cloud_resources.conf"])],
    scripts = ["cloud_scheduler", "cloud_status"],
) 
