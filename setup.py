# setup.py - standard distutils setup file for Cloud Scheduler


from distutils.core import setup

#This is a list of files to install, and where
#(relative to the 'root' dir, where setup.py is)
#You could be more specific.
files = ["wsdl/*"]

setup(name = "Cloud Scheduler",
    version = "0.3",
    description = "Cloud Scheduler is a cloud-enabled Condor backend. It boots VMs to suit the jobs you submit to your Condor pool",
    author = "Duncan Penfold-Brown, Chris Usher, Patrick Armstrong, Ian Gable at University of Victoria",
    author_email = "dpb@uvic.ca",
    url = "http://github.com/hep-gc/cloud-scheduler",
    packages = ['cloudscheduler'],
    package_data = {'cloudscheduler' : files },
    #'runner' is in the root.
    scripts = ["cloud_scheduler", "cloud_status"],
) 
