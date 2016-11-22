# setup.py - standard distutils setup file for Cloud Scheduler
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

setup(name = "cloud-scheduler",
    version = version.version,
    license="'GPL3' or 'Apache 2'",
    install_requires=[
        "boto>=2.38.0",
        "requests>=2.2.0",
        "web.py>=0.3",
        "boto3",
        "botocore",
        ],
    description = "A cloud-enabled distributed resource manager",
    author = "Duncan Penfold-Brown, Chris Usher, Patrick Armstrong, Ian Gable, Michael Paterson, Andre Charbonneau",
    author_email = "mhp@uvic.ca",
    url = "http://github.com/hep-gc/cloud-scheduler",
    packages = ['cloudscheduler'],
    package_data = {'cloudscheduler' : ["wsdl/*"] },
    data_files = [("share/cloud-scheduler/", ["cloud_scheduler.conf", "cloud_resources.conf", "default.yaml","scripts/cloud_scheduler.init.d", "scripts/cloud_scheduler.sysconf"])],
    scripts = ["cloud_scheduler", "cloud_status", "cloud_admin"],
) 
