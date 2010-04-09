# setup.py - standard distutils setup file for EC2 Context Helper
from distutils.core import setup

setup(name = "Context Helper",
    version = 0.0,
    description = "Does Nimbus-style contextualization with Amazon EC2",
    author = "Patrick Armstrong at University of Victoria",
    author_email = "patricka@uvic.ca",
    url = "http://github.com/hep-gc/cloud-scheduler",
    data_files = [("/etc/init.d/", ["context"])],
    scripts = ["contexthelper"],
) 

