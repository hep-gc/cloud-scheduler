# Cloud Scheduler 0.9.1 README

## Introduction
Cloud Scheduler: Automatically boot VMs for your HTC jobs

Cloud Scheduler manages virtual machines on clouds configured with Nimbus,
Eucalyptus, or Amazon EC2 to create an environment for HTC batch job execution.
Users submit their jobs to a Condor job queue, and Cloud Scheduler boots VMs to
suit those jobs, creating a malleable environment for efficient job execution
and resource utilization.

For more documentation on Cloud Scheduler, please refer to:

-  [Cloud Scheduler Wiki](http://wiki.github.com/hep-gc/cloud-scheduler)
-  [Cloud Scheduler Homepage](http://cloudscheduler.org)


## Prerequisites

* A working Condor 7.5.x install (details below)
* Nimbus Cloud Client tools (details below)
* [Python 2.6+](http://www.python.org/)
* [Suds 0.3.9+](https://fedorahosted.org/suds/)
* [boto](http://code.google.com/p/boto/)
* [lxml](http://codespeak.net/lxml/)

## Optional Prerequisites

* [Guppy](http://guppy-pe.sourceforge.net/) -- Used for memory usage info.

### Special help for RHEL 5

Since Cloud Scheduler requires Python 2.6, and we recognize that RHEL 5 comes
with and requires Python 2.4, here's a quick guide to getting Python 2.6 
installed on those systems:

Install the tools we need to build Python and its modules:

    # yum install gcc gdbm-devel readline-devel ncurses-devel zlib-devel \
      bzip2-devel sqlite-devel db4-devel openssl-devel tk-devel \
      bluez-libs-devel libxslt libxslt-devel libxml2-devel libxml2

Download and compile Python 2.6:

    $ VERSION=2.6.5
    $ mkdir /tmp/src 
    $ cd /tmp/src/
    $ wget http://python.org/ftp/python/$VERSION/Python-$VERSION.tar.bz2
    $ tar xjf Python-$VERSION.tar.bz2
    $ rm Python-$VERSION.tar.bz2
    $ cd Python-$VERSION 
    $ ./configure --prefix=/opt
    $ make
    $ sudo make install

Now we need to install Python setuputils:

    $ cd /tmp/src
    $ wget http://pypi.python.org/packages/2.6/s/setuptools/setuptools-0.6c11-py2.6.egg#md5=bfa92100bd772d5a213eedd356d64086
    $ sudo PATH=/opt/bin:$PATH sh setuptools-0.6c11-py2.6.egg

Now install pip to install the rest of our dependencies:

    $ sudo /opt/bin/easy_install pip

And the rest of our dependencies:
 
    $ sudo /opt/bin/pip install simplejson suds boto lxml virtualenv

Now clean everything up:

    $ sudo rm -Rf /tmp/src/

Finally, once you've set up the rest of Cloud Scheduler, you'll want to set
your Python version in the Cloud Scheduler init script, or use virtualenv.
Do this by changing the PYTHON variable to /opt/bin/python

### Other distros:

You can install the Python libraries listed above with pip:

lxml requires libxml2 and libxslt and their development libs to be installed. 

Install pip:

    # easy_install pip

And all your packages:

    # pip install simplejson suds boto lxml

## Install
To install cloud scheduler, as root, run:

    # python setup.py install

## Condor Install
Cloud Scheduler works with [Condor](http://www.cs.wisc.edu/condor/), which needs
to be installed and able to manage resources. You can install it on the same
machine that runs Cloud Scheduler (or not). You need to enable SOAP to allow
Cloud Scheduler to communicate with Condor. You can do this by adding the
following to your Condor config file, which is usually located at:
/etc/condor/condor_config:

    ## CLOUD SCHEDULER SETTINGS
    ENABLE_SOAP = TRUE
    ENABLE_WEB_SERVER = TRUE
    WEB_ROOT_DIR=$(RELEASE_DIR)/web
    ALLOW_SOAP=localhost, 127.0.0.1
    SCHEDD_ARGS = -p 8080

We also recommend the following settings, especially if you're planning on
using Condor CCB:

    UPDATE_COLLECTOR_WITH_TCP=True
    COLLECTOR_SOCKET_CACHE_SIZE=10000
    COLLECTOR.MAX_FILE_DESCRIPTORS = 10000


We have also placed an example Condor config in scripts/condor/manager

Make sure you can run condor_status and condor_q, and make sure your
ALLOW_WRITE will permit the VMs you will start to add themselves to your Condor
Pool.

Condor must also be installed on your VM images that will run your jobs. There
is a sample configuration for your Condor installation in scripts/condor/worker/
condor_config, condor_config.local and central_manager must be in /etc/condor/
and you must use the customized condor init script scripts/condor/worker/condor

## Installing Nimbus Cloud Client

The Nimbus Cloud Client is the standard client used to connect to Nimbus
clouds. Cloud Scheduler uses this client to communicate with Nimbus.

Until Nimbus Cloud Client 015 is released, you need to install a patched
version of Cloud Client. We assume you want to install Cloud Client to /opt.

    $ wget http://cloud.github.com/downloads/oldpatricka/nimbus/nimbus-cloud-client-015.tar.gz
    $ tar xzf nimbus-cloud-client-015.tar.gz

Normally, the workspace reference client, workspace.sh, isn't executable. The
way Cloud Scheduler uses it makes this neccessary. 

    $ chmod +x nimbus-cloud-client-015/lib/workspace.sh

You'll need to point your Cloud Scheduler install to this client. Set the
workspace_path option to point there.

Be sure that you have a valid x509 proxy available before starting Cloud
Scheduler. You can create one with:

    $ nimbus-cloud-client-015/bin/grid-proxy-init.sh

## Configuration

There are two Cloud Scheduler configuration files.

### The general cloud scheduler configuration file

The general (or central) cloud scheduler configuration file contains fields for
defining cloud scheduler program functionality, including Condor job pool con-
figuration information, logging information, and default cloud resource config-
uration options. 

The cloud scheduler config file can be manually specified on the command line 
when the cloud scheduler is run via the -f option, or can be stored in the
following locations:
    ~/.cloudscheduler/cloud_scheduler.conf
    /etc/cloudscheduler/cloud_scheduler.conf

Note: the cloud scheduler will attempt first to get the general configuration
file from the command-line, then from the ~/... directory, and finally from the
/etc/... directory.

### The cloud resource configuration file

The cloud resource configuration file contains information on the cloud-enabled
clusters that the cloud scheduler will use as resources. Clusters in this con-
figuration file will be used by the cloud scheduler to create and manage VMs.
See the cloud_resources.conf file for an explanation of cluster configuration parameters.

The cloud resource config file can be specified on the command-line with the
-c option. If the cloud resource config file is not specified on the command
line, it will be taken from the location given in the cloud_resource_config
field of the cloud_scheduler.conf file.

## Init Script
There is a cloud scheduler init script at scripts/cloud_scheduler. To install
it on systems with System V style init scripts, you can do so with:

    # cp scripts/cloud_scheduler /etc/init.d/

Start it with:

    # /etc/init.d/cloud_scheduler start

On Red Hat-like systems you can enable it to run at boot with:

    # chkconfig cloud_scheduler on

## Configuring a VM for EC2 / Eucalyptus

The way Cloud Scheduler manipulates Condor to connect to the correct central
manager is by writing files which are read by the Condor init script to
configure itself. Nimbus supports this out of the box, but EC2 requires a 
helper script to accomplish this. This section explains how to install it.

0. Install the EC2 Context Helper script to your machine. This is a part of the
   Cloud Scheduler release tarball, and is in the scripts/ec2contexthelper/
   directory.

1. Switch to the ec2contexthelper directory, and run setup.py

    # cd scripts/ec2contexthelper/
    # python setup.py install
    # which contexthelper
    /usr/bin/contexthelper

2. Enable the init script.

    # chkconfig context on

## Job Submission

Submitting a job for use with Cloud Scheduler is very similar to submitting a
job for use with a regular Condor Scheduler. It would be helpful to read
through Chapter 2 of the Condor Manual for help on submitting jobs to Condor.

Jobs meant to be run by VMs started by Cloud Scheduler need a few extra
parameters to work properly. These are: (Required parameters are highlighted)

* *Requirements = VMType =?= “your.vm.type”* :  The type of VM that the job must run on. This is a custom attribute of the VM advertised to the Condor central manager. It should be specified on the VM’s condor_config or condor_config.local file.
* *VMLoc* or *VMAMI* : The URL (for Nimbus) or AMI (for EC2-like clusters) of the image required for the job to run
* VMCPUArch : The CPU architecture that the job requires. x86 or x86_64. Defaults to x86.
* VMCPUCores : The number of CPU cores for the VM. Defaults to 1.
* VMStorage : The amount of scratch storage space the job requires. (Currently ignored on EC2-like Clusters)
* VMMem : The amount of RAM that the VM requires.
* VMNetwork : The type of networking required for your VM. Only used with Nimbus. Corresponds to Nimbus’s network pool.

### A Sample Job

    # Regular Condor Attributes
    Universe   = vanilla
    Executable = script.sh
    Arguments  = one two three
    Log        = script.log
    Output     = script.out
    Error      = script.error
    should_transfer_files = YES
    when_to_transfer_output = ON_EXIT
    # 
    # Cloud Scheduler Attributes
    Requirements = VMType =?= "vm.for.script"
    +VMLoc         = "http://repository.tld/your.vm.img.gz"
    +VMAMI = "ami-dfasfds"
    +VMCPUArch     = "x86"
    +VMCPUCores    = "1"
    +VMNetwork     = "private"
    +VMMem         = "512"
    +VMStorage     = "20"
    Queue

## License

This program is free software; you can redistribute it and/or modify
it under the terms of either:

a) the GNU General Public License as published by the Free
Software Foundation; either version 3, or (at your option) any
later version, or

b) the Apache v2 License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See either
the GNU General Public License or the Apache v2 License for more details.

You should have received a copy of the Apache v2 License with this
software, in the file named "LICENSE".

You should also have received a copy of the GNU General Public License
along with this program in the file named "COPYING". If not, write to the
Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, 
Boston, MA 02110-1301, USA or visit their web page on the internet at
http://www.gnu.org/copyleft/gpl.html.


