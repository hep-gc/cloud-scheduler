# Cloud Scheduler 1.12 README

## Introduction
Cloud Scheduler: Automatically boot VMs for your HTC jobs

Cloud Scheduler manages virtual machines on clouds configured with OpenStack,
Google Compute Engine, or Amazon EC2 to create an environment for HTC batch job execution.
Users submit their jobs to a Condor job queue, and Cloud Scheduler boots VMs to
suit those jobs. 

For more documentation on Cloud Scheduler, please refer to:

-  [Cloud Scheduler Wiki](http://wiki.github.com/hep-gc/cloud-scheduler)

## Prerequisites

* A working Condor 7.5.x or later install (details below)
* [Python 2.6+](http://www.python.org/)
* [boto](http://code.google.com/p/boto/) for using EC2 API clouds (Amazon, OpenStack)
* [web.py](http://webpy.org/) for admin and status REST servers
* [requests](https://github.com/kennethreitz/requests) for admin and status REST clients
* [OpenStack novaclient](https://pypi.python.org/pypi/python-novaclient/) and [OpenStack keystoneclient](https://pypi.python.org/pypi/python-keystoneclient/) for using the native OpenStack APIs
* [Google API Python Client](pip install --upgrade google-api-python-client) to use Google Compute Engine
* [Azure Service Management](https://pypi.python.org/pypi/azure-servicemanagement-legacy/) to use Microsoft Azure
* redhat-lsb 

## Optional Prerequisites

* [Guppy](http://guppy-pe.sourceforge.net/) -- Used for memory usage info.

## Basic Steps to get Jobs Running via Cloud Scheduler

1. Install Prerequiste libraries
2. Install Cloud Scheduler & Condor
3. Configure Condor and Cloud Scheduler
4. Setup a VM Image with Condor installed & CS Condor Scripts
5. Add the Required CS Attributes to a job submission file
6. Start CS and Submit job(s)

### Quick Start for People Who Think They Know What They're Doing

    # pip install cloud-scheduler

* This will install the latest master release, latest dev release available through github

### Special help for RHEL 5

Since Cloud Scheduler requires Python 2.6+, and we recognize that RHEL 5 comes
with and requires Python 2.4, here's a quick guide to getting Python 
installed on those systems:

Python 2.6 may be in the repos depending on your version(5.5+):

    $ yum install python26 python26-distribute

For Python 2.7:

Install the tools we need to build Python and its modules:

    # yum install gcc gdbm-devel readline-devel ncurses-devel zlib-devel \
      bzip2-devel sqlite-devel db4-devel openssl-devel tk-devel \
      bluez-libs-devel libxslt libxslt-devel libxml2-devel libxml2

Download and compile Python 2.7.1:

    $ VERSION=2.7.1
    $ mkdir /tmp/src 
    $ cd /tmp/src/
    $ wget http://python.org/ftp/python/$VERSION/Python-$VERSION.tar.bz2
    $ tar xjf Python-$VERSION.tar.bz2
    $ rm Python-$VERSION.tar.bz2
    $ cd Python-$VERSION 
    $ ./configure
    $ make
    $ sudo make altinstall

Now we need to install Python setuputils:

    $ cd /tmp/src
    $ wget http://pypi.python.org/packages/2.7/s/setuptools/setuptools-0.6c11-py2.7.egg
    $ sudo sh setuptools-0.6c11-py2.7.egg

Now install pip to install the rest of our dependencies:

    $ sudo easy_install-2.7 pip

And the rest of our dependencies:
 
    $ sudo pip-2.7 install cloud-scheduler

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

And Cloud Scheduler and its dependencies:

    # pip install cloud-scheduler

## Install without pip
To install without using pip:

Download the zip from github
	
	# wget https://github.com/hep-gc/cloud-scheduler/archive/master.zip
	# unzip master.zip
	# cd cloud_scheduler
   	# python setup.py install

## Condor Install
Cloud Scheduler works with [Condor](http://www.cs.wisc.edu/condor/), which needs
to be installed and able to manage resources. You must install it on the same
machine that runs Cloud Scheduler.

We recommend the following settings, especially if you're planning on
using Condor CCB:

    UPDATE_COLLECTOR_WITH_TCP=True
    COLLECTOR_SOCKET_CACHE_SIZE=10000
    COLLECTOR.MAX_FILE_DESCRIPTORS = 10000


We have also placed an example Condor config in scripts/condor/manager

Make sure you can run condor_status and condor_q, and make sure your
[HOST]ALLOW_WRITE will permit the VMs you will start to add themselves to your Condor
Pool.

## Preparing VM Images

The VM images you would like to run jobs with need to be prepared to join your
Condor pool. Cloud Scheduler will do most of the heavy lifting for you, but at
the very least, you need to install Condor, and configure it as a worker that
will join your Condor pool. The easiest way to do this is use the example
configuration (at least as inspiration) from scripts/condor/worker/ . You'll
want to put these in your /etc/condor directory. You will probably also want to
use our custom Condor init script. This does things like set up an appropriate
environment for when Condor is started with private networking only, when
started on EC2, and also will automatically point your node to your Condor
Pool. When using the custom init script and doing offline testing of the VM
image, ensure you place the central_manager file from scripts/condor/worker into
/etc/condor as the init script will read the value of the CONDOR_HOST from
this file.

## Configuration

### cloud_scheduler.conf

The Cloud Scheduler configuration file allows you to configure most of its 
functionality, and you'll need to open it up to get a usable installation.
All of its options are described inline in the example configuration file
cloud_scheduler.conf, which is included with Cloud Scheduler. 

By default, the Cloud Scheduler setup script installs its configuration files
to /etc/cloudscheduler/, but you can manually select a different configuration
by running cloud_scheduler with the -f option. If you're running as a non-root
user, Cloud Scheduler will also check for config files in ~/.cloud_scheduler/

Cloud Scheduler checks for config files in the following order, and will use the first one it finds:

    [config specified with the -f option]
    ~/.cloudscheduler/cloud_scheduler.conf
    /etc/cloudscheduler/cloud_scheduler.conf
    /usr/local/share/cloud-scheduler/cloud_scheduler.conf

#### cloud init files

Cloud Scheduler has a default cloud config file included with the installation, it should be located
in /usr/local/share/cloud-scheduler/default.yaml if you've installed from pip. The location can be set
in the cloud_scheduler.conf file. Additional customization can be done by users by setting an AMIConfig list
of cloud init files along with their jobs.

### cloud_resources.conf

The cloud resource configuration file, cloud_resources.conf, is where you
define which clouds Cloud Scheduler should use for starting VMs. You'll specify
how many VMs you want to boot on each cloud, and what it's capabilities are.
The best way to get familiar with this file is to open up the sample
cloud_resources.conf file, where all of its configuration options, and a sample
configuration are included.

Like cloud_scheduler.conf, the Cloud Scheduler setup script installs this file
in /etc/cloudscheduler/, but you can manually select a different configuration
by running cloud_scheduler with the -c option. You can also specify the
location of this file with the cloud_resource_config option in the
cloud_scheduler.conf file.


## Init Script
There is a cloud scheduler init script at scripts/cloud_scheduler. To install
it on systems with System V style init scripts, you can do so with:

    # cp scripts/cloud_scheduler /etc/init.d/
    

if you've installed from pip

	# cp /usr/local/share/cloud-scheduler/cloud_scheduler.init.d /etc/init.d/cloud_scheduler
	# cp /usr/local/share/cloud-scheduler/cloud_scheduler.sysconf /etc/sysconfig/cloud_scheduler

Start it with:

    # /etc/init.d/cloud_scheduler start

On Red Hat-like systems you can enable it to run at boot with:

    # chkconfig cloud_scheduler on

NOTE: If you've used a non-default Python, you may need to set the PYTHON variable
in the init script. If you've installed in a non-default location, you may need to 
set your EXECUTABLEPATH variable.

To Stop Cloud Scheduler without it shutting down VMs (Current VMs will be saved
to the persistence file specified in the cloud_scheduler.conf and get reloaded 
when Cloud Scheduler is started - Note that loading the VMs from persistence may 
take awhile)

    # /etc/init.d/cloud_scheduler forcekill

To Reload the cloud_resources.conf and cloud_scheduler.conf with killing VMs

    # /etc/init.d/cloud_scheduler quickrestart



## Job Submission

Submitting a job for use with Cloud Scheduleris very similar to submitting a
job for use with a regular Condor Scheduler. It would be helpful to read
through Chapter 2 of the Condor Manual for help on submitting jobs to Condor.

Jobs meant to be run by VMs started by Cloud Scheduler need a few extra
parameters to work properly. These are: (Required parameters are highlighted)

* *Requirements = VMType =?= “your.vm.type”* :  The type of VM that the job must run on. This is a custom attribute of the VM advertised to the Condor central manager. It should be specified on the VM’s condor_config or condor_config.local file.
* VMAMI : The AMI (for EC2-like clusters) or image name of the image required for the job to run
* VMCPUCores : The number of CPU cores for the VM. Defaults to 1.
* VMStorage : The amount of scratch storage space the job requires. (Currently ignored on EC2-like Clusters)
* VMMem : The amount of RAM that the VM requires.
* VMNetwork : The network group used for your VM. Only used with OpenStackNative if default network not available.
* VMInstanceType : The EC2 instance type of the VM requested. Only used with EC2 clouds like Amazon.
* VMMaximumPrice : The maximum price in cents per hour for a VM (EC2 Only)
* VMKeepAlive : Number of minutes a VM should stay up after job finishes
* VMHighPriority : 1 (Optional flag) Indicates a high priority job to Cloud Scheduler – high priority job support can be enabled in the cloud_scheduler.conf
* TargetClouds : A comma separated list of names of clouds that you would like your job to use
* CSMyProxyServer : The hostname of the myproxy server you’d like to use for credential renewal
* CSMyProxyCredsName : The name of your myproxy credentials
* VMJobPerCore : bool – Assigns multiple slots to a multi-core VM

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
    +VMCPUCores    = "1"
    +VMNetwork     = "private"
    +VMMem         = "512"
    +VMStorage     = "20"
    Queue

## Using Proxy Certificates

For a more secure, but more complicated setup allowing your users to use their
own proxy certificates, there is a guide on the heprc wiki:

https://wiki.heprc.uvic.ca/twiki/bin/view/Main/CsGsiSupport

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


