#!/usr/bin/env python
# utilities.py - utility functions not specific to cloud scheduler

import os
import sys
import socket
import logging
import ConfigParser
import subprocess
from urlparse import urlparse
import config

def determine_path ():
    """Borrowed from wxglade.py"""
    try:
        root = __file__
        if os.path.islink (root):
            root = os.path.realpath (root)
        return os.path.dirname (os.path.abspath (root))
    except:
        print "I'm sorry, but something is wrong."
        print "There is no __file__ variable. Please contact the author."
        sys.exit ()

LEVELS = {'DEBUG': logging.DEBUG,
          'VERBOSE': logging.DEBUG-1,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL,}

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

def get_cloudscheduler_logger():
    logging.VERBOSE = LEVELS["VERBOSE"]
    logging.addLevelName(logging.VERBOSE, "VERBOSE")
    log = logging.getLogger("cloudscheduler")
    setattr(log, "verbose", lambda *args: log.log(logging.VERBOSE, *args))
    log.addHandler(NullHandler())

    return log


def get_hostname_from_url(url):
    return urlparse(url)[1].split(":")[0]

def get_or_none(config, section, value):
    if config.has_option(section, value):
        return config.get(section, value)
    else:
        return None

def myproxy_init(myproxy_server, myproxy_server_port, myproxy_creds_name):
    log = get_cloudscheduler_logger()
    job_proxy_file_path = None
    if myproxy_creds_name != None:
        log.debug("myproxy_creds_name: %s" % (myproxy_creds_name))
        if myproxy_server == None:
            log.warning("MyProxy credential name given but missing MyProxy server host. Defaulting to localhost")
            myproxy_server = "localhost"
            
        if myproxy_server_port == None:
            log.debug("No MyProxy server port given; using default port (7512)")
            myproxy_server_port = "7512"
            
        # Check to see if $GLOBUS_LOCATION is defined.
        if os.environ["GLOBUS_LOCATION"] == None:
            log.error("GLOBUS_LOCATION not set.  Please set GLOBUS_LOCATION.")
            return None

        # Check to see of myproxy-logon is present in globus installation
        if not os.path.exists(os.environ["GLOBUS_LOCATION"] + "/bin/myproxy-logon"):
            log.error("MyProxy credentials specified but $GLOBUS_LOCATION/bin/myproxy-logon not found.  Make sure you have a valid MyProxy client installation on your system.")
            return None

        job_proxy_file_path = "/tmp/" + myproxy_creds_name + ".cs_x509proxy"
        log.debug("job_proxy_file_path: %s" % (job_proxy_file_path))
        myproxy_logon_cmd = '. $GLOBUS_LOCATION/etc/globus-user-env.sh && $GLOBUS_LOCATION/bin/myproxy-logon -s %s -p %s -l %s -o %s -n' % (myproxy_server, myproxy_server_port, myproxy_creds_name, job_proxy_file_path)
        log.debug('myproxy-logon command: [%s]' % (myproxy_logon_cmd))
        log.debug('Invoking myproxy-logon command...')
        myproxy_logon_process = subprocess.Popen(myproxy_logon_cmd, shell=True)
        myproxy_logon_process.wait()
        log.debug('myproxy-logon command returned %d' % (myproxy_logon_process.returncode))
        if myproxy_logon_process.returncode != 0:
            log.error("Error fetching proxy from MyProxy server.  Aborting vm creation...")
            return None

    
    return job_proxy_file_path


# This utility function will extract the subject DN from an x509
# certificate.
# It requires the openssl package to the installed.
def get_cert_DN(cert_file_path):
    log = get_cloudscheduler_logger()
    openssl_cmd = ['/usr/bin/openssl', 'x509', '-in', cert_file_path, '-subject', '-noout']
    return subprocess.Popen(openssl_cmd, stdout=subprocess.PIPE).communicate()[0].strip()[9:]
    
def match_host_with_condor_host(hostname, condor_hostname):
    """
    match_host_with_condor_host -- determine if hostname matches condor's hostname

    These can look like:

    [slotx@](xxx.xxx.xxx.xxx|host.name)

    returns True if matching, and false if not.
    """

    # Strip off slotx@
    try:
        condor_hostname_parts = condor_hostname.split("@")
        condor_hostname = condor_hostname_parts[1]
    except:
        condor_hostname = condor_hostname

    if hostname == condor_hostname:
        return True

    # Check if it's an IP address
    try:
        # If it's an IP address, and it doesn't match to this point,
        # it'll never match.
        socket.inet_aton(condor_hostname)
        return False
    except:
        # If it's a hostname, let's try to match the first bit of the
        # name, otherwise, it'll never match
        condor_hostname_parts = condor_hostname.split(".")
        condor_hostname = condor_hostname_parts[0]

        hostname_parts = hostname.split(".")
        hostname = hostname_parts[0]

        if hostname == condor_hostname:
            return True

    return False

class CircleQueue():
    def __init__(self, length):
        self.data = [None for x in range(0, length)]

    def append(self, x):
        self.data.pop(0)
        self.data.append(x)

    def get(self):
        return self.data

    def clear(self):
        self.data = [None for x in range(0, len(self.data))]

    def min_use(self):
        min_use = True
        if self.data[0] == None:
            min_use = False
        return min_use

class ErrTrackQueue(CircleQueue):
    def __init__(self, name):
        CircleQueue.__init__(self, config.ban_min_track)
        self.name = name

    def dist_true(self):
        tc = 0
        for x in self.data:
            if x:
                tc += 1
        return float(tc) / float(len(self.data))

    def dist_false(self):
        return 1.0 - self.dist_true()
