#!/usr/bin/env python
# utilities.py - utility functions not specific to cloud scheduler

import os
import sys
import socket
import logging
import ConfigParser
import subprocess
import time
import errno
from urlparse import urlparse
from datetime import datetime
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

def splitnstrip(sep, str):
    return [x.strip() for x in str.split(sep)];


def get_globus_path(executable="grid-proxy-init"):
    """
    Finds the path for Globus executables on the machine. 

    If GLOBUS_LOCATION is set, and executable exists, use that,
    otherwise, check the path to see if its in there,
    otherwise, raise an exception.
    """

    try:
        os.environ["GLOBUS_LOCATION"]
        retcode = subprocess.call("$GLOBUS_LOCATION/bin/%s -help" % executable, shell=True, 
                                  stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)

        if retcode != 0:
            raise EnvironmentError(retcode, "GLOBUS_LOCATION is in your environment, but unable to call '%s'" % executable)
        else:
            return os.environ["GLOBUS_LOCATION"] + "/bin/"

    except:
        retcode = subprocess.call("%s -help" % executable, shell=True, 
                                  stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)
        if retcode == 127:
            raise EnvironmentError(retcode, "'%s' is not in your PATH" % executable)
        elif retcode != 0:
            raise EnvironmentError(retcode, "'%s' is in your PATH, but it returned '%s'" % (executable, retcode))
        else:
            return ""



# This utility function will extract the subject DN from an x509
# certificate.
# It requires the openssl package to be installed.
def get_cert_DN(cert_file_path):
    log = get_cloudscheduler_logger()
    openssl_cmd = ['/usr/bin/openssl', 'x509', '-in', cert_file_path, '-subject', '-noout']
    try:
        dn = subprocess.Popen(openssl_cmd, stdout=subprocess.PIPE).communicate()[0].strip()[9:]
        return dn
    except:
        log.exception("Problem getting cert DN")
        return None

# This utility function will extract the certificate's identity from an x509
# proxy certificate.
# The identity is the subject DN of the first non-impersonation proxy 
#  (which is usually an end-entity certificate) in the proxy certificate chain. 
# It requires the openssl command
#
# TODO: This version only does a simple parsing of the proxy's subject DN.
#       We should do a better job by actually traversing the certificate
#       chain.
def get_proxy_identity(proxy_file_path):
    dn = get_cert_DN(proxy_file_path)
    if dn.count('/CN=') > 1:
        return dn[0:dn.find('/CN=', dn.find('/CN=')+1)]
    else:
        return dn
    
# This utility function will extract the expiry time from an x509
# certificate.
# It requires the openssl package to be installed.
# Returns a datetime instance, with UTC time.
# Returns None on error
def get_cert_expiry_time(cert_file_path):
    log = get_cloudscheduler_logger()
    openssl_cmd = ['/usr/bin/openssl', 'x509', '-in', cert_file_path, '-enddate', '-noout']
    try:
        stdout_stderr = subprocess.Popen(openssl_cmd, stdout=subprocess.PIPE).communicate()
        datetime_string = stdout_stderr[0].strip().split('=')[1]
        expiry_time = datetime.strptime(datetime_string, '%b %d %H:%M:%S %Y %Z')
        return expiry_time
    except:
        log.exception("Problem getting certificate time")
        return None
    
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

    def length(self):
        length = 0
        for x in self.data:
            if x != None:
                length += 1
        return length

class ErrTrackQueue(CircleQueue):
    def __init__(self, name):
        CircleQueue.__init__(self, config.ban_min_track)
        self.name = name

    def dist_true(self):
        tc = 0
        for x in self.data:
            if x:
                tc += 1
        return (float(tc) / float(len(self.data))) if len(self.data) > 0 else 0

    def dist_false(self):
        return 1.0 - self.dist_true()
    
class JobRunTrackQueue(CircleQueue):
    def __init__(self, name):
        CircleQueue.__init__(self, 10)
        self.name = name
        self.avg = 0
        
    def average(self):
        total = 0
        for x in self.data:
            if x:
                total += x
        if self.length() > 0:
            self.avg = total / self.length()
        return self.avg

# Timeout feature for subprocess.Popen - polls the process for timeout seconds waiting for it to complete
# If the process has exited return False (process did not timeout)
# Else if the process times out attempt to terminate the process or kill if terminate fails and return True (process timed out)
def check_popen_timeout(process, timeout=180):
    log = get_cloudscheduler_logger()
    ret = True
    while timeout > 0:
        if process.poll() != None:
            ret = False
            break
        time.sleep(1)
        timeout -= 1
    if timeout == 0:
        log.debug("subprocess timed out - attempting to terminate")
        try:
            process.terminate()
        except OSError, e:
            if e.errno != errno.ESRCH:
                raise
        time.sleep(2) # give OS a chance to terminate
        if process.poll() == None: # Did not terminate
            log.debug("terminate() failed using kill()")
            try:
                process.kill()
            except OSError, e:
                if e.errno != errno.ESRCH:
                    raise
    return ret
