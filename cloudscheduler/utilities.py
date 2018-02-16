#!/usr/bin/env python
"""
utilities.py - utility functions not specific to cloud scheduler
"""

import os
import sys
import socket
import logging
import subprocess
import time
import gzip
import errno
from urlparse import urlparse
from datetime import datetime
from cStringIO import StringIO
from collections import deque
try:
    from OpenSSL import crypto
except ImportError:
    pass

def determine_path():
    """Borrowed from wxglade.py"""
    try:
        root = __file__
        if os.path.islink(root):
            root = os.path.realpath(root)
        return os.path.dirname(os.path.abspath(root))
    except:
        print "I'm sorry, but something is wrong."
        print "There is no __file__ variable. Please contact the author."
        sys.exit()


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
    """Gets a reference to the 'cloudscheduler' log handle."""
    logging.VERBOSE = LEVELS["VERBOSE"]
    logging.addLevelName(logging.VERBOSE, "VERBOSE")
    log = logging.getLogger("cloudscheduler")
    setattr(log, "verbose", lambda *args: log.log(logging.VERBOSE, *args))
    log.addHandler(NullHandler())

    return log


def get_hostname_from_url(url):
    """Return the hostname parsed from a full url."""
    return urlparse(url)[1].split(":")[0]


def get_or_none(config, section, value):
    """Return the value of a config option if it exists, none otherwise."""
    if config.has_option(section, value):
        return config.get(section, value)
    else:
        return None


def splitnstrip(sep, val):
    """Return a list of items trimed of excess whitespace
    from a string(typically comma separated)."""
    return [x.strip() for x in val.split(sep)]


def get_globus_path(executable="grid-proxy-init"):
    """
    Finds the path for Globus executables on the machine.

    If GLOBUS_LOCATION is set, and executable exists, use that,
    otherwise, check the path to see if its in there,
    otherwise, raise an exception.
    """

    try:
        if os.environ["GLOBUS_LOCATION"]:
            retcode = subprocess.call("$GLOBUS_LOCATION/bin/%s -help" % executable, shell=True,
                                  stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)
            if retcode != 0:
                raise EnvironmentError(retcode, "GLOBUS_LOCATION is in your environment,\
                                   but unable to call '%s'" % executable)
            else:
                return os.environ["GLOBUS_LOCATION"] + "/bin/"

    except:
        retcode = subprocess.call("%s -help" % executable, shell=True,
                                  stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)
        if retcode == 127:
            raise EnvironmentError(retcode, "'%s' is not in your PATH" % executable)
        elif retcode != 0:
            raise EnvironmentError(retcode, "'%s' is in your PATH, but it \
                                   returned '%s'" % (executable, retcode))
        else:
            return ""


def get_cert_DN(cert_file_path):
    """This utility function will extract the subject DN from an x509 certificate.

       Note that this method is affected by the use_pyopenssl config variable.
       If use_pyopenssl is True, then the pyopenssl librairies will be used to
       extract the certificate subject.  Else a openssl subprocess will be
       forked to extract the info out of the certificate.

       It requires the openssl package to be installed.

    """
    if config_val.getboolean('global', 'use_pyopenssl'):
        try:
            cert_file = open(cert_file_path, 'r')
            cert_data = cert_file.read()
            cert = crypto.load_certificate(crypto.FILETYPE_PEM, cert_data)
            cert_file.close()
            return cert.get_subject()
        except:
            log = get_cloudscheduler_logger()
            log.exception('Error extracting cert subject using pyopenssl.')
            return None
    else:
        openssl_cmd = [config_val.get('global', 'openssl_path'), 'x509', '-in', cert_file_path, '-subject', '-noout']
        try:
            sub_dn = subprocess.Popen(openssl_cmd, stdout=subprocess.PIPE).communicate()[0].strip()[9:]
            return sub_dn
        except:
            log = get_cloudscheduler_logger()
            log.exception('Error extracting cert subject using openssl.')
            return None


def get_cert_expiry_time(cert_file_path):
    """This utility function will extract the expiry time from an x509 certificate.

    It requires the openssl package to be installed.
    Returns a datetime instance, with UTC time (naive).

    Note that this method is affected by the use_pyopenssl config variable.
    If use_pyopenssl is True, then the pyopenssl librairies will be used to extract
    the certificate expiry time.  Else a openssl subprocess will be forked to
    extract the info out of the certificate.

    Returns None on error

    """
    if config_val.getboolean('global', 'use_pyopenssl'):
        try:
            cert_file = open(cert_file_path, 'r')
            cert_data = cert_file.read()
            cert = crypto.load_certificate(crypto.FILETYPE_PEM, cert_data)
            cert_file.close()
            # Note that the following time format string ends with 'Z'.
            # This is not a typo (i.e., we need 'Z', not %Z)
            return datetime.strptime(cert.get_notAfter(), '%Y%m%d%H%M%SZ')
        except:
            log = get_cloudscheduler_logger()
            log.exception('Error extracting cert expiry time using pyopenssl.')
            return None
    else:
        openssl_cmd = [config_val.get('global', 'openssl_path'), 'x509', '-in', cert_file_path, '-enddate', '-noout']
        try:
            stdout_stderr = subprocess.Popen(openssl_cmd, stdout=subprocess.PIPE).communicate()
            datetime_string = stdout_stderr[0].strip().split('=')[1]
            expiry_time = datetime.strptime(datetime_string, '%b %d %H:%M:%S %Y %Z')
            return expiry_time
        except:
            log = get_cloudscheduler_logger()
            log.exception('Error extracting cert expiry time using openssl.')
            return None


def match_host_with_condor_host(hostname, condor_hostname):
    """
    match_host_with_condor_host -- determine if hostname matches condor's hostname

    These can look like:

    [slotx@](xxx.xxx.xxx.xxx|host.name)

    returns True if matching, and false if not.
    """
    if hostname is None:
        return False
    # Strip off slotx@
    try:
        condor_hostname_parts = condor_hostname.split("@")
        condor_hostname_noslot = condor_hostname_parts[1]
    except:
        condor_hostname = condor_hostname
        condor_hostname_noslot = condor_hostname

    if hostname == condor_hostname_noslot:
        return True

    # Check if it's an IP address
    try:
        # If it's an IP address, and it doesn't match to this point,
        # it'll never match.
        socket.inet_aton(condor_hostname)
        return False
    except:
        pass
    # If it's a hostname, let's try to match the first bit of the
    # name, otherwise, it'll never match
    condor_hostname = condor_hostname_noslot.split(".")[0]
    hostname = hostname.split(".")[0]

    if hostname == condor_hostname:
        return True

    return False


def match_host_with_condor_host_master(hostname, condor_hostname):
    """
    match_host_with_condor_host -- determine if hostname matches condor's hostname

    These can look like:

    [slotx@](xxx.xxx.xxx.xxx|host.name)

    returns True if matching, and false if not.
    """
    if hostname is None:
        return False
    # Strip off slotx@
    try:
        condor_hostname_parts = condor_hostname.split("@")
        condor_hostname = condor_hostname_parts[1]
    except:
        condor_hostname = condor_hostname

    # Strip off slotx from hostname
    try:
        hostname_parts = hostname.split("@")
        hostname = hostname_parts[1]
    except:
        hostname = hostname

    if hostname == condor_hostname:
        return True

    # Check if it's an IP address
    try:
        # If it's an IP address, and it doesn't match to this point,
        # it'll never match.
        socket.inet_aton(condor_hostname)
        return False
    except:
        pass
    # If it's a hostname, let's try to match the first bit of the
    # name, otherwise, it'll never match
    condor_hostname_parts = condor_hostname.split(".")
    condor_hostname = condor_hostname_parts[0]

    hostname_parts = hostname.split(".")
    hostname = hostname_parts[0]

    if hostname == condor_hostname:
        return True

    return False


class ErrTrackQueue(object):
    """Error Tracking Queue - Keeps a True/False record of each VM Boot."""
    def __init__(self, name):
        """Initializes new queue with the configured length."""
        self.data = deque(maxlen=10)
        self.name = name

    def dist_true(self):
        """Calculate the distribution of True(succuessful) starts."""
        truec = sum(self.data)
        return (float(truec) / float(len(self.data))) if len(self.data) > 0 else 0

    def dist_false(self):
        """Calculate the distribution of False(failed) starts."""
        return 1.0 - self.dist_true()


class JobRunTrackQueue(object):
    """Job Run-[time] Tracking Queue. Keeps a list of job runtimes for  stats purposes."""
    def __init__(self, name):
        """Initizlizes new queue, of length 10."""
        self.data = deque(maxlen=10)
        self.name = name
        self.avg = 0

    def average(self):
        """"Returns the average run-time of jobs in the queue."""
        total = sum(self.data)
        if len(self.data) > 0:
            self.avg = total / len(self.data)
        return self.avg


def check_popen_timeout(process, timeout=180):
    """ Timeout feature for subprocess.Popen -
        polls the process for timeout seconds waiting for it to complete
        If the process has exited return False (process did not timeout)
        Else if the process times out attempt to terminate the process
        or kill if terminate fails and return True (process timed out)
    """
    log = get_cloudscheduler_logger()
    ret = True
    while timeout > 0:
        if process.poll() != None:
            ret = False
            break
        time.sleep(1)
        timeout -= 1
    if timeout == 0:
        log.debug("subprocess timed out - attempting to terminate pid %s", process.pid)
        try:
            process.terminate()
        except OSError, e:
            if e.errno != errno.ESRCH:
                raise
        time.sleep(2) # give OS a chance to terminate
        if process.poll() is None: # Did not terminate
            log.debug("terminate() on pid %s failed using kill()", process.pid)
            try:
                process.kill()
            except OSError, e:
                if e.errno != errno.ESRCH:
                    raise
    return ret


def gzip_userdata(user_data):
    # Compress the user data to try and get under the limit
    if not user_data:
        return ""
    udbuf = StringIO()
    udf = gzip.GzipFile(mode='wb', fileobj=udbuf)
    try:
        udf.write(user_data)
    finally:
        udf.close()
    return udbuf.getvalue()

