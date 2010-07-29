#!/usr/bin/env python
# utilities.py - utility functions not specific to cloud scheduler

import os
import sys
import socket
import logging
import ConfigParser
from urlparse import urlparse
import cloudscheduler.config as config

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

<<<<<<< HEAD
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
=======
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

>>>>>>> Banning jobs from resources related.
