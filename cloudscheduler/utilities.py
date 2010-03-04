#!/usr/bin/env python
# utilities.py - utility functions not specific to cloud scheduler

import os
import sys
import logging
from urlparse import urlparse

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
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL,}
class NullHandler(logging.Handler):
    def emit(self, record):
        pass

def get_hostname_from_url(url):
    return urlparse(url)[1].split(":")[0]
