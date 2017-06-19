#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

import os
import sys
import ConfigParser

# Cloud Scheduler Status Options Module.

# Set default values
info_server_port = 8111


def setup(path=None):
    """Setup cloudscheduler using config file.
       setup will look for a configuration file specified on the command line,
       or in ~/.cloudscheduler.conf or /etc/cloudscheduler.conf
    """

    global info_server_port

    homedir = os.path.expanduser('~')

    # Find config file
    if not path:
        if os.path.exists(homedir + "/.cloudscheduler/cloud_scheduler_status.conf"):
            path = homedir + "/.cloudscheduler/cloud_scheduler_status.conf"
        elif os.path.exists("/etc/cloudscheduler/cloud_scheduler_status.conf"):
            path = "/etc/cloudscheduler/cloud_scheduler_status.conf"
        elif os.path.exists("/usr/local/share/cloud-scheduler/cloud_scheduler_status.conf"):
            path = "/usr/local/share/cloud-scheduler/cloud_scheduler_status.conf"
        else:
            print >> sys.stderr, "Configuration file problem: There doesn't " \
                  "seem to be a configuration file. " \
                  "You can specify one with the --config-file parameter, " \
                  "or put one in ~/.cloudscheduler/cloud_scheduler_status.conf or "\
                  "/etc/cloudscheduler/cloud_scheduler_status.conf "\
                  "Running in full default value mode."
            return

    # Read config file
    config_file = ConfigParser.ConfigParser()
    try:
        config_file.read(path)
    except IOError:
        print >> sys.stderr, "Configuration file problem: There was a " \
              "problem reading %s. Check that it is readable," \
              "and that it exists. " % path
        raise
    except ConfigParser.ParsingError:
        print >> sys.stderr, "Configuration file problem: Couldn't " \
              "parse your file. Check for spaces before or after variables."
        raise
    except:
        print "Configuration file problem: There is something wrong with " \
              "your config file."
        raise

    if config_file.has_option("global", "info_server_port"):
        try:
            info_server_port = config_file.getint("global", "info_server_port")
        except ValueError:
            print "Configuration file problem: info_server_port must be an " \
                  "integer value."
            sys.exit(1)
