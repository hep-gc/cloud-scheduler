#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

# Copyright (C) 2009 University of Victoria
# You may distribute under the terms of either the GNU General Public
# License or the Apache v2 License, as specified in the README file.

## Auth.: Patrick Armstrong

import os
import sys
import ConfigParser

# Cloud Scheduler Options Module.

# Set default values
condor_webservice_url = "http://localhost:8080"
condor_collector_url = "http://localhost:9618"
cloud_resource_config = None
log_level = "INFO"
log_location = None
log_stdout = False
log_max_size = None
info_server_port = 8111


# setup will look for a configuration file specified on the command line,
# or in ~/.cloudscheduler.conf or /etc/cloudscheduler.conf
def setup(path=None):

    global condor_webservice_url
    global condor_collector_url
    global cloud_resource_config
    global info_server_port
    global log_level
    global log_location
    global log_stdout
    global log_max_size

    homedir = os.path.expanduser('~')

    # Find config file
    if not path:
        if os.path.exists(homedir + "/.cloudscheduler/cloud_scheduler.conf"):
            path = homedir + "/.cloudscheduler/cloud_scheduler.conf"
        elif os.path.exists("/etc/cloudscheduler/cloud_scheduler.conf"):
            path = "/etc/cloudscheduler/cloud_scheduler.conf"
        else:
            print >> sys.stderr, "Configuration file problem: There doesn't " \
                  "seem to be a configuration file. " \
                  "You can specify one with the --config-file parameter, " \
                  "or put one in ~/.cloudscheduler/cloud_scheduler.conf or "\
                  "/etc/cloudscheduler/cloud_scheduler.conf"
            sys.exit(1)

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

    if config_file.has_option("global", "condor_webservice_url"):
        condor_webservice_url = config_file.get("global",
                                                "condor_webservice_url")
    if config_file.has_option("global", "condor_collector_url"):
        condor_collector_url = config_file.get("global",
                                                "condor_collector_url")
    if config_file.has_option("global", "cloud_resource_config"):
        cloud_resource_config = config_file.get("global",
                                                "cloud_resource_config")

    if config_file.has_option("global", "info_server_port"):
        try:
            info_server_port = config_file.getint("global", "info_server_port")
        except ValueError:
            print "Configuration file problem: info_server_port must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("logging", "log_level"):
        log_level = config_file.get("logging", "log_level")

    if config_file.has_option("logging", "log_location"):
        log_location = os.path.expanduser(config_file.get("logging", "log_location"))

    if config_file.has_option("logging", "log_stdout"):
        log_stdout = config_file.getboolean("logging", "log_stdout")

    if config_file.has_option("logging", "log_max_size"):
        try:
            log_max_size = config_file.getint("logging", "log_max_size")
        except ValueError:
            print "Configuration file problem: log_max_size must be an " \
                  "integer value in bytes."
            sys.exit(1)
