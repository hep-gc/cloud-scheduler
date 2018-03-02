#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:
"""
Reads the config file from either a specified path
or checks in default locations and creates a configparser
object. Uses a specified set of defaults in default.cfg
and checks for invalid values in read in config files
"""
import os
import sys
import ConfigParser

import utilities

config_options = ConfigParser.ConfigParser()

# Cloud Scheduler Options Module.
def setup(path=None):
    """Setup cloudscheduler using config file.
       setup will look fora configuration file specified on the command line,
       or in ~/.cloudscheduler.onf or /etc/cloudscheduler.conf
    """
    homedir = os.path.expanduser('~')

    #Find the default file
    if os.path.exists(homedir + '/.cloudscheduler/defaults.cfg'):
        def_path = homedir + '/.cloudscheduler/defautls.cfg'
    elif os.path.exists('/etc/cloudscheduler/defaults.cfg'):
        def_path = '/etc/cloudscheduler/defaults.cfg'
    elif os.path.exists('/usr/local/share/cloud-scheduler/defaults.cfg'):
        def_path = '/usr/local/share/cloud-scheduler/defaults.cfg'
    else:
        print "Error: Can't find default configuration values"
        sys.exit(1)

    #Create defaults dictionary
    default = {}
    
    with open(def_path) as file_def:
        for line in file_def:
            base = line.strip().split('=')
            if len(base) == 1:
                default[base[0].strip()] = None
            else:
                base[1] = base[1].strip()
                default[base[0].strip()] = base[1].strip('"')

    # Find config file
    if not path:
        if os.path.exists(homedir + "/.cloudscheduler/cloud_scheduler.conf"):
            path = homedir + "/.cloudscheduler/cloud_scheduler.conf"
        elif os.path.exists("/etc/cloudscheduler/cloud_scheduler.conf"):
            path = "/etc/cloudscheduler/cloud_scheduler.conf"
        elif os.path.exists("/usr/local/share/cloud-scheduler/cloud_scheduler.conf"):
            path = "/usr/local/share/cloud-scheduler/cloud_scheduler.conf"
        else:
            #print >> sys.stderr, "Configuration file problem: There doesn't " \
            #      "seem to be a configuration file. " \
            #      "You can specify one with the --config-file parameter, " \
            #      "or put one in ~/.cloudscheduler/cloud_scheduler.conf or "\
            #      "/etc/cloudscheduler/cloud_scheduler.conf "\
            #      "Running in full default value mode."
            return

    # Read config file
    config_file = ConfigParser.ConfigParser(default, allow_no_value=True)
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

    #Check that some options are in the correct format

    try:
        config_file.getint('global', 'vm_lifetime')
    except ValueError:
        print "Configuration file problem: vm_lifetime must be an int"
        sys.exit(1)

    try:
        config_file.getint('global', 'info_server_port')
    except ValueError:
        print "Configuration file problem: info_server_port must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'admin_server_port')
    except ValueError:
        print "Configuration file problem: admin_server_port must be an integer value"
        sys.exit(1)

    try:
        job_ban_timeout = config_file.getint('global', 'job_ban_timeout')
        if job_ban_timeout != 3600:
            job_ban_timeout = job_ban_timeout * 60
            config_file.set('global', 'job_ban_timeout', job_ban_timeout)
    except ValueError as e:
        print e
        print "Configuration file problem: job_ban_timeout must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'polling_error_threshold')
    except ValueError:
        print "Configuration file problem: polling_error_threshold must be an integer value"
        sys.exit(1)

    try:
        config_file.getfloat('global', 'ban_failrate_threshold')
    except ValueError:
        print "Configuration file problem: ban_failrate_threshold must be a float value"
        sys.exit(1)

    try:
        config_file.getint('global', 'ban_min_track')
    except ValueError:
        print "Configuration file problem: ban_min_track must be an integer value"
        sys.exit(1)

    try:
        condor_register_time_limit = config_file.getint('global', 'condor_register_time_limit')
        if condor_register_time_limit != 900:
            condor_register_time_limit = condor_register_time_limit * 60
            config_file.set('global', 'condor_register_time_limit', condor_register_time_limit)
    except ValueError:
        print "Configuration file problem: condor_register_time_limit must be an int value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'ban_tracking')
    except ValueError:
        print "Configuration file problem: ban_tracking must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'retire_before_lifetime')
    except ValueError:
        print "Configuration file problem: retire_before_lifetime must be a boolean value"
        sys.exit(1)

    try:
        retire_before_lifetime_factor = config_file.getfloat('global', 'retire_before_lifetime_factor')
        if retire_before_lifetime_factor < 1.0:
            print "Please use a float value (1.0, X] for the retire_before_lifetime_factor"
            sys.exit(1)
    except ValueError:
        print "Configuration file problem: retire_before_lifetime_factor must be a float value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'graceful_shutdown')
    except ValueError:
        print "Configuration file problem: graceful_shutdown must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'cleanup_missing_vms')
    except ValueError:
        print "Configuration file problem: cleanup_missing_vms must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'clean_shutdown_idle')
    except ValueError:
        print "Configuration file problem: clean_shutdown_idle must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'getclouds')
    except ValueError:
        print "Configuration file problem: getclouds must be a boolean value"
        sys.exit(1)

    try:
        memory_distribution_weight = config_file.getfloat('global', 'memory_distribution_weight')
        if memory_distribution_weight <= 0:
            print "Please use a float value (0, X] for the memory_distribution_weight"
            sys.exit()
    except ValueError:
        print "Configuration file problem: memory_distribution_weight must be a float value"
        sys.exit(1)

    try:
        cpu_distribution_weight = config_file.getfloat('global', 'cpu_distribution_weight')
        if cpu_distribution_weight <= 0:
            print "Please use a float value 90, X] for the cpu_distribution_weight"
            sys.exit()
    except ValueError:
        print "Configuration file problem: cpu_distribution_weight must be a float value"
        sys.exit(1)

    try:
        storage_distribution_weight = config_file.getfloat('global', 'storage_distribution_weight')
        if storage_distribution_weight <= 0:
            print "Please use a float value (0, X] for the storage_distribution_weight"
            sys.exit(1)
    except ValueError:
        print "Configuration file problem: storage_distribution_weight must be a float value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'high_priority_job_support')
    except ValueError:
        print "Configuation file problem: high_priority_job_support must be a boolean value"
        sys.exit(1)

    try:
        config_file.getint('global', 'high_priority_job_weight')
    except ValueError:
        print " Configuration file problem: high_priority_job_weight must be an integer value"

    try:
        config_file.getint('global', 'scheduler_interval')
    except ValueError:
        print "Configuration file problem, scheduler_interval must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'vm_poller_interval')
    except ValueError:
        print "Configuration file problem: vm_poller_interval must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'job_poller_interval')
    except ValueError:
        print "Configuration file problem: job_poller_interval must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'machine_poller_interval')
    except ValueError:
        print "Configuration file problem: machine_poller_interval must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'cleanup_interval')
    except ValueError:
        print "Configuration file problem: cleanup_interval must be an integer value"

    try:
        config_file.getint('global', 'job_proxy_refresher_interval')
    except ValueError:
        print "Configuration file problem: job_proxy_refresher_interval must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'job_proxy_renewal_threshold')
    except ValueError:
        print "Configuration file problem: job_proxy_renewal_threshold must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'vm_proxy_refresher_interval')
    except ValueError:
        print "Configuration file problem: vm_proxy_refresher_interval must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'vm_proxy_renewal_threshold')
    except ValueError:
        print "Configuration file problem: vm_proxy_renewal_threshold must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'vm_proxy_shutdown_threshold')
    except ValueError:
        print "Configuration file problem: vm_proxy_shutdown_threshold must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'vm_connection_fail_threshold')
    except ValueError:
        print "Configuration file problem: vm_connection_threshold must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('global', 'vm_idle_threshold')
    except ValueError:
        print "Configuration file problem: vm_idle_threshold must be an integer value"
        sys.exit(1)

    try:
        config_file.getint("global", "vm_start_running_timeout")
    except ValueError:
        print "Configuration file problem: vm_start_running_timeout must be an " \
              "integer value."
        sys.exit(1)

    try:
        max_starting_vm = config_file.getint('global', 'max_starting_vm')
        if max_starting_vm < -1:
            config_file.set('global', 'max_starting_vm', -1)
    except ValueError:
        print "Configuration file problem: max_starting_vm must be an integer value"
        sys.exit(1)

    try:
        max_keepalive = config_file.getint('global', 'max_keepalive')
        if max_starting_vm < 0:
            config_file.set('global', 'max_keepalive', 0)
    except ValueError:
        print "Configuration file problem: max_keepalive must be an integer value"
        sys.exit(1)

    try:
        max_destroy_threads = config_file.getint('global', 'max_destroy_threads')
        if max_destroy_threads < -1:
            config_file.set('global', 'max_destroy_threads', -1)
    except ValueError:
        print "Configuration file problem: max_destroy_threads must be an integer value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'override_vmtype')
    except ValueError:
        print "Configuation file problem: override_vmtype must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'vm_reqs_from_condor_reqs')
    except ValueError:
        print "Configuation file problem: vm_reqs_from_condor_reqs must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'adjust_insufficient_resources')
    except ValueError:
        print "Configuation file problem: adjust_insufficent_resources must be a boolean value"
        sys.exit(1)

    try:
        config_file.getint('global', 'connection_fail_disable_time')
    except ValueError:
        print "Configuation file problem: connection_fail_disable_time must be an interger value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'use_cloud_init')
    except ValueError:
        print "Configuation file problem: use_cloud_init must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'validate_yaml')
    except ValueError:
        print "Configuation file problem: validate_yaml must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'retire_reallocate')
    except ValueError:
        print "Configuation file problem: retire_reallocate must be a boolean value"
        sys.exit(1)

    if config_file.get('logging', 'log_location') == 'None':
        config_file.set('logging', 'log_location')
    else:
        log_location = os.path.expanduser(config_file.get('logging', 'log_location'))
        config_file.set('logging', 'log_location', log_location)

    if config_file.get('logging', 'log_location_cloud_admin') == 'None':
        config_file.set('logging', 'log_location_cloud_admin')
    else:
        log_location_cloud_admin = os.path.expanduser(config_file.get('logging', 'log_location_cloud_admin'))
        config_file.set('logging', 'log_location_cloud_admin', log_location_cloud_admin)

    try:
        config_file.getboolean('logging', 'admin_log_comments')
    except ValueError:
        print "Configuration file problem: admin_log_comments must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('logging', 'log_syslog')
    except ValueError:
        print "Configuration file problem: log_syslog must be a boolean value"
        sys.exit(1)

    try:
        config_file.getint('logging', 'log_max_size')
    except ValueError:
        if config_file.get('logging', 'log_max_size'):
            print "Configuration file problem: log_max_size must be an integer value"
            sys.exit(1)

    try:
        config_file.getint('job', 'default_VMMem')
    except ValueError:
        print "Configuration file problem: default_VMMem must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('job', 'default_VMCPUCores')
    except ValueError:
        print "Configuration file problem: default_VMCPUCores must be an integer value"
        sys.exit(1)

    try:
        config_file.getint('job', 'default_VMStorage')
    except ValueError:
        print "Configuration file problem: default_VMStorage must be an integer value"
        sys.exit(1)

    try:
        config_file.getfloat('job', 'default_VMMaximumPrice')
    except ValueError:
        print "Configuration file problem: default_VMMaximumPrice must be a float value"
        sys.exit(1)

    try:
        config_file.getboolean('job', 'default_VMProxyNonBoot')
    except ValueError:
        print "Configuration file problem: default_VMProxyNonBoot must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('job', 'default_VMJobPerCore')
    except ValueError:
        print "Configuration file problem: default_VMJobPerCore must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('job', 'default_VMInjectCA')
    except ValueError:
        print "Configuration file problem: default_VMInjectCA must be a boolean value"
        sys.exit(1)

    try:
        config_file.getboolean('global', 'use_pyopenssl')
    except ValueError:
        print "Configuration file problem: use_pyopenssl must be a boolean value"
        sys.exit(1)

    if os.path.exists('/usr/local/share/cloud-scheduler/default.yaml'):
        config_file.set('global', 'default_yaml', '/usr/local/share/cloud-scheduler/default.yaml')

    global config_options
    config_options = config_file

    return config_file

def get_config_parser(path=None):
    if path:
        config = setup(path)
    else:
        config = setup()

    return config
