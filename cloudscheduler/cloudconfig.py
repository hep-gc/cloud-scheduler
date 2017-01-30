import utilities

log = utilities.get_cloudscheduler_logger()


def verify_cloud_conf_openstacknative(conf, name):
    """
    :param conf: Ref to config file data
    :param name: name of cloud checking conf values for
    :return: True if all required fields present, False otherise
    """
    required_options_openstack = {'auth_url', 'cloud_type', 'password', 'regions', 'security_group', 'tenant_name',
                                  'username', 'vm_slots'}
    options = set(conf.options(name))
    diff = required_options_openstack - options
    if len(diff) > 0:
        log.error("Missing required options in %s: %s" % (name, str(diff)))
        return False
    return True


def verify_cloud_conf_azure(conf, name):
    """
    :param conf: Ref to config file data
    :param name: name of cloud checking conf values for
    :return: True if all required fields present, False otherise
    """
    required_options_azure = {'blob_url', 'cloud_type', 'password', 'regions', 'tenant_name',
                              'username', 'vm_slots'}
    options = set(conf.options(name))
    diff = required_options_azure - options
    if len(diff) > 0:
        log.error("Missing required options in %s: %s" % (name, str(diff)))
        return False
    return True


def verify_cloud_conf_ec2(conf, name):
    """
    :param conf: Ref to config file data
    :param name: name of cloud checking conf values for
    :return: True if all required fields present, False otherise
    """
    required_options_ec2 = {'access_key_id', 'cloud_type', 'host', 'memory', 'regions', 'secret_access_key',
                            'security_group', 'vm_slots'}
    options = set(conf.options(name))
    diff = required_options_ec2 - options
    if len(diff) > 0:
        log.error("Missing required options in %s: %s" % (name, str(diff)))
        return False
    return True


def verify_cloud_conf_gce(conf, name):
    """
    :param conf: Ref to config file data
    :param name: name of cloud checking conf values for
    :return: True if all required fields present, False otherise
    """
    required_options_gce = {'auth_dat_file', 'cloud_type', 'networks', 'project_id', 'secret_file', 'security_group',
                            'vm_slots'}
    options = set(conf.options(name))
    diff = required_options_gce - options
    if len(diff) > 0:
        log.error("Missing required options in %s: %s" % (name, str(diff)))
        return False
    return True


def verify_cloud_conf_stratuslab(conf, name):
    """
    :param conf: Ref to config file data
    :param name: name of cloud checking conf values for
    :return: True if all required fields present, False otherise
    """
    required_options_stratuslab = {'cloud_type', 'contextualization', 'host', 'vm_slots'}
    options = set(conf.options(name))
    diff = required_options_stratuslab - options
    if len(diff) > 0:
        log.error("Missing required options in %s: %s" % (name, str(diff)))
        return False
    return True


def verify_sections_base(conf, name):
    """
    Check the sections to make sure there's no extra or misspelled keys.
    :param conf: The config parse ref
    :param name: The name of cloud to operate on
    :return: True if conf good, False if problem detected
    """
    valid_option_names = {'access_key_id', 'auth_dat_file', 'auth_url', 'blob_url', 'boot_timeout', 'cacert',
                          'cloud_type', 'contextualization', 'cpu_archs', 'cpu_cores', 'host',
                          'image_attach_device', 'key_name', 'keycert', 'max_vm_mem', 'max_vm_storage', 'memory',
                          'networks', 'password', 'placement_zone', 'port', 'priority', 'project_id', 'regions',
                          'reverse_dns_lookup', 'scratch_attach_device', 'secret_access_key', 'secret_file',
                          'secure_connection', 'security_group', 'service_name', 'storage', 'temp_lease_storage',
                          'tenant_name', 'total_cpu_cores', 'username', 'vm_keep_alive', 'vm_lifetime', 'vm_slots'}
    options = set(conf.options(name))
    diff = options - valid_option_names
    if len(diff) > 0:
        for option in diff:
            log.error(
                "%s in section %s is not a valid option for cloud_resources.conf cloud definitions." % (option, name))
        return False
    return True
