#!/usr/bin/env python

import unittest
import os
import sys
import tempfile
import ConfigParser
from cStringIO import StringIO

import cloudscheduler.config
import cloudscheduler.cloud_management
import cloudscheduler.nimbus_xml
import cloudscheduler.utilities as utilities


held, sys.stderr = sys.stderr, StringIO() # Hide stderr

class ConfigParserSetsCorrectValues(unittest.TestCase):

    def setUp(self):

        # set values for each option
        self.condor_webservice_url = "http://testhost:1234"
        self.condor_host = "testhost"
        self.condor_host_on_vm = "vmtesthost"
        self.cert_file = "/path/to/cert"
        self.key_file = "/path/to/key"
        self.cert_file_on_vm = "/path/to/certonvm"
        self.key_file_on_vm = "/path/to/keyonvm"
        self.condor_context_file = "/etc/testlocation"
        self.image_attach_device = "deva"
        self.scratch_attach_device = "devb"
        self.cloud_resource_config = "/home/testuser/cloud"
        self.info_server_port = "1234"

        self.log_level = "ERROR"
        self.log_location = "/tmp/test.log"
        self.log_stdout = "true"
        self.log_max_size = "1312312"

        # build config file
        (self.configfile, self.configfilename) = tempfile.mkstemp()
        testconfig = ConfigParser.RawConfigParser()

        testconfig.add_section('global')
        testconfig.set('global', 'condor_webservice_url', self.condor_webservice_url)
        testconfig.set('global', 'condor_host_on_vm', self.condor_host_on_vm)
        testconfig.set('global', 'cert_file', self.cert_file)
        testconfig.set('global', 'key_file', self.key_file)
        testconfig.set('global', 'cert_file_on_vm', self.cert_file_on_vm)
        testconfig.set('global', 'key_file_on_vm', self.key_file_on_vm)
        testconfig.set('global', 'condor_context_file', self.condor_context_file)
        testconfig.set('global', 'cloud_resource_config', self.cloud_resource_config)
        testconfig.set('global', 'image_attach_device', self.image_attach_device)
        testconfig.set('global', 'scratch_attach_device', self.scratch_attach_device)
        testconfig.set('global', 'info_server_port', self.info_server_port)

        testconfig.add_section('logging')
        testconfig.set('logging', 'log_level', self.log_level)
        testconfig.set('logging', 'log_location', self.log_location)
        testconfig.set('logging', 'log_stdout', self.log_stdout)
        testconfig.set('logging', 'log_max_size', self.log_max_size)

        # write temporary config file
        configfile = open(self.configfilename, 'wb')
        testconfig.write(configfile)
        configfile.close()
        cloudscheduler.config.setup(path=self.configfilename)


    def test_condor_webservice_url(self):
        self.assertEqual(self.condor_webservice_url, cloudscheduler.config.condor_webservice_url)
    def test_condor_host(self):
        if self.condor_host_on_vm:
            self.assertEqual(self.condor_host_on_vm, cloudscheduler.config.condor_host)
        else:
            self.assertEqual(self.condor_host, cloudscheduler.config.condor_host)

    def test_condor_host_on_vm(self):
        self.assertEqual(self.condor_host_on_vm, cloudscheduler.config.condor_host_on_vm)
    def test_condor_context_file(self):
        self.assertEqual(self.condor_context_file, cloudscheduler.config.condor_context_file)

    def test_cert_file(self):
        self.assertEqual(self.cert_file, cloudscheduler.config.cert_file)

    def test_key_file(self):
        self.assertEqual(self.key_file, cloudscheduler.config.key_file)

    def test_cert_file_on_vm(self):
        self.assertEqual(self.cert_file_on_vm, cloudscheduler.config.cert_file_on_vm)

    def test_key_file_on_vm(self):
        self.assertEqual(self.key_file_on_vm, cloudscheduler.config.key_file_on_vm)

    def test_cloud_resource_config(self):
        self.assertEqual(self.cloud_resource_config, cloudscheduler.config.cloud_resource_config)

    def test_image_attach_device(self):
        self.assertEqual(self.image_attach_device, cloudscheduler.config.image_attach_device)

    def test_scratch_attach_device(self):
        self.assertEqual(self.scratch_attach_device, cloudscheduler.config.scratch_attach_device)

    def test_info_server_port(self):
        self.assertEqual(int(self.info_server_port), cloudscheduler.config.info_server_port)

    def test_log_level(self):
        self.assertEqual(self.log_level, cloudscheduler.config.log_level)

    def test_log_location(self):
        self.assertEqual(self.log_location, cloudscheduler.config.log_location)

    def test_log_stdout(self):
        self.assertEqual(bool(self.log_stdout), cloudscheduler.config.log_stdout)

    def test_log_max_size(self):
        self.assertEqual(int(self.log_max_size), cloudscheduler.config.log_max_size)

    def test_for_spaces_before_values(self):

        config_with_spaces = '''[global]
 condor_webservice_url = "localhost"
        '''

        configfile = open(self.configfilename, 'wb')
        configfile.write(config_with_spaces)
        configfile.close()

        self.assertRaises(ConfigParser.ParsingError,
                          cloudscheduler.config.setup, path=self.configfilename)

    def tearDown(self):
        os.remove(self.configfilename)

class ResourcePoolSetup(unittest.TestCase):

    def setUp(self):

        # set values for each option
        self.cloud_name0 = "example0"
        self.host0 = "cloud.example.com"
        self.cloud_type0 = "Nimbus"
        self.vm_slots0 = 100
        self.cpu_cores0 = 4
        self.storage0 = 1000
        self.memory0 = 2048
        self.cpu_archs0 = "x86"
        self.networks0 = "private"

        self.cloud_name1 = "example1"
        self.host1 = "cloud.example.com"
        self.cloud_type1 = "Nimbus"
        self.vm_slots1 = 100
        self.cpu_cores1 = 4
        self.storage1 = 1000
        self.memory1 = 2048
        self.cpu_archs1 = "x86"
        self.networks1 = "private"

        # build config file
        (self.configfile, self.configfilename) = tempfile.mkstemp()
        testconfig = ConfigParser.RawConfigParser()

        testconfig.add_section(self.cloud_name0)
        testconfig.set(self.cloud_name0, 'host', self.host0)
        testconfig.set(self.cloud_name0, 'cloud_type', self.cloud_type0)
        testconfig.set(self.cloud_name0, 'vm_slots', self.vm_slots0)
        testconfig.set(self.cloud_name0, 'cpu_cores', self.cpu_cores0)
        testconfig.set(self.cloud_name0, 'storage', self.storage0)
        testconfig.set(self.cloud_name0, 'memory', self.memory0)
        testconfig.set(self.cloud_name0, 'cpu_archs', self.cpu_archs0)
        testconfig.set(self.cloud_name0, 'networks', self.networks0)

        testconfig.add_section(self.cloud_name1)
        testconfig.set(self.cloud_name1, 'host', self.host1)
        testconfig.set(self.cloud_name1, 'cloud_type', self.cloud_type1)
        testconfig.set(self.cloud_name1, 'vm_slots', self.vm_slots1)
        testconfig.set(self.cloud_name1, 'cpu_cores', self.cpu_cores1)
        testconfig.set(self.cloud_name1, 'storage', self.storage1)
        testconfig.set(self.cloud_name1, 'memory', self.memory1)
        testconfig.set(self.cloud_name1, 'cpu_archs', self.cpu_archs1)
        testconfig.set(self.cloud_name1, 'networks', self.networks1)

        # write temporary config file
        configfile = open(self.configfilename, 'wb')
        testconfig.write(configfile)
        configfile.close()
        cloudscheduler.config.setup(path=self.configfilename)

        self.test_pool = cloudscheduler.cloud_management.ResourcePool("Test Pool")
        self.test_pool.setup(self.configfilename)

    def test_cluster_is_created(self):

        # Check that cloud_name0 is found
        found_cluster0 = False
        for cluster in self.test_pool.resources:
            if cluster.name == self.cloud_name0:
                found_cluster0 = True
        self.assertTrue(found_cluster0)

        # Check that cloud_name1 is found
        found_cluster1 = False
        for cluster in self.test_pool.resources:
            if cluster.name == self.cloud_name1:
                found_cluster1 = True
        self.assertTrue(found_cluster1)

    def test_for_spaces_before_values(self):

        config_with_spaces = '''[cluster]
 host = "localhost"
        '''

        configfile = open(self.configfilename, 'wb')
        configfile.write(config_with_spaces)
        configfile.close()

        self.assertRaises(ConfigParser.ParsingError,
                          self.test_pool.setup,
                          self.configfilename)

    def tearDown(self):
        os.remove(self.configfilename)

class NimbusXMLTests(unittest.TestCase):

    def setUp(self):
        self.custom_filename = "/tmp/filename"
        self.custom_string = "stringtoput"
        self.custom_tasks = [(self.custom_string, self.custom_filename)]
        self.optional_xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?><OptionalParameters><filewrite><content>%s</content><pathOnVM>%s</pathOnVM></filewrite></OptionalParameters>" % (self.custom_string, self.custom_filename)

    def test_for_good_optional_parameters(self):
        txml = cloudscheduler.nimbus_xml.ws_optional_factory(self.custom_tasks)
        
        xml_file = open(txml, "r")
        generated_xml = xml_file.read()
        # Some versions of python xml insert a newline
        generated_xml = generated_xml.replace("\n", "")
        self.assertEqual(generated_xml, self.optional_xml)
        
        xml_file.close()
        os.remove(txml)

    def test_optional_with_bad_path(self):

        bad_filename = "not a filepath"
        bad_custom_task = [(bad_filename, self.custom_string)]

        txml = cloudscheduler.nimbus_xml.ws_optional_factory(bad_custom_task)

        self.assertEqual(None, txml)

    def test_optional_with_empty_path(self):

        bad_filename = ""
        bad_custom_task = [(bad_filename, self.custom_string)]

        txml = cloudscheduler.nimbus_xml.ws_optional_factory(bad_custom_task)

        self.assertEqual(None, txml)

class GetOrNoneTests(unittest.TestCase):

    def setUp(self):

        self.section_name = "section"
        self.good_name = "nameofparam"
        self.good_val = "itsvalue"

        # build config file
        (self.configfile, self.configfilename) = tempfile.mkstemp()
        testconfig = ConfigParser.RawConfigParser()

        testconfig.add_section(self.section_name)
        testconfig.set(self.section_name, self.good_name, self.good_val)

        # write temporary config file
        configfile = open(self.configfilename, 'wb')
        testconfig.write(configfile)
        configfile.close()
        cloudscheduler.config.setup(path=self.configfilename)


    def test_get(self):
        config = ConfigParser.ConfigParser()
        config.read(self.configfilename)
        value = utilities.get_or_none(config, self.section_name, self.good_name)
        self.assertEqual(self.good_val, value)

    def test_none(self):
        config = ConfigParser.ConfigParser()
        config.read(self.configfilename)
        value = utilities.get_or_none(config, self.section_name, "fakeitem")
        self.assertEqual(None, value)

if __name__ == '__main__':
    unittest.main()
