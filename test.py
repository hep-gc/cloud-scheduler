#!/usr/bin/env python

import unittest
import os
import tempfile
import ConfigParser

import cloudscheduler.config

class ConfigParserSetsCorrectValues(unittest.TestCase):

    def setUp(self):

        # set values for each option
        self.condor_webservice_url = "http://testhost:1234"
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
        testconfig.set('global', 'cloud_resource_config', self.cloud_resource_config)
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

    def test_cloud_resource_config(self):
        self.assertEqual(self.cloud_resource_config, cloudscheduler.config.cloud_resource_config)

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

    def tearDown(self):
        os.remove(self.configfilename)

if __name__ == '__main__':
    unittest.main()
