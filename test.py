#!/usr/bin/env python

import unittest
import os
import sys
import tempfile
import ConfigParser
from cStringIO import StringIO
from lxml import etree


import cloudscheduler.config
import cloudscheduler.cloud_management
import cloudscheduler.job_management
import cloudscheduler.nimbus_xml
import cloudscheduler.utilities as utilities

log = utilities.get_cloudscheduler_logger()

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
        self.workspace_path = "/path/to/workspace"
        self.persistence_file = "/path/to/persistence"
        self.scheduler_interval = 42
        self.vm_poller_interval = 42
        self.job_poller_interval = 42
        self.machine_poller_interval = 42
        self.cleanup_interval = 42

        self.log_level = "ERROR"
        self.log_location = "/tmp/test.log"
        self.log_stdout = "true"
        self.log_max_size = "1312312"
        self.log_format = "format_string"

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
        testconfig.set('global', 'workspace_path', self.workspace_path)
        testconfig.set('global', 'persistence_file', self.persistence_file)
        testconfig.set('global', 'scheduler_interval', self.scheduler_interval)
        testconfig.set('global', 'vm_poller_interval', self.vm_poller_interval)
        testconfig.set('global', 'job_poller_interval', self.job_poller_interval)
        testconfig.set('global', 'machine_poller_interval', self.machine_poller_interval)
        testconfig.set('global', 'cleanup_interval', self.cleanup_interval)

        testconfig.add_section('logging')
        testconfig.set('logging', 'log_level', self.log_level)
        testconfig.set('logging', 'log_location', self.log_location)
        testconfig.set('logging', 'log_stdout', self.log_stdout)
        testconfig.set('logging', 'log_max_size', self.log_max_size)
        testconfig.set('logging', 'log_format', self.log_format)

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

    def test_workspace_path(self):
        self.assertEqual(self.workspace_path, cloudscheduler.config.workspace_path)

    def test_persistence_file(self):
        self.assertEqual(self.persistence_file, cloudscheduler.config.persistence_file)

    def test_scheduler_interval(self):
        self.assertEqual(int(self.scheduler_interval), cloudscheduler.config.scheduler_interval)

    def test_vm_poller_interval(self):
        self.assertEqual(int(self.vm_poller_interval), cloudscheduler.config.vm_poller_interval)

    def test_job_poller_interval(self):
        self.assertEqual(int(self.job_poller_interval), cloudscheduler.config.job_poller_interval)

    def test_machine_poller_interval(self):
        self.assertEqual(int(self.machine_poller_interval), cloudscheduler.config.machine_poller_interval)

    def test_cleanup_interval(self):
        self.assertEqual(int(self.cleanup_interval), cloudscheduler.config.cleanup_interval)

    def test_log_level(self):
        self.assertEqual(self.log_level, cloudscheduler.config.log_level)

    def test_log_location(self):
        self.assertEqual(self.log_location, cloudscheduler.config.log_location)

    def test_log_stdout(self):
        self.assertEqual(bool(self.log_stdout), cloudscheduler.config.log_stdout)

    def test_log_max_size(self):
        self.assertEqual(int(self.log_max_size), cloudscheduler.config.log_max_size)

    def test_log_format(self):
        self.assertEqual(self.log_format, cloudscheduler.config.log_format)

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
        self.test_pool.config_file = self.configfilename
        self.test_pool.setup()

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


    def tearDown(self):
        os.remove(self.configfilename)

class NimbusXMLTests(unittest.TestCase):

    def setUp(self):
        self.custom_filename = "/tmp/filename"
        self.custom_string = "stringtoput"
        self.custom_tasks = [(self.custom_string, self.custom_filename)]
        self.optional_xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?><OptionalParameters><filewrite><content>%s</content><pathOnVM>%s</pathOnVM></filewrite></OptionalParameters>" % (self.custom_string, self.custom_filename)
        self.workspace_id = 42
        self.bad_workspace_id = "whatev"
        self.nimbus_hostname = "your.nimbus.tld"
        self.epr_xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?><WORKSPACE_EPR xmlns:ns1=\"http://schemas.xmlsoap.org/ws/2004/03/addressing\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"ns1:EndpointReferenceType\"><ns1:Address xsi:type=\"ns1:AttributedURI\">https://%s:8443/wsrf/services/WorkspaceService</ns1:Address><ns1:ReferenceProperties xsi:type=\"ns1:ReferencePropertiesType\"><ns2:WorkspaceKey xmlns:ns2=\"http://www.globus.org/2008/06/workspace\">%d</ns2:WorkspaceKey></ns1:ReferenceProperties><ns1:ReferenceParameters xsi:type=\"ns1:ReferenceParametersType\"/></WORKSPACE_EPR>" % (self.nimbus_hostname, self.workspace_id)

    def test_for_good_epr_parameters(self):
        txml = cloudscheduler.nimbus_xml.ws_epr(self.workspace_id, self.nimbus_hostname)

        self.assertEqual(txml, self.epr_xml)

    def test_for_bad_epr_parameters(self):
        txml = cloudscheduler.nimbus_xml.ws_epr(self.bad_workspace_id, self.nimbus_hostname)

        self.assertEqual(txml, None)

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

class NimbusClusterTests(unittest.TestCase):

    def test_extract_hostname(self):
        from cloudscheduler.cluster_tools import NimbusCluster
        nimbus_string = """
Workspace Factory Service:
    https://calliopex.phys.uvic.ca:8443/wsrf/services/WorkspaceFactoryService

Read metadata file: "/tmp/nimbus.1276022787.xml"
Read deployment request file: "/tmp/nimbus.deployment.1276022787.xml"
Duration argument provided: overriding duration found in deployment request file, it is now: 100 minutes

Creating workspace "http://calliopex.phys.uvic.ca/sl54base_i386.img.gz"... done.



Workspace created: id 2
eth0
      Association: private
       IP address: 192.168.107.1
         Hostname: musecloud-01
          Gateway: 192.168.1.217

       Start time: Tue Jun 08 11:46:29 PDT 2010
         Duration: 100 minutes.
    Shutdown time: Tue Jun 08 13:26:29 PDT 2010
 Termination time: Tue Jun 08 13:36:29 PDT 2010

Wrote EPR to "./nimbus.1276022787.epr"


Waiting for updates.

"http://calliopex.phys.uvic.ca/sl54base_i386.img.gz" state change: Unstaged --> Unpropagated
"""
        extracted_host = NimbusCluster._extract_hostname(nimbus_string)
        self.assertEqual("musecloud-01", extracted_host)

        bad_host = NimbusCluster._extract_hostname("")
        self.assertEqual("", bad_host)

    def test_extract_state(self):
        from cloudscheduler.cluster_tools import NimbusCluster
        nimbus_string_non_existant = """
Problem: This workspace is unknown to the service (likely because it was terminated).
"""
        nimbus_string_good = """

NIC: eth0
  - Association: private
  - IP: 192.168.107.1
  - Hostname: musecloud01
  - Gateway: 192.168.1.217

Schedule:
  -        Start time: Mon Jul 19 11:51:36 PDT 2010
  -          Duration: 100 minutes.
  -     Shutdown time: Mon Jul 19 13:31:36 PDT 2010
  -  Termination time: Mon Jul 19 13:41:36 PDT 2010

State: Unpropagated
"""
        extracted_state = NimbusCluster._extract_state(nimbus_string_good)
        self.assertEqual("Starting", extracted_state)

        destroyed = NimbusCluster._extract_state(nimbus_string_non_existant)
        self.assertEqual("Destroyed", destroyed)


class ResourcePoolTests(unittest.TestCase):

    def test_condorxml_to_native_empty_list(self):

        from cloudscheduler.cloud_management import ResourcePool
        condor_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:condor="urn:condor"><SOAP-ENV:Header></SOAP-ENV:Header><SOAP-ENV:Body><condor:queryStartdAdsResponse><result></result></condor:queryStartdAdsResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>"""

        xml2native = cloudscheduler.cloud_management.ResourcePool._condor_machine_xml_to_machine_list
        machines = xml2native(condor_xml)
        self.assertEqual([], machines)

    def test_condorxml_to_native_one_machine(self):

        from cloudscheduler.cloud_management import ResourcePool

        ServerTime = "1278352861"

        condor_xml = """<?xml version="1.0" encoding="utf-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xmlns:xsd="http://www.w3.org/2001/XMLSchema"
xmlns:condor="urn:condor">
  <SOAP-ENV:Header></SOAP-ENV:Header>
  <SOAP-ENV:Body>
    <condor:queryStartdAdsResponse>
      <result>
        <item>
          <item>
            <name>MyType</name>
            <type>STRING-ATTR</type>
            <value>Machine</value>
          </item>
          <item>
            <name>TargetType</name>
            <type>STRING-ATTR</type>
            <value>Job</value>
          </item>
          <item>
            <name>ServerTime</name>
            <type>INTEGER-ATTR</type>
            <value>1278352861</value>
          </item>
          <item>
            <name>Machine</name>
            <type>STRING-ATTR</type>
            <value>hermes-xen057</value>
          </item>
          <item>
            <name>LastHeardFrom</name>
            <type>INTEGER-ATTR</type>
            <value>1278352749</value>
          </item>
          <item>
            <name>UpdateSequenceNumber</name>
            <type>INTEGER-ATTR</type>
            <value>1124</value>
          </item>
          <item>
            <name>JavaVersion</name>
            <type>STRING-ATTR</type>
            <value>1.4.2</value>
          </item>
          <item>
            <name>JobId</name>
            <type>STRING-ATTR</type>
            <value>202.571</value>
          </item>
          <item>
            <name>HasMPI</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>TotalClaimRunTime</name>
            <type>INTEGER-ATTR</type>
            <value>5641</value>
          </item>
          <item>
            <name>CpuIsBusy</name>
            <type>BOOLEAN-ATTR</type>
            <value>FALSE</value>
          </item>
          <item>
            <name>HasVM</name>
            <type>BOOLEAN-ATTR</type>
            <value>FALSE</value>
          </item>
          <item>
            <name>JavaVendor</name>
            <type>STRING-ATTR</type>
            <value>Free Software Foundation, Inc.</value>
          </item>
          <item>
            <name>FileSystemDomain</name>
            <type>STRING-ATTR</type>
            <value>hermes-xen057</value>
          </item>
          <item>
            <name>Name</name>
            <type>STRING-ATTR</type>
            <value>hermes-xen057</value>
          </item>
          <item>
            <name>ImageSize</name>
            <type>INTEGER-ATTR</type>
            <value>151980</value>
          </item>
          <item>
            <name>MonitorSelfTime</name>
            <type>INTEGER-ATTR</type>
            <value>1278352716</value>
          </item>
          <item>
            <name>TimeToLive</name>
            <type>INTEGER-ATTR</type>
            <value>2147483647</value>
          </item>
          <item>
            <name>KeyboardIdle</name>
            <type>INTEGER-ATTR</type>
            <value>377368</value>
          </item>
          <item>
            <name>LastBenchmark</name>
            <type>INTEGER-ATTR</type>
            <value>1278344639</value>
          </item>
          <item>
            <name>TotalDisk</name>
            <type>INTEGER-ATTR</type>
            <value>7505032</value>
          </item>
          <item>
            <name>MaxJobRetirementTime</name>
            <type>INTEGER-ATTR</type>
            <value>0</value>
          </item>
          <item>
            <name>CondorPlatform</name>
            <type>STRING-ATTR</type>
            <value>$CondorPlatform: I386-LINUX_RHEL5 $</value>
          </item>
          <item>
            <name>Unhibernate</name>
            <type>EXPRESSION-ATTR</type>
            <value>MY.MachineLastMatchTime =!= undefined</value>
          </item>
          <item>
            <name>HasJICLocalStdin</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>UpdatesTotal</name>
            <type>INTEGER-ATTR</type>
            <value>1050</value>
          </item>
          <item>
            <name>IsValidCheckpointPlatform</name>
            <type>EXPRESSION-ATTR</type>
            <value>( ( ( TARGET.JobUniverse == 1 ) == false ) || (
            ( MY.CheckpointPlatform =!= undefined ) &amp;&amp; ( (
            TARGET.LastCheckpointPlatform =?= MY.CheckpointPlatform
            ) || ( TARGET.NumCkpts == 0 ) ) ) )</value>
          </item>
          <item>
            <name>Cpus</name>
            <type>INTEGER-ATTR</type>
            <value>1</value>
          </item>
          <item>
            <name>MonitorSelfCPUUsage</name>
            <type>FLOAT-ATTR</type>
            <value>0.000000</value>
          </item>
          <item>
            <name>TotalTimePreemptingKilling</name>
            <type>INTEGER-ATTR</type>
            <value>30</value>
          </item>
          <item>
            <name>ClockDay</name>
            <type>INTEGER-ATTR</type>
            <value>1</value>
          </item>
          <item>
            <name>IsWakeOnLanEnabled</name>
            <type>BOOLEAN-ATTR</type>
            <value>FALSE</value>
          </item>
          <item>
            <name>StarterAbilityList</name>
            <type>STRING-ATTR</type>
            <value>
            HasMPI,HasVM,HasJICLocalStdin,HasJICLocalConfig,HasJava,HasJobDeferral,HasTDP,HasFileTransfer,HasPerFileEncryption,HasReconnect,HasRemoteSyscalls,HasCheckpointing</value>
          </item>
          <item>
            <name>JavaSpecificationVersion</name>
            <type>STRING-ATTR</type>
            <value>1.4</value>
          </item>
          <item>
            <name>TotalTimeUnclaimedIdle</name>
            <type>INTEGER-ATTR</type>
            <value>12087</value>
          </item>
          <item>
            <name>CondorVersion</name>
            <type>STRING-ATTR</type>
            <value>$CondorVersion: 7.5.2 Apr 20 2010 BuildID:
            232940 $</value>
          </item>
          <item>
            <name>JobUniverse</name>
            <type>INTEGER-ATTR</type>
            <value>5</value>
          </item>
          <item>
            <name>HasIOProxy</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>TotalTimeClaimedBusy</name>
            <type>INTEGER-ATTR</type>
            <value>362756</value>
          </item>
          <item>
            <name>MonitorSelfImageSize</name>
            <type>FLOAT-ATTR</type>
            <value>10252.000000</value>
          </item>
          <item>
            <name>TotalTimeOwnerIdle</name>
            <type>INTEGER-ATTR</type>
            <value>4</value>
          </item>
          <item>
            <name>HibernationSupportedStates</name>
            <type>STRING-ATTR</type>
            <value>S3</value>
          </item>
          <item>
            <name>ExecutableSize</name>
            <type>INTEGER-ATTR</type>
            <value>1</value>
          </item>
          <item>
            <name>LastFetchWorkSpawned</name>
            <type>INTEGER-ATTR</type>
            <value>0</value>
          </item>
          <item>
            <name>Requirements</name>
            <type>EXPRESSION-ATTR</type>
            <value>( START ) &amp;&amp; ( IsValidCheckpointPlatform
            )</value>
          </item>
          <item>
            <name>TotalTimeClaimedIdle</name>
            <type>INTEGER-ATTR</type>
            <value>2431</value>
          </item>
          <item>
            <name>TotalMemory</name>
            <type>INTEGER-ATTR</type>
            <value>2048</value>
          </item>
          <item>
            <name>DaemonStartTime</name>
            <type>INTEGER-ATTR</type>
            <value>1277975423</value>
          </item>
          <item>
            <name>EnteredCurrentActivity</name>
            <type>INTEGER-ATTR</type>
            <value>1278347108</value>
          </item>
          <item>
            <name>MyAddress</name>
            <type>STRING-ATTR</type>
            <value>
            &lt;172.20.97.57:40021?CCBID=142.104.63.28:9618#56046&gt;</value>
          </item>
          <item>
            <name>HasJICLocalConfig</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>GlobalJobId</name>
            <type>STRING-ATTR</type>
            <value>
            canfarpool.phys.uvic.ca#202.571#1278178882</value>
          </item>
          <item>
            <name>HasJava</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>EnteredCurrentState</name>
            <type>INTEGER-ATTR</type>
            <value>1278347108</value>
          </item>
          <item>
            <name>CpuBusyTime</name>
            <type>INTEGER-ATTR</type>
            <value>0</value>
          </item>
          <item>
            <name>CpuBusy</name>
            <type>EXPRESSION-ATTR</type>
            <value>( ( LoadAvg - CondorLoadAvg ) &gt;= 0.500000
            )</value>
          </item>
          <item>
            <name>COLLECTOR_HOST_STRING</name>
            <type>STRING-ATTR</type>
            <value>canfarpool.phys.uvic.ca</value>
          </item>
          <item>
            <name>Memory</name>
            <type>INTEGER-ATTR</type>
            <value>2048</value>
          </item>
          <item>
            <name>IsWakeAble</name>
            <type>BOOLEAN-ATTR</type>
            <value>FALSE</value>
          </item>
          <item>
            <name>MyCurrentTime</name>
            <type>INTEGER-ATTR</type>
            <value>1278352749</value>
          </item>
          <item>
            <name>MonitorSelfRegisteredSocketCount</name>
            <type>INTEGER-ATTR</type>
            <value>3</value>
          </item>
          <item>
            <name>TotalTimeUnclaimedBenchmarking</name>
            <type>INTEGER-ATTR</type>
            <value>9</value>
          </item>
          <item>
            <name>TotalCpus</name>
            <type>INTEGER-ATTR</type>
            <value>1</value>
          </item>
          <item>
            <name>ClockMin</name>
            <type>INTEGER-ATTR</type>
            <value>659</value>
          </item>
          <item>
            <name>CurrentRank</name>
            <type>FLOAT-ATTR</type>
            <value>0.000000</value>
          </item>
          <item>
            <name>AuthenticatedIdentity</name>
            <type>STRING-ATTR</type>
            <value>unauthenticated@unmapped</value>
          </item>
          <item>
            <name>NextFetchWorkDelay</name>
            <type>EXPRESSION-ATTR</type>
            <value>-1</value>
          </item>
          <item>
            <name>OpSys</name>
            <type>STRING-ATTR</type>
            <value>LINUX</value>
          </item>
          <item>
            <name>State</name>
            <type>STRING-ATTR</type>
            <value>Claimed</value>
          </item>
          <item>
            <name>UpdatesHistory</name>
            <type>STRING-ATTR</type>
            <value>0x00000000000000000000000000000000</value>
          </item>
          <item>
            <name>UpdatesSequenced</name>
            <type>INTEGER-ATTR</type>
            <value>1049</value>
          </item>
          <item>
            <name>KFlops</name>
            <type>INTEGER-ATTR</type>
            <value>1782641</value>
          </item>
          <item>
            <name>Start</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>RemoteUser</name>
            <type>STRING-ATTR</type>
            <value>sharon@canfarpool.phys.uvic.ca</value>
          </item>
          <item>
            <name>HasRemoteSyscalls</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>HasJobDeferral</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>HasCheckpointing</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>MonitorSelfResidentSetSize</name>
            <type>INTEGER-ATTR</type>
            <value>4920</value>
          </item>
          <item>
            <name>Mips</name>
            <type>INTEGER-ATTR</type>
            <value>5562</value>
          </item>
          <item>
            <name>Arch</name>
            <type>STRING-ATTR</type>
            <value>INTEL</value>
          </item>
          <item>
            <name>Activity</name>
            <type>STRING-ATTR</type>
            <value>Busy</value>
          </item>
          <item>
            <name>ClientMachine</name>
            <type>STRING-ATTR</type>
            <value>canfarpool.phys.UVic.CA</value>
          </item>
          <item>
            <name>IsWakeOnLanSupported</name>
            <type>BOOLEAN-ATTR</type>
            <value>FALSE</value>
          </item>
          <item>
            <name>LastFetchWorkCompleted</name>
            <type>INTEGER-ATTR</type>
            <value>0</value>
          </item>
          <item>
            <name>SubnetMask</name>
            <type>STRING-ATTR</type>
            <value>255.255.224.0</value>
          </item>
          <item>
            <name>HasTDP</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>ConsoleIdle</name>
            <type>INTEGER-ATTR</type>
            <value>377368</value>
          </item>
          <item>
            <name>UpdatesLost</name>
            <type>INTEGER-ATTR</type>
            <value>0</value>
          </item>
          <item>
            <name>TotalJobRunTime</name>
            <type>INTEGER-ATTR</type>
            <value>5641</value>
          </item>
          <item>
            <name>StartdIpAddr</name>
            <type>STRING-ATTR</type>
            <value>
            &lt;172.20.97.57:40021?CCBID=142.104.63.28:9618#56046&gt;</value>
          </item>
          <item>
            <name>WakeOnLanEnabledFlags</name>
            <type>STRING-ATTR</type>
            <value>NONE</value>
          </item>
          <item>
            <name>NiceUser</name>
            <type>BOOLEAN-ATTR</type>
            <value>FALSE</value>
          </item>
          <item>
            <name>HibernationLevel</name>
            <type>INTEGER-ATTR</type>
            <value>0</value>
          </item>
          <item>
            <name>HasFileTransfer</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>TotalLoadAvg</name>
            <type>FLOAT-ATTR</type>
            <value>1.000000</value>
          </item>
          <item>
            <name>Rank</name>
            <type>FLOAT-ATTR</type>
            <value>0.000000</value>
          </item>
          <item>
            <name>MonitorSelfSecuritySessions</name>
            <type>INTEGER-ATTR</type>
            <value>3</value>
          </item>
          <item>
            <name>HibernationState</name>
            <type>STRING-ATTR</type>
            <value>NONE</value>
          </item>
          <item>
            <name>JavaMFlops</name>
            <type>FLOAT-ATTR</type>
            <value>17.634478</value>
          </item>
          <item>
            <name>MonitorSelfAge</name>
            <type>INTEGER-ATTR</type>
            <value>0</value>
          </item>
          <item>
            <name>VMType</name>
            <type>STRING-ATTR</type>
            <value>canfarbase_seb</value>
          </item>
          <item>
            <name>LoadAvg</name>
            <type>FLOAT-ATTR</type>
            <value>1.000000</value>
          </item>
          <item>
            <name>WakeOnLanSupportedFlags</name>
            <type>STRING-ATTR</type>
            <value>NONE</value>
          </item>
          <item>
            <name>HasPerFileEncryption</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>CheckpointPlatform</name>
            <type>STRING-ATTR</type>
            <value>LINUX INTEL 2.6.x normal 0x40000000</value>
          </item>
          <item>
            <name>JobStart</name>
            <type>INTEGER-ATTR</type>
            <value>1278347108</value>
          </item>
          <item>
            <name>CurrentTime</name>
            <type>EXPRESSION-ATTR</type>
            <value>time()</value>
          </item>
          <item>
            <name>RemoteOwner</name>
            <type>STRING-ATTR</type>
            <value>sharon@canfarpool.phys.uvic.ca</value>
          </item>
          <item>
            <name>Disk</name>
            <type>INTEGER-ATTR</type>
            <value>7505032</value>
          </item>
          <item>
            <name>VirtualMemory</name>
            <type>INTEGER-ATTR</type>
            <value>0</value>
          </item>
          <item>
            <name>TotalSlots</name>
            <type>INTEGER-ATTR</type>
            <value>1</value>
          </item>
          <item>
            <name>TotalVirtualMemory</name>
            <type>INTEGER-ATTR</type>
            <value>0</value>
          </item>
          <item>
            <name>UidDomain</name>
            <type>STRING-ATTR</type>
            <value>hermes-xen057</value>
          </item>
          <item>
            <name>SlotID</name>
            <type>INTEGER-ATTR</type>
            <value>1</value>
          </item>
          <item>
            <name>SlotWeight</name>
            <type>EXPRESSION-ATTR</type>
            <value>Cpus</value>
          </item>
          <item>
            <name>HasReconnect</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
          <item>
            <name>HardwareAddress</name>
            <type>STRING-ATTR</type>
            <value>a2:aa:bb:18:56:75</value>
          </item>
          <item>
            <name>CanHibernate</name>
            <type>BOOLEAN-ATTR</type>
            <value>TRUE</value>
          </item>
        </item>
      </result>
    </condor:queryStartdAdsResponse>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""

        xml2native = cloudscheduler.cloud_management.ResourcePool._condor_machine_xml_to_machine_list
        parsed_machines = xml2native(condor_xml)
        parsed_machine = parsed_machines[0]
        parsed_server_time = parsed_machine["ServerTime"]

        self.assertEqual(parsed_server_time, ServerTime)

class JobPoolTests(unittest.TestCase):

    def test_condorxml_to_native_empty_list(self):

        from cloudscheduler.job_management import JobPool
        condor_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:condor="urn:condor"><SOAP-ENV:Header></SOAP-ENV:Header><SOAP-ENV:Body><condor:getJobAdsResponse><response><status><code>SUCCESS</code><message>Success</message></status><classAdArray></classAdArray></response></condor:getJobAdsResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>"""

        xml2native = cloudscheduler.job_management.JobPool._condor_job_xml_to_job_list
        jobs = xml2native(condor_xml)
        self.assertEqual([], jobs)

    def test_condorxml_to_native_not_success(self):

        condor_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:condor="urn:condor"><SOAP-ENV:Header></SOAP-ENV:Header><SOAP-ENV:Body><condor:getJobAdsResponse><response><status><code>FAIL</code><message>Success</message></status><classAdArray></classAdArray></response></condor:getJobAdsResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>"""

        xml2native = cloudscheduler.job_management.JobPool._condor_job_xml_to_job_list
        jobs = xml2native(condor_xml)
        self.assertEqual([], jobs)

    def test_condorxml_to_native_one_job(self):

        condor_xml = """<?xml version="1.0" encoding="utf-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xmlns:xsd="http://www.w3.org/2001/XMLSchema"
xmlns:condor="urn:condor">
  <SOAP-ENV:Header></SOAP-ENV:Header>
  <SOAP-ENV:Body>
    <condor:getJobAdsResponse>
      <response>
        <status>
          <code>SUCCESS</code>
          <message>Success</message>
        </status>
        <classAdArray>
          <item>
            <item>
              <name>MyType</name>
              <type>STRING-ATTR</type>
              <value>Job</value>
            </item>
            <item>
              <name>TargetType</name>
              <type>STRING-ATTR</type>
              <value>Machine</value>
            </item>
            <item>
              <name>ServerTime</name>
              <type>INTEGER-ATTR</type>
              <value>1274118753</value>
            </item>
            <item>
              <name>LastJobStatus</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>JobCurrentStartDate</name>
              <type>INTEGER-ATTR</type>
              <value>1274118697</value>
            </item>
            <item>
              <name>PublicClaimId</name>
              <type>STRING-ATTR</type>
              <value>
              &lt;192.168.107.12:40000&gt;#1274063306#3#...</value>
            </item>
            <item>
              <name>RemoteHost</name>
              <type>STRING-ATTR</type>
              <value>musecloud12</value>
            </item>
            <item>
              <name>NumJobMatches</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>JobStatus</name>
              <type>INTEGER-ATTR</type>
              <value>2</value>
            </item>
            <item>
              <name>EnteredCurrentStatus</name>
              <type>INTEGER-ATTR</type>
              <value>1274118697</value>
            </item>
            <item>
              <name>ShadowBday</name>
              <type>INTEGER-ATTR</type>
              <value>1274118697</value>
            </item>
            <item>
              <name>StartdPrincipal</name>
              <type>STRING-ATTR</type>
              <value>142.104.61.62</value>
            </item>
            <item>
              <name>GlobalJobId</name>
              <type>STRING-ATTR</type>
              <value>vmcgs35.phys.uvic.ca#85.0#1274118681</value>
            </item>
            <item>
              <name>JobStartDate</name>
              <type>INTEGER-ATTR</type>
              <value>1274118697</value>
            </item>
            <item>
              <name>LastJobLeaseRenewal</name>
              <type>INTEGER-ATTR</type>
              <value>1274118713</value>
            </item>
            <item>
              <name>ProcId</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>OrigMaxHosts</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>CurrentHosts</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>AutoClusterAttrs</name>
              <type>STRING-ATTR</type>
              <value>
              JobUniverse,LastCheckpointPlatform,NumCkpts,CondorLoadAvg,DiskUsage,ImageSize,RequestMemory,Requirements,NiceUser,ConcurrencyLimits</value>
            </item>
            <item>
              <name>WantMatchDiagnostics</name>
              <type>BOOLEAN-ATTR</type>
              <value>TRUE</value>
            </item>
            <item>
              <name>StartdIpAddr</name>
              <type>STRING-ATTR</type>
              <value>
              &lt;192.168.107.12:40000?CCBID=142.104.63.35:9618#608&gt;</value>
            </item>
            <item>
              <name>AutoClusterId</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>LastMatchTime</name>
              <type>INTEGER-ATTR</type>
              <value>1274118697</value>
            </item>
            <item>
              <name>NumShadowStarts</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>CurrentTime</name>
              <type>EXPRESSION-ATTR</type>
              <value>time()</value>
            </item>
            <item>
              <name>JobRunCount</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>RemoteSlotID</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>LastSuspensionTime</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>Out</name>
              <type>STRING-ATTR</type>
              <value>x.out</value>
            </item>
            <item>
              <name>VMLoc</name>
              <type>STRING-ATTR</type>
              <value>
              http://vmrepo.phys.uvic.ca/vms/canfarbase_i386.img.gz</value>
            </item>
            <item>
              <name>BufferBlockSize</name>
              <type>INTEGER-ATTR</type>
              <value>32768</value>
            </item>
            <item>
              <name>JobNotification</name>
              <type>INTEGER-ATTR</type>
              <value>2</value>
            </item>
            <item>
              <name>TransferFiles</name>
              <type>STRING-ATTR</type>
              <value>ONEXIT</value>
            </item>
            <item>
              <name>JobLeaseDuration</name>
              <type>INTEGER-ATTR</type>
              <value>1200</value>
            </item>
            <item>
              <name>ImageSize_RAW</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>StreamOut</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
            <item>
              <name>NumRestarts</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>Cmd</name>
              <type>STRING-ATTR</type>
              <value>/home/patricka/dumb.sh</value>
            </item>
            <item>
              <name>ImageSize</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>LeaveJobInQueue</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
            <item>
              <name>PeriodicRemove</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
            <item>
              <name>Iwd</name>
              <type>STRING-ATTR</type>
              <value>/home/patricka</value>
            </item>
            <item>
              <name>PeriodicHold</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
            <item>
              <name>CondorPlatform</name>
              <type>STRING-ATTR</type>
              <value>$CondorPlatform: I386-LINUX_RHEL5 $</value>
            </item>
            <item>
              <name>NumCkpts</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>ExitBySignal</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
            <item>
              <name>EnteredCurrentStatus</name>
              <type>INTEGER-ATTR</type>
              <value>1274118681</value>
            </item>
            <item>
              <name>In</name>
              <type>STRING-ATTR</type>
              <value>/dev/null</value>
            </item>
            <item>
              <name>ClusterId</name>
              <type>INTEGER-ATTR</type>
              <value>85</value>
            </item>
            <item>
              <name>RemoteUserCpu</name>
              <type>FLOAT-ATTR</type>
              <value>0.000000</value>
            </item>
            <item>
              <name>CondorVersion</name>
              <type>STRING-ATTR</type>
              <value>$CondorVersion: 7.5.1 Mar 1 2010 BuildID:
              220663 $</value>
            </item>
            <item>
              <name>NumSystemHolds</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>MinHosts</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>WantRemoteSyscalls</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
            <item>
              <name>JobUniverse</name>
              <type>INTEGER-ATTR</type>
              <value>5</value>
            </item>
            <item>
              <name>Environment</name>
              <type>STRING-ATTR</type>
              <value></value>
            </item>
            <item>
              <name>PeriodicRelease</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
            <item>
              <name>RequestDisk</name>
              <type>EXPRESSION-ATTR</type>
              <value>DiskUsage</value>
            </item>
            <item>
              <name>CumulativeSuspensionTime</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>ExecutableSize</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>RootDir</name>
              <type>STRING-ATTR</type>
              <value>/</value>
            </item>
            <item>
              <name>Requirements</name>
              <type>EXPRESSION-ATTR</type>
              <value>( VMType =?= "canfarbase" ) &amp;&amp; (
              TARGET.Arch == "INTEL" ) &amp;&amp; ( TARGET.OpSys ==
              "LINUX" ) &amp;&amp; ( TARGET.Disk &gt;= DiskUsage )
              &amp;&amp; ( ( ( TARGET.Memory * 1024 ) &gt;=
              ImageSize ) &amp;&amp; ( ( RequestMemory * 1024 )
              &gt;= ImageSize ) ) &amp;&amp; (
              TARGET.HasFileTransfer )</value>
            </item>
            <item>
              <name>ShouldTransferFiles</name>
              <type>STRING-ATTR</type>
              <value>YES</value>
            </item>
            <item>
              <name>LocalSysCpu</name>
              <type>FLOAT-ATTR</type>
              <value>0.000000</value>
            </item>
            <item>
              <name>DiskUsage</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>WhenToTransferOutput</name>
              <type>STRING-ATTR</type>
              <value>ON_EXIT</value>
            </item>
            <item>
              <name>UserLog</name>
              <type>STRING-ATTR</type>
              <value>/home/patricka/x.log</value>
            </item>
            <item>
              <name>RequestMemory</name>
              <type>EXPRESSION-ATTR</type>
              <value>ceiling(ifThenElse(JobVMMemory =!=
              undefined,JobVMMemory,ImageSize /
              1024.000000))</value>
            </item>
            <item>
              <name>KillSig</name>
              <type>STRING-ATTR</type>
              <value>SIGTERM</value>
            </item>
            <item>
              <name>NumCkpts_RAW</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>ExecutableSize_RAW</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>MaxHosts</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>CoreSize</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>WantCheckpoint</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
            <item>
              <name>Err</name>
              <type>STRING-ATTR</type>
              <value>x.error</value>
            </item>
            <item>
              <name>CurrentHosts</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>VMStorage</name>
              <type>INTEGER-ATTR</type>
              <value>10</value>
            </item>
            <item>
              <name>VMAMI</name>
              <type>STRING-ATTR</type>
              <value>ami-fdee0094</value>
            </item>
            <item>
              <name>VMMaximumPrice</name>
              <type>INTEGER-ATTR</type>
              <value>15</value>
            </item>
            <item>
              <name>DiskUsage_RAW</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>CommittedTime</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>RemoteSysCpu</name>
              <type>FLOAT-ATTR</type>
              <value>0.000000</value>
            </item>
            <item>
              <name>OnExitRemove</name>
              <type>BOOLEAN-ATTR</type>
              <value>TRUE</value>
            </item>
            <item>
              <name>TotalSuspensions</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>RequestCpus</name>
              <type>INTEGER-ATTR</type>
              <value>1</value>
            </item>
            <item>
              <name>LocalUserCpu</name>
              <type>FLOAT-ATTR</type>
              <value>0.000000</value>
            </item>
            <item>
              <name>StreamErr</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
            <item>
              <name>NiceUser</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
            <item>
              <name>QDate</name>
              <type>INTEGER-ATTR</type>
              <value>1274118681</value>
            </item>
            <item>
              <name>CompletionDate</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>Rank</name>
              <type>FLOAT-ATTR</type>
              <value>0.000000</value>
            </item>
            <item>
              <name>OnExitHold</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
            <item>
              <name>RemoteWallClockTime</name>
              <type>FLOAT-ATTR</type>
              <value>0.000000</value>
            </item>
            <item>
              <name>JobPrio</name>
              <type>INTEGER-ATTR</type>
              <value>10</value>
            </item>
            <item>
              <name>NumJobStarts</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>Args</name>
              <type>STRING-ATTR</type>
              <value>4 10</value>
            </item>
            <item>
              <name>WantRemoteIO</name>
              <type>BOOLEAN-ATTR</type>
              <value>TRUE</value>
            </item>
            <item>
              <name>User</name>
              <type>STRING-ATTR</type>
              <value>patricka@vmcgs35.phys.uvic.ca</value>
            </item>
            <item>
              <name>CurrentTime</name>
              <type>EXPRESSION-ATTR</type>
              <value>time()</value>
            </item>
            <item>
              <name>BufferSize</name>
              <type>INTEGER-ATTR</type>
              <value>524288</value>
            </item>
            <item>
              <name>ExitStatus</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>LastSuspensionTime</name>
              <type>INTEGER-ATTR</type>
              <value>0</value>
            </item>
            <item>
              <name>Owner</name>
              <type>STRING-ATTR</type>
              <value>patricka</value>
            </item>
            <item>
              <name>TransferIn</name>
              <type>BOOLEAN-ATTR</type>
              <value>FALSE</value>
            </item>
          </item>
        </classAdArray>
      </response>
    </condor:getJobAdsResponse>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""

        xml2native = cloudscheduler.job_management.JobPool._condor_job_xml_to_job_list
        jobs = xml2native(condor_xml)

        job_dictionary = {'GlobalJobId': 'vmcgs35.phys.uvic.ca#85.0#1274118681',
                          'Owner': 'patricka',
                          'JobPrio': 10,
                          'JobStatus': 2,
                          'ClusterId': 85,
                          'ProcId': 0,
                          'VMType': 'canfarbase',
                          'VMAMI': 'ami-fdee0094',
                          'VMLoc': 'http://vmrepo.phys.uvic.ca/vms/canfarbase_i386.img.gz',
                          'VMStorage': 10,
                          'VMMaximumPrice': 15,
                          }

        test_job = cloudscheduler.job_management.Job(**job_dictionary)
        parsed_job = jobs[0]
        self.assertEqual(parsed_job.id, test_job.id)
        self.assertEqual(parsed_job.user, test_job.user)
        self.assertEqual(parsed_job.priority, test_job.priority)
        self.assertEqual(parsed_job.job_status, test_job.job_status)
        self.assertEqual(parsed_job.cluster_id, test_job.cluster_id)
        self.assertEqual(parsed_job.proc_id, test_job.proc_id)
        self.assertEqual(parsed_job.req_vmtype, test_job.req_vmtype)
        self.assertEqual(parsed_job.req_network, test_job.req_network)
        self.assertEqual(parsed_job.req_cpuarch, test_job.req_cpuarch)
        self.assertEqual(parsed_job.req_image, test_job.req_image)
        self.assertEqual(parsed_job.req_imageloc, test_job.req_imageloc)
        self.assertEqual(parsed_job.req_ami, test_job.req_ami)
        self.assertEqual(parsed_job.req_memory, test_job.req_memory)
        self.assertEqual(parsed_job.req_cpucores, test_job.req_cpucores)
        self.assertEqual(parsed_job.req_storage, test_job.req_storage)
        self.assertEqual(parsed_job.keep_alive, test_job.keep_alive)
        self.assertEqual(parsed_job.instance_type, test_job.instance_type)
        self.assertEqual(parsed_job.maximum_price, test_job.maximum_price)

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
