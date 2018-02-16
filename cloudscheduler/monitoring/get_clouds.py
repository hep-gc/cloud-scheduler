#!/usr/bin/python

"""*
 * Copyright 2009 University of Victoria
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * AUTHOR - Adam Bishop - ahbishop@uvic.ca
 *
 * For comments or questions please contact the above e-mail address
 * or Ian Gable - igable@uvic.ca
 *
 * """


import ConfigParser
import sys
import os
from redis import Redis, ConnectionError
from cloudscheduler.monitoring.cloud_logger import Logger
import amara

RET_CRITICAL = -1

CONF_FILE = "get_clouds.cfg"
CONF_FILE_SECTION = "Get_Clouds"
REDISDB_SERVER_HOSTNAME = "RedisDB_Server_Hostname"
REDISDB_SERVER_PORT = "RedisDB_Server_Port"
SERVER_TMP_LOCATION = "Server_Tmp_Location"
CLOUDS_DB = "Clouds_RedisDB_Num"
CLOUDS_KEY = "Clouds_Key"

ConfigMapping = {}

def loadGetCloudsClientConfig(logger):
    """
    This global method loads all the user configured options from the configuration file
    and saves them into the global ConfigMapping dictionary
    """

    cfgFile = ConfigParser.ConfigParser()
    # Prevent an exception from being generated should the config file
    # not be found in the current directory
    if os.path.exists(CONF_FILE):
        cfgFile.read(CONF_FILE)
        try:
            ConfigMapping[SERVER_TMP_LOCATION] = cfgFile.get(CONF_FILE_SECTION,
                                                             SERVER_TMP_LOCATION, 0)
            ConfigMapping[REDISDB_SERVER_HOSTNAME] = cfgFile.get(CONF_FILE_SECTION,
                                                                 REDISDB_SERVER_HOSTNAME, 0)
            ConfigMapping[REDISDB_SERVER_PORT] = cfgFile.get(CONF_FILE_SECTION,
                                                             REDISDB_SERVER_PORT, 0)
            ConfigMapping[CLOUDS_DB] = cfgFile.get(CONF_FILE_SECTION, CLOUDS_DB, 0)
            ConfigMapping[CLOUDS_KEY] = cfgFile.get(CONF_FILE_SECTION, CLOUDS_KEY, 0)
        except ConfigParser.NoSectionError:
            logger.error("Unable to locate "+CONF_FILE_SECTION+" section in conf file \
                          - Malformed config file?")
            sys.exit(RET_CRITICAL)
        except ConfigParser.NoOptionError as nopt:
            logger.error(nopt.message+" of configuration file")
            sys.exit(RET_CRITICAL)
    else:
        logger.error("Configuration file not found in this file's directory!")
        sys.exit(RET_CRITICAL)

class getCloudsClient(object):

    """
     This class is responsible for querying the RedisDB for the Sky XML and binding it back into
     a usable data structure. This data structure format is a nested Dictionary of dictionaries
     and lists. The exact format and names for various keys mimics the public XML format. This
     means that this class is dependant on the public XML format - If the XML changes this class
     needs to be updated accordingly. This dependancy is unavoidable conceptually, and the use of
     the Amara utility to provide the binding mechanism requires this knowledge.

    """
    def __init__(self, logger=None):
        """Constructor for getClient."""
        if logger:
            self.logger = logger
        else:
            self.logger = Logger("get_clouds", "get_clouds.log")

        loadGetCloudsClientConfig(self.logger)

        self.db = Redis(db=ConfigMapping[CLOUDS_DB], host=ConfigMapping[REDISDB_SERVER_HOSTNAME],
                        port=int(ConfigMapping[REDISDB_SERVER_PORT]))
        try:
            self.db.ping()
        except ConnectionError:
            self.logger.error("ConnectionError pinging DB - redis-server running on desired port?")
            sys.exit(RET_CRITICAL)

    def _lookupCloudsXML(self):
        """Get the aggregate XML file from redis."""
        # Query the Redis DB for the complete, aggregated XML file
        clouds_xml = self.db.get(ConfigMapping[CLOUDS_KEY])
        # amDoc will contain a data structure that mirrors the public XML Schema,
        # allowing for direct access via name.name2.name3 style
        am_doc = amara.parse(str(clouds_xml))

        return am_doc

    @staticmethod
    def _getWorkerNodeVirtualizationTech(node):
        """Get the virtualization info from a node."""
        return str(node.VirtualizationTech.Type)

    @staticmethod
    def _getWorkerNodeCPUID(node):
        """Get the CPU id of a node."""
        return str(node.CPUID.Description)

    @staticmethod
    def _getWorkerNodeCPUCores(node):
        """Get the number of cores of a node."""
        return str(node.CPUCores)

    @staticmethod
    def _getWorkerNodeMem(node):
        """Get the memory info of a node."""
        total_dict = {}

        total_dict["TotalMB"] = str(node.Memory.TotalMB)
        total_dict["FreeMB"] = str(node.Memory.FreeMB)

        return total_dict

    def _populateWorkerNode(self, node):
        """Construct worker node dict."""
        t_dict = {}
        t_dict["CPUCores"] = self._getWorkerNodeCPUCores(node)
        t_dict["Memory"] = self._getWorkerNodeMem(node)
        t_dict["VirtualizationTech"] = self._getWorkerNodeVirtualizationTech(node)
        t_dict["CPUID"] = self._getWorkerNodeCPUID(node)

        ret_dict = {"Node": t_dict}
        return ret_dict

    def _getCloudWorkerNodes(self, cloud):
        """Get all nodes for a cloud."""
        temp_nodes = []
        for curnode in cloud.WorkerNodes.Node:
            temp_nodes.append(self._populateWorkerNode(curnode))

        return temp_nodes

    @staticmethod
    def _getCloudVMMemoryPools(cloud):
        """Get all Memory pools for a cloud."""
        temp_nodes = []
        for curnode in cloud.VMM_Pools.Pool:
            t_dict = {}
            # This loop gives all the children of the 'Pool' XML node
            for entry in  curnode.xml_properties.keys():
                t_dict[entry] = str(curnode.xml_properties[entry])
            temp_nodes.append(t_dict)
        return temp_nodes

    @staticmethod
    def _getCloudNetworkPools(cloud):
        """Get network pools for a cloud."""
        temp_nodes = []

        for curnode in cloud.Network_Pools.Pool:
            t_dict = {}
            # This loop gives all the children of the 'Pool' XML node
            for entry in curnode.xml_properties.keys():
                t_dict[entry] = str(curnode.xml_properties[entry])
            temp_nodes.append(t_dict)
        return temp_nodes

    @staticmethod
    def _getCloudServiceData(cloud):
        """Get the service info for a cloud."""
        t_dict = {}
        t_dict["Path"] = str(cloud.Service.Path)
        t_dict["HostName"] = str(cloud.Service.HostName)
        t_dict["Type"] = str(cloud.Service.Type)
        t_dict["Port"] = str(cloud.Service.Port)
        t_dict["IP"] = str(cloud.Service.IP)

        return t_dict

    @staticmethod
    def _getCloudIaaSDiagnostics(cloud):
        """Get the IaaS Diagnostics info for a cloud."""
        t_dict = {}
        t_dict["InternalRepresentation"] = str(cloud.IaasDiagnostics.InternalRepresentation)

        return t_dict

    def getCloudsView(self):
        """get the overall view of all clouds."""
        clouds = []
        boundXML = self._lookupCloudsXML()

        for cloud in boundXML.Sky.Cloud:

            cloudDescriptor = {}
            cloudDescriptor["Service"] = self._getCloudServiceData(cloud)
            cloudDescriptor["IaaSDiagnostics"] = self._getCloudIaaSDiagnostics(cloud)
            cloudDescriptor["VMMemoryPools"] = self._getCloudVMMemoryPools(cloud)
            cloudDescriptor["WorkerNodes"] = self._getCloudWorkerNodes(cloud)
            cloudDescriptor["NetworkPools"] = self._getCloudNetworkPools(cloud)
            clouds.append(cloudDescriptor)
        return clouds

if __name__ == '__main__':

    cloud_client = getCloudsClient()
    cloud_view = cloud_client.getCloudsView()

    print(cloud_view)
