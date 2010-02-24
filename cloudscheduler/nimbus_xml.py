#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

# Copyright (C) 2009 University of Victoria
# You may distribute under the terms of either the GNU General Public
# License or the Apache v2 License, as specified in the README file.

# A script containing a factory for creating workspace metadata and 
# deployment request xml files for Nimbus workspace commands.


import os
import tempfile
import xml.dom.minidom


## Global Variables for xml population (VM and VM host machine information)
# Deployment request constants
PARTITION_NAME = "blankdisk1"       # Goes in both deployment and metadata files
SHUTDOWN_MECH = "Trash"
BLANKSPACE_MOUNT = "sdb"

# Metadata constants
NAME_URI_LVL = "http://"
VM_NIC = "eth0"
ACQUISITION_METHOD = "AllocateAndConfigure"
VIRT_TYPE = "Xen"
VIRT_VERSION = "3"
VM_MOUNT = "sda"
VM_PERMISSIONS = "ReadWrite"


# A support method to translate a time int to the supported duration time format
# Format: PT##M
# EG: PT30M
def format_duration_time(time):
    return ("PT" + str(time) + "M")

# A support method for converting the vm storage string (a string rep'ing
# gigs of storage) to the deployment request file format (a string of the
# number of megs of storage desired)
def format_storage(storage_gb):
    return str(int(storage_gb) * 1024) 


# Creates and returns a Nimbus deployment request XML string.
def ws_deployment_factory(vm_duration, vm_targetstate, vm_mem, vm_storage, vm_nodes):
    
    # Namespace variables for populating the xml file
    root_nmspc = "http://www.globus.org/2008/06/workspace/negotiable"
    jsdl_nmspc = "http://schemas.ggf.org/jsdl/2005/11/jsdl"
    xsi_nmspc  = "http://www.w3.org/2001/XMLSchema-instance"
    
    # Create the XML doc
    doc = xml.dom.minidom.Document()
    
    ##
    ## Create XML document heirarchy
    ##
    
    # Create the WorkspaceDeployment (root) element
    wsd_el = doc.createElementNS(root_nmspc, "WorkspaceDeployment")
    
    # Define namespaces in the root element
    wsd_el.setAttribute("xmlns", root_nmspc )
    wsd_el.setAttribute("xmlns:jsdl", jsdl_nmspc)
    wsd_el.setAttribute("xmlns:xsi", xsi_nmspc)
    
    # Add the Workspace deployment element (first, to be root)
    doc.appendChild(wsd_el)
        
    #-lvl 1
    deploymenttime_el = doc.createElementNS(root_nmspc, "DeploymentTime")
    wsd_el.appendChild(deploymenttime_el)
    #--lvl2
    minduration_el = doc.createElementNS(root_nmspc, "minDuration")
    deploymenttime_el.appendChild(minduration_el)
    
    #-lvl 1
    initialstate_el = doc.createElementNS(root_nmspc, "InitialState")
    wsd_el.appendChild(initialstate_el)
    
    #-lvl 1
    rsrcallocation_el = doc.createElementNS(root_nmspc, "ResourceAllocation")
    wsd_el.appendChild(rsrcallocation_el)
    #--lvl 2
    memory_el = doc.createElementNS(jsdl_nmspc, "jsdl:IndividualPhysicalMemory")
    rsrcallocation_el.appendChild(memory_el)
    #---lvl 3
    exactmem_el = doc.createElementNS(jsdl_nmspc, "jsdl:Exact")
    memory_el.appendChild(exactmem_el)
    #--lvl 2
    storage_el = doc.createElementNS(root_nmspc, "Storage")
    rsrcallocation_el.appendChild(storage_el)
    #---lvl 3    
    entry_el = doc.createElementNS(root_nmspc, "entry")
    storage_el.appendChild(entry_el)
    #----lvl 4
    partitionname_el = doc.createElementNS(root_nmspc, "partitionName")
    entry_el.appendChild(partitionname_el)
    #----lvl 4
    diskspace_el = doc.createElementNS(jsdl_nmspc, "jsdl:IndividualDiskSpace")
    entry_el.appendChild(diskspace_el)
    #-----lvl 5
    exactdisk_el = doc.createElementNS(jsdl_nmspc, "jsdl:Exact")
    diskspace_el.appendChild(exactdisk_el)
    
    #-lvl 1
    nodenumber_el = doc.createElementNS(root_nmspc, "NodeNumber")
    wsd_el.appendChild(nodenumber_el)
    
    #-lvl 1
    shutdown_el = doc.createElementNS(root_nmspc, "ShutdownMechanism")
    wsd_el.appendChild(shutdown_el)
    
    ##
    ## Set field values
    ##
    
    minduration_txt = doc.createTextNode(format_duration_time(vm_duration))
    minduration_el.appendChild(minduration_txt)
    
    initialstate_txt = doc.createTextNode(vm_targetstate)
    initialstate_el.appendChild(initialstate_txt)
    
    exactmem_txt = doc.createTextNode(str(vm_mem))
    exactmem_el.appendChild(exactmem_txt)
    
    partitionname_txt = doc.createTextNode(PARTITION_NAME)
    partitionname_el.appendChild(partitionname_txt)
    
    exactdisk_txt = doc.createTextNode(format_storage(vm_storage))
    exactdisk_el.appendChild(exactdisk_txt)
    
    nodenumber_txt = doc.createTextNode(str(vm_nodes))
    nodenumber_el.appendChild(nodenumber_txt)
    
    shutdown_txt = doc.createTextNode(SHUTDOWN_MECH)
    shutdown_el.appendChild(shutdown_txt)
    
    ##
    ## Create output file. Write xml. Close file.
    ##
    
    (xml_out, file_name) = tempfile.mkstemp()

    # Note: toprettyxml causes parse errors with Sax
    #xml_out.write(doc.toprettyxml(encoding="utf-8"))
    os.write(xml_out, doc.toxml(encoding="utf-8"))
    os.close(xml_out)

    # Print document (in pretty)
    #print (doc.toprettyxml(encoding="utf-8"))

    # Return the filename of the created metadata file
    return file_name    



# Creates and returns a Nimbus workspace metadata XML string.
def ws_metadata_factory(vm_name, vm_networkassoc, vm_cpuarch, vm_imagelocation):

    # Namespace variables for populating the xml file
    root_nmspc = "http://www.globus.org/2008/06/workspace/metadata"
    def_nmspc  = "http://www.globus.org/2008/06/workspace/metadata/definition"
    log_nmspc  = "http://www.globus.org/2008/06/workspace/metadata/logistics"
    jsdl_nmspc = "http://schemas.ggf.org/jsdl/2005/11/jsdl"
    xsi_nmspc  = "http://www.w3.org/2001/XMLSchema-instance"

    # Create document
    doc = xml.dom.minidom.Document()

    # Create the VirtualWorkspace (root) element
    vws_el = doc.createElementNS(root_nmspc, "VirtualWorkspace")

    # Set document attributes (namespaces)
    vws_el.setAttribute("xmlns", root_nmspc)
    vws_el.setAttribute("xmlns:def", def_nmspc)
    vws_el.setAttribute("xmlns:log", log_nmspc)
    vws_el.setAttribute("xmlns:jsdl", jsdl_nmspc)
    vws_el.setAttribute("xmlns:xsi", xsi_nmspc)

    # Add the VirtualWorkspace to the document as the root element
    doc.appendChild(vws_el)

    # Create the name element, in the top namespace. Add name to VirtualWorkspace
    name_el = doc.createElementNS(root_nmspc, "name")
    vws_el.appendChild(name_el)

    # Create and add the logistics level-1  child
    logistics_el = doc.createElementNS(log_nmspc, "log:logistics")
    vws_el.appendChild(logistics_el)

    # Create and add the networking level-2 child
    networking_el = doc.createElementNS(log_nmspc, "log:networking")
    logistics_el.appendChild(networking_el)

    # Create and add the nic level-3 child
    nic_el = doc.createElementNS(log_nmspc, "log:nic")
    networking_el.appendChild(nic_el)

    # Create and add the name level-4 child
    log_name_el = doc.createElementNS(log_nmspc, "log:name")
    nic_el.appendChild(log_name_el)

    # Create and add the ipConfig level-4 child
    ipConfig_el = doc.createElementNS(log_nmspc, "log:ipConfig")
    nic_el.appendChild(ipConfig_el)

    # acquisitionMethod level-5
    acquisitionMethod_el = doc.createElementNS(log_nmspc, "log:acquisitionMethod")
    ipConfig_el.appendChild(acquisitionMethod_el)

    # association level-4
    association_el = doc.createElementNS(log_nmspc, "log:association")
    nic_el.appendChild(association_el)


    # Create and add the definition level-1 child
    definition_el = doc.createElementNS(def_nmspc, "def:definition")
    vws_el.appendChild(definition_el)

    # requirements level-2
    requirements_el = doc.createElementNS(def_nmspc, "def:requirements")
    definition_el.appendChild(requirements_el)

    # CPUArchitecture level-3
    CPUArch_el = doc.createElementNS(jsdl_nmspc, "jsdl:CPUArchitecture")
    requirements_el.appendChild(CPUArch_el)

    # CPUArchitectureName level-4
    CPUArchName_el = doc.createElementNS(jsdl_nmspc, "jsdl:CPUArchitectureName")
    CPUArch_el.appendChild(CPUArchName_el)

    # VMM level-3
    VMM_el = doc.createElementNS(def_nmspc, "def:VMM")
    requirements_el.appendChild(VMM_el)

    # type level-4
    type_el = doc.createElementNS(def_nmspc, "def:type")
    VMM_el.appendChild(type_el)

    # version level-4
    version_el = doc.createElementNS(def_nmspc, "def:version")
    VMM_el.appendChild(version_el)

    #--lvl 2
    diskCollection_el = doc.createElementNS(def_nmspc, "def:diskCollection")
    definition_el.appendChild(diskCollection_el)
    #---lvl 3
    rootVBD_el = doc.createElementNS(def_nmspc, "def:rootVBD")
    diskCollection_el.appendChild(rootVBD_el)
    #----lvl 4
    location_el = doc.createElementNS(def_nmspc, "def:location")
    rootVBD_el.appendChild(location_el)
    #----lvl 4
    mountAs_el = doc.createElementNS(def_nmspc, "def:mountAs")
    rootVBD_el.appendChild(mountAs_el)
    #----lvl 4
    permissions_el = doc.createElementNS(def_nmspc, "def:permissions")
    rootVBD_el.appendChild(permissions_el)
    #---lvl 3
    blankspacePartition_el = doc.createElementNS(def_nmspc, "def:blankspacePartition")
    diskCollection_el.appendChild(blankspacePartition_el)
    #----lvl 4
    partitionName_el = doc.createElementNS(def_nmspc, "def:partitionName")
    blankspacePartition_el.appendChild(partitionName_el)
    #----lvl 4
    mountAsPartition_el = doc.createElementNS(def_nmspc, "def:mountAs")
    blankspacePartition_el.appendChild(mountAsPartition_el)


    ##
    ## Set field values (separate from tree structure)
    ##

    # Create and set the name text value
    name_txt = doc.createTextNode(NAME_URI_LVL+vm_name)
    name_el.appendChild(name_txt)

    log_name_txt = doc.createTextNode(VM_NIC)
    log_name_el.appendChild(log_name_txt)

    acquisitionMethod_txt = doc.createTextNode(ACQUISITION_METHOD)
    acquisitionMethod_el.appendChild(acquisitionMethod_txt)

    association_txt = doc.createTextNode(vm_networkassoc)
    association_el.appendChild(association_txt)

    CPUArchName_txt = doc.createTextNode(vm_cpuarch)
    CPUArchName_el.appendChild(CPUArchName_txt)

    type_txt = doc.createTextNode(VIRT_TYPE)
    type_el.appendChild(type_txt)

    version_txt = doc.createTextNode(VIRT_VERSION)
    version_el.appendChild(version_txt)

    location_txt = doc.createTextNode(vm_imagelocation)
    location_el.appendChild(location_txt)

    # TODO: mountAs  may need to change (automatically / parameter?)
    mountAs_txt = doc.createTextNode(VM_MOUNT)
    mountAs_el.appendChild(mountAs_txt)

    permissions_txt = doc.createTextNode(VM_PERMISSIONS)
    permissions_el.appendChild(permissions_txt)
    
    partitionName_txt = doc.createTextNode(PARTITION_NAME)
    partitionName_el.appendChild(partitionName_txt)
    
    mountAsPartition_txt = doc.createTextNode(BLANKSPACE_MOUNT)
    mountAsPartition_el.appendChild(mountAsPartition_txt)

    ## Create output file. Write xml. Close file.
    (xml_out, file_name) = tempfile.mkstemp()

    # Note: toprettyxml causes parse errors with Sax
    #xml_out.write(doc.toprettyxml(encoding="utf-8"))
    os.write(xml_out, doc.toxml(encoding="utf-8"))
    os.close(xml_out)

    # Print document (in pretty)
    #print (doc.toprettyxml(encoding="utf-8"))

    # Return the filename of the created metadata file
    return file_name


## Main Functionality

# deployment request test
# ws_deployment_factory(vm_duration, vm_targetstate, vm_mem, vm_storage, vm_nodes):
#request_out = ws_deployment_factory("10000", "Running", 1024, 1, "1")
#print "Request file: %s" % request_out

#metadata_out = ws_metadata_factory("http://test_image/name/over::Passed", "public", \
#        "x86", "http://vmrepo.phys.uvic.ca/vms/dev-green_x86.img.gz")
#print "Metadata file: %s" % metadata_out

