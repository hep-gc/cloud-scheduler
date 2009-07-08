#!/usr/bin/python

## A script to develop a factory for creating workspace metadata files
## for Nimbus workspace commands.


import xml.dom.ext
import xml.dom.minidom	


def ws_metadata_factory(vm_name, network, cpu_arch, vm_location):
    
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

    # diskCollection level-2
    diskCollection_el = doc.createElementNS(def_nmspc, "def:diskCollection")
    definition_el.appendChild(diskCollection_el)

    # rootVBD level-3
    rootVBD_el = doc.createElementNS(def_nmspc, "def:rootVBD")
    diskCollection_el.appendChild(rootVBD_el)

    # location level-4
    location_el = doc.createElementNS(def_nmspc, "def:location")
    rootVBD_el.appendChild(location_el)

    # mountAs level-4
    mountAs_el = doc.createElementNS(def_nmspc, "def:mountAs")
    rootVBD_el.appendChild(mountAs_el)

    # permissions level-4
    permissions_el = doc.createElementNS(def_nmspc, "def:permissions")
    rootVBD_el.appendChild(permissions_el)

   
    ##
    ## Set field values (separate from tree structure)
    ##

    # Create and set the name text value
    name_txt = doc.createTextNode(vm_name)
    name_el.appendChild(name_txt)

    log_name_txt = doc.createTextNode("eth0")
    log_name_el.appendChild(log_name_txt)

    acquisitionMethod_txt = doc.createTextNode("AllocateAndConfigure")
    acquisitionMethod_el.appendChild(acquisitionMethod_txt)

    association_txt = doc.createTextNode(network)
    association_el.appendChild(association_txt)

    CPUArchName_txt = doc.createTextNode(cpu_arch)
    CPUArchName_el.appendChild(CPUArchName_txt)

    type_txt = doc.createTextNode("Xen")
    type_el.appendChild(type_txt)

    version_txt = doc.createTextNode("3")
    version_el.appendChild(version_txt)

    location_txt = doc.createTextNode(vm_location)
    location_el.appendChild(location_txt)

    # TODO: mountAs needs to change (automatically / parameter?)
    mountAs_txt = doc.createTextNode("sda")
    mountAs_el.appendChild(mountAs_txt)
    
    permissions_txt = doc.createTextNode("ReadWrite")
    permissions_el.appendChild(permissions_txt)

    ## Create output file. Write xml. Close file.
    ## (NOTE: Overwrites previous file)
    file_name = "tmp_nimbusmetadata.xml"
    xml_out = open(file_name, "w")
    xml_out.write(doc.toprettyxml(encoding="utf-8"))
    xml_out.close()

    # Print document (in pretty)
    # print (doc.toprettyxml(encoding="utf-8"))

    # Return the filename of the created metadata file
    return file_name


## Main Functionality

# tfo = ws_metadata_factory("http://test_image/name/over::Passed", "public", "x86", "http://some_image")
# print "Filename out: %s" %(tfo)

