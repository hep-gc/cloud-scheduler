'''
Created on Jun 23, 2014

@author: mhp
'''
import os
import logging
import urllib2
import cloudscheduler.config as config

log = logging.getLogger("cloudscheduler")

def inject_customizations(pre_init, cloud_init):
    """ Inject cloud init style customizations into an ami/cloud init script given by user. """
    # cloud init should be a list of file contents
    # Need to see if the write_files preamble exists
    found_write_files = False
    found_cloud_init = False
    index_of_write_files = 0
    splitscript = []
    for initscript in pre_init:
        splitscript = initscript.split('\n')
        
        for i, line in enumerate(splitscript): # need to iterate with index counts as well
            if line == '#cloud-config':
                found_cloud_init = True
            if line == 'write_files:':
                found_write_files = True
                index_of_write_files = i # save the index to insert at later
                break
            if found_write_files:
                break
    if not found_write_files:
        # no writes_files found - inject one at end and do customizations
        cloud_init.insert(0, 'write_files:')
        splitscript.append('\n'.join(cloud_init))
    else:
        # need to insert at the index
        splitscript.insert(index_of_write_files+1, '\n'.join(cloud_init))
    if not found_cloud_init:
        splitscript.insert(0, '#cloud-config')
    return '\n'.join(splitscript)
    
def build_write_files_cloud_init(custom_tasks):
    """
    Argument:
    custom_tasks -- A list of tuples were the first item is the file to edit
                    and the second is the string to place there. In these
                    tuples, the first element is the content, and the second
                    is the location. 3rd if present, is the permissions
    """
    cloud_init = []
    for task in custom_tasks:
        if not task[1][0] or task[1][0] != '/':
            continue
        cloud_init.append('-   content: |')
        formatted_task = []
        lines = task[0].split('\n')
        for line in lines:
            formatted_task.append(''.join(['        ', line]))
        
        cloud_init.append('\n'.join(formatted_task))
        cloud_init.append('    path: %s' % task[1])
        if len(task) > 2:
            cloud_init.append('    permissions: %s' % task[2])
    return cloud_init

def build_multi_mime_message(content_type_pairs, file_type_pairs):
    """
    Argument:
    content_type_pairs - A list of tuples [(content, mime-type, filename)]
    file_type_pairs -- A list of strings formatted as file-path : mime-type
    """
    import sys

    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    
    if len(file_type_pairs) == 0:
        return ""
    
    combined_message = MIMEMultipart()
    for i in file_type_pairs:
        #try:
        #    (filename, format_type) = i.split(":", 1)
        #    filename = filename.strip()
        #    format_type = format_type.strip()
        #except ValueError:
        #    filename = i
        #    format_type = "cloud-config"
        #if not os.path.exists(filename):
        #    log.error("Unable to find file: %s skipping" % filename)
        #    continue
        #with open(filename) as fh:
        #    contents = fh.read()
        (contents, format_type) = read_file_type_pairs(i)
        if contents == None or format_type == None:
            return None
        sub_message = MIMEText(contents, format_type, sys.getdefaultencoding())
        sub_message.add_header('Content-Disposition', 'attachment; filename="%s"' % (i))
        combined_message.attach(sub_message)
    for i in content_type_pairs:
        sub_message = MIMEText(i[0], i[1].strip(), sys.getdefaultencoding())
        if len(i) <= 3:
            sub_message.add_header('Content-Disposition', 'attachment; filename="%s"' % (i[2].strip()))
        else:
            sub_message.add_header('Content-Disposition', 'attachment; filename="%s"' % ("cs-cloud-init.yaml"))
        combined_message.attach(sub_message)
    
    return str(combined_message)

def read_file_type_pairs(file_type_pair):
    """
    :param file_type_pair: string in filepath:mimetype format - may be http:// based
    :return: tuple with content of the file, and content mime type
    """
    content = None
    format_type = None
    if file_type_pair.startswith('http'):
        try:
            (pre, http_loc, format_type) = file_type_pair.split(":", 2)
            http_loc = ':'.join([pre, http_loc])
            http_loc = http_loc.strip()
            format_type = format_type.strip()
        except ValueError:
            if len(file_type_pair.split(":")) == 2: # missing the content type
                http_loc = file_type_pair.strip()
                format_type = "cloud-config"
        try:
            content = urllib2.urlopen(http_loc).read()
        except Exception as e:
            log.error("Unable to read url: %s" % http_loc)
            return (None, None)
    else:
        try:
            (filename, format_type) = file_type_pair.split(":", 1)
            filename = filename.strip()
            format_type = format_type.strip()
        except ValueError:
            filename = file_type_pair
            format_type = "cloud-config"
        if not os.path.exists(filename):
            log.error("Unable to find file: %s skipping" % filename)
            return (None, None)
        with open(filename) as fh:
            content = fh.read()

    if len(content) == 0:
        return (None, None)

    return (content, format_type)

def validate_yaml(content):
    """ Try to load yaml to see if it passes basic validation."""
    try:
        import yaml
        y = yaml.load(content)
        if not y.has_key('merge_type'):
            log.error("Yaml submitted without a merge_type.")
            return "Missing merge_type:"
    except yaml.YAMLError as e:
        log.error("Problem validating yaml: %s" % e)
        return ' '.join(['Line: ', str(e.problem_mark.line), ' Col: ', str(e.problem_mark.column)]) # use e.problem_mark.[name,column,line]
    except UnboundLocalError as e:
        log.error("Caught an exception trying to validate yaml. Is the pyyaml module installed?")
    return None
