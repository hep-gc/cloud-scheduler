'''
Created on Jun 23, 2014

@author: mhp
'''

def inject_customizations(pre_init, cloud_init):
    """ Inject cloud init style customizations into an ami/cloud init script given by user
    Replaces the old nimbus style xml. """
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
        pre_init.append('\n'.join(cloud_init))
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
                    is the location
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
    return cloud_init
        