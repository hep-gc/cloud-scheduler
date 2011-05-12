import os
import threading
import subprocess
import shutil
import shlex

import cloudscheduler.config as config
import cloudscheduler.utilities as utilities

log = utilities.get_cloudscheduler_logger()

class ProxyReplaceException(Exception):
    pass

class ProxyReplacer():
    """
    ProxyReplacer - This class is used to replace a job or VM proxy with
    another one (from another Condor job).
    """


    def __init__(self):
        pass

    def process_proxy_replace_jobs(self, proxy_replace_jobs, job_container, clusters = None):
        if job_container.is_empty():
            log.info('Empty job container.  Proxy refresh jobs ignored for now.')
            return

        for proxy_replace_job_classad in proxy_replace_jobs:
            try:
                if 'userProxyOverwriteTargetJob' in proxy_replace_job_classad:
                    target_job = job_container.get_job_by_id(proxy_replace_job_classad['userProxyOverwriteTargetJob'])
                    if target_job != None:
                        self.replace_job_proxy(proxy_replace_job_classad, target_job)
                    else:
                        log.warn('Could not fetch job %s while attempting to replace user proxy.' % (proxy_replace_job_classad['userProxyOverwriteTargetJob']))
                if (clusters != None) and ('userProxyOverwriteTargetVM' in proxy_replace_job_classad):
                    for cluster in clusters:
                        target_vm = cluster.get_vm(proxy_replace_job_classad['userProxyOverwriteTargetVM'])
                        # TODO: The above line will make a linear search to access each VM.
                        # OK for small number of VMs, but might need to be optimized for large number
                        # of VMs.
                        if target_vm != None:
                            self.replace_vm_proxy(proxy_replace_job_classad, target_vm)
                            break

                # Let's not forget to remove the proxy replace job from the condor_q
                jobid = proxy_replace_job_classad['GlobalJobId'].split('#')[1]
                try:
                    cmd = config.condor_rm_command + ' ' + jobid
                    condor_rm = shlex.split(cmd)
                    sp = subprocess.Popen(condor_rm, shell=False,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    (condor_out, condor_err) = sp.communicate(input=None)
                    returncode = sp.returncode
                    if returncode == 0:
                        log.info('Proxy replace job %s removed from condor queue.' % (jobid))
                    else:
                        raise ProxyReplaceException("Got non-zero return code '%s' from '%s'. stderr was: %s" %
                                          (returncode, cmd, condor_err))
                except:
                    raise ProxyReplaceException("Problem running %s, unexpected error" % cmd)
            except ProxyReplaceException, e:
                # Something went wrong processing the proxy replace job.
                # Log the problem and continue to the next proxy replace job, if any.
                log.exception(e)

            return



    def replace_job_proxy(self, src_classad, target_job):
        proxy_to_replace = target_job.get_x509userproxy()
        source_proxy = src_classad['x509userproxy']
        if (source_proxy != None) and ('Iwd' in src_classad):
            source_proxy = src_classad['Iwd'] + '/' + source_proxy
        self._replace_proxy(source_proxy, proxy_to_replace)

    def replace_vm_proxy(self, src_classad, target_vm):
        proxy_to_replace = target_vm.get_x509userproxy()
        source_proxy = src_classad['x509userproxy']
        if (source_proxy != None) and ('Iwd' in src_classad):
            source_proxy = src_classad['Iwd'] + '/' + source_proxy
        self._replace_proxy(source_proxy, proxy_to_replace)
        

    def _replace_proxy(self, source_proxy, destination_proxy):
        # Check to make sure source and destination are valid.
        if destination_proxy == None:
            raise ProxyReplaceException('Attempt to replace proxy for target job with no user proxy defined.')
        if source_proxy == None:
            raise ProxyReplaceException('Attempt to replace proxy with no source proxy.')

        # IMPORTANT: Check to make sure that both proxy identities match.
        if utilities.get_proxy_identity(destination_proxy) != utilities.get_proxy_identity(source_proxy):
            # Proxy identities do not match!  Log a warning and abort operation.
            raise ProxyReplaceException('Attempt to replace a proxy with identity mismatch.  Operation aborted.')

        # Replace proxy: overwrite target with source proxy
        previous_expiry_time = utilities.get_cert_expiry_time(destination_proxy)
        try:
            shutil.copy2(source_proxy, destination_proxy)
        except Exception, e:
            raise ProxyReplaceException('Error replacing proxy %s with %s.\n%s' % (destination_proxy, source_proxy, e))
        new_expiry_time =  utilities.get_cert_expiry_time(destination_proxy)

        log.info('Proxy %s [%s] successfully replace with %s [%s]' % (source_proxy, previous_expiry_time, destination_proxy, new_expiry_time))
        return

