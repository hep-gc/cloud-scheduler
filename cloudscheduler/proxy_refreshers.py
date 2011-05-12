import os
import threading
import time
import traceback
import datetime
import subprocess
import tempfile
import shutil
import shlex

import cloudscheduler.config as config
import cloudscheduler.utilities as utilities
import cloudscheduler.job_management as job_management
import cloudscheduler.cloud_management as cloud_management

log = utilities.get_cloudscheduler_logger()

class JobProxyRefresher(threading.Thread):
    """
    JobProxyRefresher - Periodically checks the expiry time on job user proxies and attempt
                        to renew the ones about to expire using MyProxy.
    """


    def __init__(self, job_pool):
        threading.Thread.__init__(self, name=self.__class__.__name__)
        self.job_pool = job_pool
        self.quit = False
        self.polling_interval = config.job_proxy_refresher_interval # proxy expiry time poll interval, in seconds

    def stop(self):
        log.debug("Waiting for job proxy refresher loop to end")
        self.quit = True

    def run(self):
        try:
            log.info("Starting JobProxyRefresher thread...")

            while not self.quit:
                # The following timestamp is use to time this proxy refresh cycle.
                cycle_start_ts = datetime.datetime.today()

                jobs = self.job_pool.job_container.get_all_jobs()
                log.debug("Refreshing job user proxies. [%d proxies to process]" % (len(jobs)))
                for job in jobs:
                    log.debug("Proxy for job %s expires on: %s" % (job.id, job.get_x509userproxy_expiry_time()))
                    if job.is_proxy_expired():
                        log.warning("Proxy for job %s is expired.  Skipping proxy renewal for this job." % (job.id))
                    elif job.needs_proxy_renewal():
                        if job.get_myproxy_creds_name() != None:
                            log.debug("Renewing proxy %s for job %s" % (job.get_x509userproxy(), job.id))
                            if MyProxyProxyRefresher().renew_proxy(job.get_x509userproxy(), job.get_myproxy_creds_name(), job.get_myproxy_server(), job.get_myproxy_server_port()):
                                # Yay, proxy renewal worked! :-)
                                log.debug("Proxy for job %s renewed." % (job.id))
                                # Don't forget to reset the proxy expiry time cache.
                                job.reset_x509userproxy_expiry_time()
                            else:
                                log.error("Error renewing proxy for job %s" % (job.id))
                        else:
                            # If we get here, this means that the proxy should be renewed, but there
                            # is not MyProxy info for that job's proxy.  Not an error; just that the
                            # owner of the job didn't give any MyProxy information to renew the
                            # credentials.
                            log.debug("Not renewing proxy for job %s because missing MyProxy info." % (job.id))
                    else:
                        log.debug("No need to renew proxy for job %s" % (job.id))

                # Lets record the current time and then log how much time the cycle took.
                cycle_end_ts = datetime.datetime.today()
                log.debug("Job proxy refreshing cycle done. [%s -> %s (%s)]" % (cycle_start_ts, cycle_end_ts, cycle_end_ts - cycle_start_ts))

                log.debug("JobProxyRefresher waiting %ds..." % self.polling_interval)
                sleep_tics = self.polling_interval
                while (not self.quit) and sleep_tics > 0:
                    time.sleep(1)
                    sleep_tics -= 1

            log.debug("Exiting JobProxyRefresher thread")
        except:
            log.error("Error in JobProxyRefresher thread.")
            log.error(traceback.format_exc())

class VMProxyRefresher(threading.Thread):
    """
    VMProxyRefresher - Periodically checks the expiry time on VM user proxies and attempt
                        to renew the ones about to expire using MyProxy.
    """


    def __init__(self, cloud_resources):
        threading.Thread.__init__(self, name=self.__class__.__name__)
        self.cloud_resources = cloud_resources
        self.quit = False
        self.polling_interval = config.vm_proxy_refresher_interval # proxy expiry time poll interval, in seconds

    def stop(self):
        log.debug("Waiting for VM proxy refresher loop to end")
        self.quit = True

    def run(self):
        try:
            log.info("Starting VMProxyRefresher thread...")

            while not self.quit:
                # The following timestamp is use to time this proxy refresh cycle.
                cycle_start_ts = datetime.datetime.today()

                vms = self.cloud_resources.get_all_vms()
                log.debug("Refreshing VM proxies. [%d proxies to process]" % (len(vms)))
                for vm in vms:
                    log.debug("Proxy for VM %s expires on: %s" % (vm.id, vm.get_x509userproxy_expiry_time()))
                    if vm.is_proxy_expired():
                        log.warning("Proxy for VM %s is expired.  Skipping proxy renewal for this VM." % (vm.id))
                    elif vm.needs_proxy_renewal():
                        if vm.get_myproxy_creds_name() != None:
                            log.debug("Renewing proxy %s for VM %s" % (vm.get_proxy_file(), vm.id))
                            if MyProxyProxyRefresher().renew_proxy(vm.get_proxy_file(), vm.get_myproxy_creds_name(), vm.get_myproxy_server(), vm.get_myproxy_server_port()):
                                # Yay, proxy renewal worked! :-)
                                log.debug("Proxy for VM %s renewed." % (vm.id))
                                # Don't forget to reset the proxy expiry time cache.
                                vm.reset_x509userproxy_expiry_time()
                            else:
                                log.error("Error renewing proxy for VM %s" % (vm.id))
                        else:
                            # If we get here, this means that the proxy should be renewed, but there
                            # is no MyProxy info for that vm's proxy.  Not an error; just that the
                            # owner of the vm didn't give any MyProxy information to renew the
                            # credentials.
                            log.debug("Not renewing proxy for VM %s because missing MyProxy info." % (vm.id))
                    else:
                        log.debug("No need to renew proxy for VM %s" % (vm.id))

                # Lets record the current time and then log how much time the cycle took.
                cycle_end_ts = datetime.datetime.today()
                log.debug("VM proxy refreshing cycle done. [%s -> %s (%s)]" % (cycle_start_ts, cycle_end_ts, cycle_end_ts - cycle_start_ts))

                log.debug("VMProxyRefresher waiting %ds..." % self.polling_interval)
                sleep_tics = self.polling_interval
                while (not self.quit) and sleep_tics > 0:
                    time.sleep(1)
                    sleep_tics -= 1

            log.debug("Exiting VMProxyRefresher thread")
        except:
            log.error("Error in VMProxyRefresher thread.")
            log.error(traceback.format_exc())


class MyProxyProxyRefresher():
    """
    Utility class used to refresh a proxy using a MyProxy server.
    """
    # This method will call the MyProxy commands to renew the credential for a given job.
    # 
    # Returns True on sucess, False otherwise.
    def renew_proxy(self, proxy_file_path, myproxy_creds_name, myproxy_server, myproxy_server_port):
        if proxy_file_path == None:
            log.error("Attemp to renew proxy for job with no proxy.  Aborting proxy renew operation.")
            return False

        if myproxy_creds_name == None:
            log.error("Missing MyProxy credential name for job %s" % (job.id))
            return False

        if myproxy_server == None:
            log.warning("MyProxy credential name given but missing MyProxy server host. Defaulting to localhost")
            myproxy_server = "localhost"

        if myproxy_server_port == None:
            myproxy_server_port = "7512"

        myproxy_command = config.myproxy_logon_command
        if not os.path.isabs(myproxy_command):
            try:
                myproxy_command = utilities.get_globus_path(executable=myproxy_command) + myproxy_command
            except:
                log.exception("Problem getting myproxy-logon path")
                return None


        # Note: Here we put the refreshed proxy in a seperate file.  We do this to protect the ownership and permisions on the
        # original user's proxy in case the condor_submit was run on the same machine as the cloud scheduler.  If we do not do
        # this, then the cloud scheduler will overwrite the user's proxy with the renewed proxy, but will be owned by root; so
        # user will not be able to create proxies anymore because he/she won't have permission to overwrite the proxy file
        # created by the cloud scheduler.
        # Once the renewed proxy is in the temporary file, we then copy it over the original job's proxy, without changing its
        # permissions.
        #
        # This is a bit of a hack; there must me a better way to handle this problem.
        (new_proxy_file, new_proxy_file_path) = tempfile.mkstemp(suffix='.csRenewedProxy')
        os.close(new_proxy_file)
        myproxy_logon_cmd = '%s -s %s -p %s -k "%s" -a %s -o %s -d -v' % (myproxy_command, myproxy_server, myproxy_server_port, myproxy_creds_name, proxy_file_path, new_proxy_file_path)
        cmd_args = shlex.split(myproxy_logon_cmd)
        log.debug('myproxy-logon command: %s' % (cmd_args))
        log.debug('Invoking myproxy-logon command to refresh proxy %s ...' % (proxy_file_path))
        myproxy_logon_process = subprocess.Popen(cmd_args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = myproxy_logon_process.communicate()
        log.debug('myproxy-logon command returned %d' % (myproxy_logon_process.returncode))
        if myproxy_logon_process.returncode != 0:
            log.error("Error renewing proxy from MyProxy server: %s %s" % (stdout, stderr))
            log.debug('(Cleanup) Deleting %s ...' % (new_proxy_file_path))
            os.remove(new_proxy_file_path)
            return False
        log.debug('Copying %s to %s ...' % (new_proxy_file_path, proxy_file_path))
        shutil.copyfile(new_proxy_file_path, proxy_file_path)
        log.debug('(Cleanup) Deleting %s ...' % (new_proxy_file_path))
        os.remove(new_proxy_file_path)

        return True

class ProxyReplaceException(Exception):
    pass

class ProxyReplacer():
    """
    ProxyReplacer - This class is used to replace a job or VM proxy with
    another one (from another Condor job).
    """


    def __init__(self):
        pass

    def process_proxy_replace_jobs(proxy_replace_jobs, job_container, cluster):
        for proxy_replace_job_classad in proxy_replace_jobs:
            try:
                if 'userProxyOverwriteTargetJob' in proxy_replace_job_classad:
                    target_job = job_container.get_job_by_id(proxy_replace_job_classad['userProxyOverwriteTargetJob'])
                    if target_job != None:
                        this.replace_job_proxy(proxy_replace_job_classad, target_job)
                if 'userProxyOverwriteTargetVM' in proxy_replace_job_classad:
                    target_vm = cluster.get_vm(proxy_replace_job_classad['userProxyOverwriteTargetVM'])
                    # TODO: The above line will make a linear search to access each VM.
                    # OK for small number of VMs, but might need to be optimized for large number
                    # of VMs.
                    if target_vm != None:
                        this.replace_vm_proxy(proxy_replace_job_classad, target_vm)


                # Let's not forget to remove the proxy replace job from the condor_q
                jobid = proxy_replace_job_classad['GlobalJobId'].split('#')[1]
                try:
                    cmd = config.condor_rm_command + ' ' + jobid)
                    condor_rm = shlex.split(cmd)
                    sp = subprocess.Popen(condor_rm, shell=False,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    (condor_out, condor_err) = sp.communicate(input=None)
                    returncode = sp.returncode
                    if returncode == 0:
                        log.debug('Proxy replace job %s removed from condor queue.' % (jobid))
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



    def replace_job_proxy(src_classad, target_job):
        proxy_to_replace = target_job.get_x509userproxy()
        source_proxy = src_classad['x509userproxy']
        if (source_proxy != None) and ('Iwd' in src_classad):
            source_proxy = src_classad['Iwd'] + '/' + source_proxy
        this.replace_proxy(source_proxy, proxy_to_replace)

    def replace_vm_proxy(src_classad, target_vm):
        proxy_to_replace = target_vm.get_x509userproxy()
        source_proxy = src_classad['x509userproxy']
        if (source_proxy != None) and ('Iwd' in src_classad):
            source_proxy = src_classad['Iwd'] + '/' + source_proxy
        this.replace_proxy(source_proxy, proxy_to_replace)
        

    def _replace_proxy(source_proxy, destination_proxy):
        # Check to make sure source and destination are valid.
        if destination_proxy == None:
            raise ProxyReplaceException('Attempt to replace proxy for target job with no user proxy defined.')
        if source_proxy == None:
            raise ProxyReplaceException('Attempt to replace proxy with no source proxy.')

        # IMPORTANT: Check to make sure that both proxy DNs match.
        if utilities.get_cert_DN(destination_proxy) != utilities.get_cert_DN(source_proxy):
            # Proxy DNs do not match!  Log a warning and abort operation.
            raise ProxyReplaceException('Attempt to replace a proxy with DN mismatch.  Operation aborted.')

        # Replace proxy: overwrite target with source proxy
        previous_expiry_time = utilities.get_cert_expiry_time(destination_proxy)
        try:
            shutil.copy2(source_proxy, destination_proxy)
        except Exception, e:
            raise ProxyReplaceException('Error replacing proxy %s with %s.\n%s' % (destination_proxy, source_proxy, e))
        new_expiry_time =  utilities.get_cert_expiry_time(destination_proxy)

        log.debug('Proxy %s [%s] successfully replace with %s [%s]' % (source_proxy, previous_expiry_time, destination_proxy, new_expiry_time))
        return

