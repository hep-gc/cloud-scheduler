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

from cloudscheduler.cluster_tools import VM
from job_management import Job

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
        self.heart_beat = time.time()
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
                log.verbose("Refreshing job user proxies. [%d proxies to process]" % (len(jobs)))
                for job in jobs:
                    jobcertextime = job.get_x509userproxy_expiry_time()
                    if jobcertextime:
                        log.verbose("Proxy for job %s expires in %s" % (job.id, jobcertextime - datetime.datetime.utcnow()))
                    if job.is_proxy_expired():
                        log.warning("Proxy for job %s is expired.  Skipping proxy renewal for this job." % (job.id))
                    elif job.needs_proxy_renewal():
                        if job.get_myproxy_creds_name() != None:
                            log.verbose("Renewing proxy %s for job %s" % (job.get_x509userproxy(), job.id))
                            if MyProxyProxyRefresher().renew_proxy(job.get_x509userproxy(), job.get_myproxy_creds_name(), job.get_myproxy_server(), job.get_myproxy_server_port(), job.get_renew_time()):
                                # Yay, proxy renewal worked! :-)
                                log.verbose("Proxy for job %s renewed." % (job.id))
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
                        log.verbose("No need to renew proxy for job %s" % (job.id))

                # Lets record the current time and then log how much time the cycle took.
                cycle_end_ts = datetime.datetime.today()
                log.verbose("Job proxy refreshing cycle done. [%d;%s;%s;%s]" % (len(jobs), cycle_start_ts, cycle_end_ts, cycle_end_ts - cycle_start_ts))

                log.verbose("JobProxyRefresher waiting %ds..." % self.polling_interval)
                sleep_tics = self.polling_interval
                self.heart_beat = time.time()
                while (not self.quit) and sleep_tics > 0:
                    time.sleep(1)
                    sleep_tics -= 1

            log.info("Exiting JobProxyRefresher thread")
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
        self.heart_beat = time.time()
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
                log.verbose("Refreshing VM proxies. [%d proxies to process]" % (len(vms)))
                for vm in vms:
                    vmcertextime = vm.get_x509userproxy_expiry_time()
                    if vmcertextime:
                        log.verbose("Proxy for VM %s expires in %s" % (vm.id, vmcertextime - datetime.datetime.utcnow()))
                    if vm.is_proxy_expired():
                        log.warning("Proxy for VM %s is expired.  Skipping proxy renewal for this VM." % (vm.id))
                    elif vm.needs_proxy_renewal():
                        if vm.get_myproxy_creds_name() != None:
                            log.debug("Renewing proxy %s for VM %s" % (vm.get_proxy_file(), vm.id))
                            if MyProxyProxyRefresher().renew_proxy(vm.get_proxy_file(), vm.get_myproxy_creds_name(), vm.get_myproxy_server(), vm.get_myproxy_server_port(), vm.get_renew_time()):
                                # Yay, proxy renewal worked! :-)
                                log.verbose("Proxy for VM %s renewed." % (vm.id))
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
                        log.verbose("No need to renew proxy for VM %s" % (vm.id))

                # Lets record the current time and then log how much time the cycle took.
                cycle_end_ts = datetime.datetime.today()
                log.verbose("VM proxy refreshing cycle done. [%d;%s;%s;%s]" % (len(vms), cycle_start_ts, cycle_end_ts, cycle_end_ts - cycle_start_ts))

                log.verbose("VMProxyRefresher waiting %ds..." % self.polling_interval)
                sleep_tics = self.polling_interval
                self.heart_beat = time.time()
                while (not self.quit) and sleep_tics > 0:
                    time.sleep(1)
                    sleep_tics -= 1

            log.info("Exiting VMProxyRefresher thread")
        except:
            log.error("Error in VMProxyRefresher thread.")
            log.error(traceback.format_exc())



class MyProxyProxyRefresher():
    """
    Utility class used to refresh a proxy using a MyProxy server.
    """
    def renew_proxy(self, proxy_file_path, myproxy_creds_name, myproxy_server, myproxy_server_port, renew_time):
        """This method will call the MyProxy commands to renew the credential for a given job.
        
        Returns True on sucess, False otherwise."""
        if proxy_file_path == None:
            log.error("Attempt to renew proxy for job with no proxy.  Aborting proxy renew operation.")
            return False

        if myproxy_creds_name == None:
            log.error("Missing MyProxy credential name for job.")
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
                return False


        # Note: Here we put the refreshed proxy in a seperate file.  We do this to protect the ownership and permisions on the
        # original user's proxy in case the condor_submit was run on the same machine as the cloud scheduler.  If we do not do
        # this, then the cloud scheduler will overwrite the user's proxy with the renewed proxy, but will be owned by root; so
        # user will not be able to create proxies anymore because he/she won't have permission to overwrite the proxy file
        # created by the cloud scheduler.
        # Once the renewed proxy is in the temporary file, we then copy it over the original job's proxy, without changing its
        # permissions.
        #
        # This is a bit of a hack; there must me a better way to handle this problem.
        try:
            (new_proxy_file, new_proxy_file_path) = tempfile.mkstemp(suffix='.csRenewedProxy')
            os.close(new_proxy_file)
            myproxy_logon_cmd = '%s -s %s -p %s -k "%s" -a %s -o %s -t %s -d -v' % (myproxy_command, myproxy_server, myproxy_server_port, myproxy_creds_name, proxy_file_path, new_proxy_file_path, renew_time)
            cmd_args = shlex.split(myproxy_logon_cmd)
            log.verbose('Invoking myproxy-logon command to refresh proxy %s ...' % (proxy_file_path))
            myproxy_logon_process = subprocess.Popen(cmd_args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (stdout, stderr) = myproxy_logon_process.communicate()
            if myproxy_logon_process.returncode != 0:
                log.error("Error renewing proxy from MyProxy server: %s %s" % (stdout, stderr))
                os.remove(new_proxy_file_path)
                return False
            else:
                log.verbose('myproxy-logon command returned successfully')

            shutil.copyfile(new_proxy_file_path, proxy_file_path)
            os.remove(new_proxy_file_path)
        except:
            log.exception('Unexpected error during proxy renewal using MyProxy.')
            return False

        return True
    
    def renew_proxy_meta(self, joborvm):
        """Single function to accept a job or vm proxy to refresh."""
        if joborvm is isinstance(Job):
            if MyProxyProxyRefresher().renew_proxy(joborvm.get_x509userproxy(), joborvm.get_myproxy_creds_name(), joborvm.get_myproxy_server(), joborvm.get_myproxy_server_port()):
                # Yay, proxy renewal worked! :-)
                log.verbose("Proxy for job %s renewed." % (joborvm.id))
                # Don't forget to reset the proxy expiry time cache.
                joborvm.reset_x509userproxy_expiry_time()
            else:
                log.error("Error renewing proxy for job %s" % (joborvm.id))
        elif joborvm is isinstance(VM):
            if self.renew_proxy(joborvm.get_proxy_file(), joborvm.get_myproxy_creds_name(), joborvm.get_myproxy_server(), joborvm.get_myproxy_server_port()):
                # Yay, proxy renewal worked! :-)
                log.verbose("Proxy for VM %s renewed." % (joborvm.id))
                # Don't forget to reset the proxy expiry time cache.
                joborvm.reset_x509userproxy_expiry_time()
            else:
                log.error("Error renewing proxy for VM %s" % (joborvm.id))

    def renew_job_proxy_user(self, job_pool, user):
        """Refresh all job proxies for a user."""
        for job in job_pool.job_container.get_jobs_for_user(user):
            self.renew_proxy_meta(job)

    def renew_vm_proxy_user(self, resource_pool, user):
        """Refresh all VM proxies for a user."""
        for vm in resource_pool.get_user_vms():
            self.renew_proxy_meta(vm)

