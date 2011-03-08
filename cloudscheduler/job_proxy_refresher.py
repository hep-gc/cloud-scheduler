import os
import threading
import time
import traceback
import datetime
import subprocess
import tempfile
import shutil

import cloudscheduler.config as config
import cloudscheduler.utilities as utilities
import cloudscheduler.job_management as job_management


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
                jobs = self.job_pool.job_container.get_all_jobs()
                log.debug("Refreshing job user proxies. [%d proxies to process]" % (len(jobs)))
                for job in jobs:
                    log.debug("Proxy for job %s expires on: %s" % (job.id, job.get_x509userproxy_expiry_time()))
                    if job.needs_proxy_renewal():
                        if job.get_myproxy_creds_name() != None:
                            log.debug("Renewing proxy %s" % (job.get_x509userproxy()))
                            if self.renew_proxy(job):
                                # Yay, proxy renewal worked! :-)
                                log.debug("Proxy for job %s renewed." % (job.id))
                            else:
                                log.error("Error renewing proxy for job %s" % (job.id))
                        else:
                            # If we get here, this means that the proxy should be renewed, but there
                            # is not MyProxy info for that job's proxy.  Not an error; just that the
                            # owner of the job didn't give any MyProxy information to renew the
                            # credentials.
                            log.debug("Not renewing proxy job %s because missing MyProxy info." % (job.id))
                    else:
                        log.debug("No need to renew proxy for job %s" % (job.id))

                log.debug("JobProxyRefresher waiting %ds..." % self.polling_interval)
                sleep_tics = self.polling_interval
                while (not self.quit) and sleep_tics > 0:
                    time.sleep(1)
                    sleep_tics -= 1

            log.debug("Exiting JobProxyRefresher thread")
        except:
            log.error("Error in JobProxyRefresher thread.")
            log.error(traceback.format_exc())


    # This method will call the MyProxy commands to renew the credential for a given job.
    # 
    # Returns True on sucess, False otherwise.
    def renew_proxy(self, job):
        job_proxy_file_path = job.get_x509userproxy()
        if job_proxy_file_path == None:
            log.error("Attemp to renew proxy for job with no proxy.  Aborting proxy renew operation.")
            return False

        myproxy_creds_name = job.get_myproxy_creds_name()
        if myproxy_creds_name == None:
            log.error("Missing MyProxy credential name for job %s" % (job.id))
            return False

        myproxy_server = job.get_myproxy_server()
        if myproxy_server == None:
            log.warning("MyProxy credential name given but missing MyProxy server host. Defaulting to localhost")
            myproxy_server = "localhost"

        myproxy_server_port = job.get_myproxy_server_port()
        if myproxy_server_port == None:
            log.debug("No MyProxy server port given; using default port (7512)")
            myproxy_server_port = "7512"

        # Check to see if $GLOBUS_LOCATION is defined.
        if os.environ["GLOBUS_LOCATION"] == None:
            log.error("GLOBUS_LOCATION not set.  Please set GLOBUS_LOCATION.")
            return False

        # Check to see of myproxy-logon is present in globus installation
        if not os.path.exists(os.environ["GLOBUS_LOCATION"] + "/bin/myproxy-logon"):
            log.error("MyProxy credentials specified but $GLOBUS_LOCATION/bin/myproxy-logon not found.  Make sure you have a valid MyProxy client installation on your system.")
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
        (new_proxy_file, new_proxy_file_path) = tempfile.mkstemp(suffix='.csRenewedProxy')
        os.close(new_proxy_file)
        myproxy_logon_cmd = '. $GLOBUS_LOCATION/etc/globus-user-env.sh && $GLOBUS_LOCATION/bin/myproxy-logon -s %s -p %s -k %s -a %s -o %s -d' % (myproxy_server, myproxy_server_port, myproxy_creds_name, job_proxy_file_path, new_proxy_file_path)
        log.debug('myproxy-logon command: [%s]' % (myproxy_logon_cmd))
        log.debug('Invoking myproxy-logon command to refresh proxy %s ...' % (job_proxy_file_path))
        myproxy_logon_process = subprocess.Popen(myproxy_logon_cmd, shell=True)
        myproxy_logon_process.wait()
        log.debug('myproxy-logon command returned %d' % (myproxy_logon_process.returncode))
        if myproxy_logon_process.returncode != 0:
            log.error("Error renewing proxy from MyProxy server.")
            log.debug('(Cleanup) Deleting %s ...' % (new_proxy_file_path))
            os.remove(new_proxy_file_path)
            return False
        log.debug('Copying %s to %s ...' % (new_proxy_file_path, job_proxy_file_path))
        shutil.copyfile(new_proxy_file_path, job_proxy_file_path)
        log.debug('(Cleanup) Deleting %s ...' % (new_proxy_file_path))
        os.remove(new_proxy_file_path)

        # Don't forget to reset the proxy expiry time cache.
        job.reset_x509userproxy_expiry_time()
        
        return True

