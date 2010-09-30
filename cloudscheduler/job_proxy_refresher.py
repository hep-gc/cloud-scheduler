import threading
import time
import traceback

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
        self.polling_interval = 60 # proxy expiry time poll interval, in seconds

    def stop(self):
        log.debug("Waiting for job proxy refresher loop to end")
        self.quit = True

    def run(self):
        try:
            log.info("Starting JobProxyRefresher thread...")

            while not self.quit:
                log.debug("Refreshing job user proxies...")
                jobs = self.job_pool.get_all_jobs()
                log.debug(jobs)
                log.debug("%d job(s) to process" % len(jobs))
                for job in jobs:
                    log.debug("Processing job %s.  Proxy: %s Expires on: %s" % (job.id, job.get_x509userproxy(), job.get_x509userproxy_expiry_time()))


                log.debug("JobProxyRefresher waiting %ds..." % self.polling_interval)
                sleep_tics = self.polling_interval
                while (not self.quit) and sleep_tics > 0:
                    time.sleep(1)
                    sleep_tics -= 1

            log.debug("Exiting JobProxyRefresher thread")
        except:
            log.error("Error in JobProxyRefresher thread.")
            log.error(traceback.format_exc())
