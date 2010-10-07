from abc import ABCMeta, abstractmethod
import threading
import logging
import cloudscheduler.config as config


log = None

class JobContainer():
    __metclass__ = ABCMeta

    # Some constants
    ADD_NEW = 1
    ADD_SCHED = 2
    ADD_HIGH = 3

    def __init__(self):
        global log
        log = logging.getLogger("cloudscheduler")
        pass

    #
    # Job addition
    #
    @abstractmethod
    def add_job(self, job, add_type):
        pass

    @abstractmethod
    def add_jobs(self, jobs, add_type):
        pass

    #
    # Job deletion
    #

    @abstractmethod
    def clear(self):
        pass

    @abstractmethod
    def remove_job(self, job):
        pass

    @abstractmethod
    def remove_jobs(self, jobs):
        pass

    @abstractmethod
    def remove_job_by_id(self, jobid):
        pass

    @abstractmethod
    def remove_jobs_by_id(self, jobids):
        pass

    @abstractmethod
    def remove_all_not_in(self, jobs_to_keep):
        pass

    @abstractmethod
    def update_job_status(self, jobid, status):
        pass

    @abstractmethod
    def schedule_job(self, job):
        pass

    @abstractmethod
    def unschedule_job(self, job):
        pass

    #
    # Getters
    #
    @abstractmethod
    def get_all_jobs(self):
        pass

    @abstractmethod
    def get_job_by_id(self, jobid):
        pass

    @abstractmethod
    def get_jobs_for_user(self, user):
        pass

    @abstractmethod
    def get_scheduled_jobs(self):
        pass

    @abstractmethod
    def get_unscheduled_jobs(self):
        pass

    @abstractmethod
    def get_high_priority_jobs(self):
        pass

    @abstractmethod
    def is_empty(self):
        pass

    abstractmethod
    def to_string(self):
        pass

    
class HashTableJobContainer(JobContainer):
    # class attributes
    all_jobs = None
    new_jobs = None
    sched_jobs = None
    high_jobs = None
    jobs_by_user = None

    # constructor
    def __init__(self):
        JobContainer.__init__(self)
        self.lock = threading.RLock()
        self.all_jobs = {}
        self.new_jobs = {}
        self.sched_jobs = {}
        self.high_jobs = {}
        self.jobs_by_user = {}
        log.debug('HashTableJobContainer instance created.')

    # methods
    def to_string(self):
        return 'HashTableJobContainer [# of jobs: %d, locked: %s]' % (len(all_jobs), self.lock.locked())

    def add_job(self, job, add_type):
        with self.lock:
            all_jobs[job.id] = job
            if job.user not in jobs_by_user:
                jobs_by_user[job.user] = {}
            jobs_by_user[job.user][job.id] = job

            if(add_type == JobContainer.ADD_NEW):
                self.new_jobs[job.id] = job
            elif (add_type == JobContainer.ADD_SCHED):
                self.sched_jobs[job.id] = job
            elif (add_type == JobContainer.ADD_HIGH):
                if config.high_priority_job_support:
                    self.high_jobs[job.id] = job
                else:
                    self.new_jobs[job.id] = job

            log.debug('job %s added to job container' % (job.id))

    def add_jobs(self, jobs, add_type):
        with self.lock:
            for job in jobs:
                self.add_job(job, add_type)

    def clear(self):
        with self.lock:
            self.all_jobs.clear()
            self.jobs_by_user.clear()
            self.new_jobs.clear()
            self.sched_jobs.clear()
            self.high_jobs.clear()
            log.debug('job container cleared')

    def remove_job(self, job):
        with self.lock:
            if job.id in self.all_jobs:
                del self.all_jobs[job.id]
            if job.user in self.jobs_by_user and (job.id in self.jobs_by_user[job.user]):
                del self.jobs_by_user[job.user][job.id]
                if len(self.jobs_by_user[job.user] == 0):
                    del self.jobs_by_user[job.user]
            if job.id in self.new_jobs:
                del self.new_jobs[job.id]
            if job.id in self.sched_jobs:
                del self.sched_jobs[job.id]
            if job.id in self.high_jobs:
                del self.high_jobs[job.id]
            log.debug('job %s removed from container' % job.id)

    def remove_jobs(self, jobs):
        with self.lock:
            for job in jobs:
                self.remove_job(job)

    def remove_job_by_id(self, jobid):
        with self.lock:
            self.remove_job(self.get_job_by_id(jobid))

    def remove_jobs_by_id(self, jobids):
        with self.lock:
            for jobid in jobids:
                self.remove_job_by_id(jobid)

    def remove_all_not_in(self, jobs_to_keep):
        with self.lock:
            # create a dictionary of the given jobs to keep first (for effeciency)
            jobs_to_keep_dict = {}
            for job in jobs_to_keep:
                jobs_to_keep_dict[job.id] = job

            for job in self.all_jobs.values():
                # If the job is not in the jobs to keep, simply remove it.
                if job.id not in jobs_to_keep_dict:
                    self.remove_job(job)
            

    def get_all_jobs(self):
        return self.all_jobs.values()

    def get_job_by_id(self, jobid):
        with self.lock:
            if jobid in self.all_jobs:
                return self.all_jobs[jobid]
            else:
                return None

    def get_jobs_for_user(self, user):
        with self.lock:
            if user in self.jobs_by_user:
                return self.jobs_by_user[user]
            else:
                return None

    def get_scheduled_jobs(self):
        return self.sched_jobs.values()

    def get_unscheduled_jobs(self):
        return self.new_jobs.values()

    def get_high_priority_jobs(self):
        return self.high_jobs.values()

    def is_empty(self):
        return len(self.all_jobs) == 0

    def update_job_status(self, jobid, status):
        with self.lock:
            job = self.get_job_by_id(jobid)
        if job != None:
            job.job_status = status
            return True
        else:
            return False

    def schedule_job(self, jobid):
        with self.lock:
            if jobid in self.new_jobs:
                job = self.new_jobs[jobid]
                job.set_status("Scheduled")
                self.sched_jobs[jobid] = job
                del self.new_jobs[jobid]
                log.debug('Job %s marked as scheduled in the job container' % (jobid))
                

    def unschedule_job(self, jobid):
        with self.lock:
            if jobid in self.sched_jobs:
                job = self.sched_jobs[jobid]
                job.set_status("Unscheduled")
                if job.high_priority == 0:
                    self.new_jobs[jobid] = job
                else:
                    if config.high_priority_job_support:
                        self.high_jobs[jobid] = job
                    else:
                        self.new_jobs[jobid] = job
                del self.sched_jobs[jobid]
                log.debug('Job %s marked as unscheduled in the job container' % (jobid))
