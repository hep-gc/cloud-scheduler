import threading

log = None

class JobContainer:
    def __init__(self):
        global log
        log = logging.getLogger("cloudscheduler")
        pass

    #
    # Job addition
    #
    @abstractmethod
    def add_job(self, job):
        pass

    @abstractmethod
    def add_jobs(self, jobs):
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

    #
    # Getters
    #
    @abstractmethod
    def get_job_by_id(self, jobid):
        pass

    @abstractmethod
    def get_jobs_for_user(self, user):
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
    jobs_by_user = None

    # constructor
    def __init__(self):
        JobContainer.__init__(self)
        self.lock = threading.RLock()
        self.all_jobs = {}
        self.jobs_by_user = {}
        log.debug('HashTableJobContainer instance created.')

    # methods
    def to_string(self):
        return 'HashTableJobContainer [# of jobs: %d, locked: %s]' % (len(all_jobs), self.lock.locked())

    def add_job(self, job):
        with self.lock:
            all_jobs[job.id] = job
            if job.user not in jobs_by_user:
                jobs_by_user[job.user] = {}
            jobs_by_user[job.user][job.id]	= job
            log.debug('job %s added to job container' % (job.id))

    def add_jobs(self, jobs):
        with self.lock:
            for job in jobs:
                self.add_job(job)

    def clear(self):
        with self.lock:
            all_jobs.clear()
            jobs_by_user.clear()
            log.debug('job container cleared')

    def remove_job(self, job):
        with self.lock:
            if job.id in all_jobs:
                del all_jobs[job.id]
            if job.user in jobs_by_user and (job.id in jobs_by_user[job.user]):
                del jobs_by_user[job.user][job.id]
                if len(jobs_by_user[job.user] == 0):
                    del jobs_by_user[job.user]
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

    def get_job_by_id(self, jobid):
        with self.lock:
            if jobid in all_jobs:
                return all_jobs[jobid]
            else:
                return None

    def get_jobs_for_user(self, user):
        with self.lock:
            if user in jobs_by_user:
                return jobs_by_user[user]
            else:
                return None

    def is_empty(self):
        return len(all_jobs) == 0
