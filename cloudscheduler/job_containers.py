from abc import ABCMeta, abstractmethod
from collections import defaultdict
import time
import threading
import logging
import cloudscheduler.config as config

# Use this global variable for logging.
log = None

#
# This is an abstract base class; do not intantiate directly.
#
# API documentation should go in here, as opposed to writing specific
# documentation for each concrete subclasses.
#
class JobContainer():
    __metclass__ = ABCMeta

    # Use this lock if you require to threadsafe an operation.
    lock = None
    ## Condor Job Status mapping
    job_status_list = ['NEW', 'IDLE', 'RUNNING', 'REMOVED', 'COMPLETE', 'HELD', 'ERROR']
    def __init__(self):
        self.lock = threading.RLock()
        global log
        log = logging.getLogger("cloudscheduler")
        pass


    # Tests if the container has a specific job, by id.
    # Returns True if the container has the given job, returns False otherwise.
    @abstractmethod
    def has_job(self, jobid):
        pass
   
    # Add a job to the container.
    # If the job already exist, it will be replaced.
    @abstractmethod
    def add_job(self, job):
        pass

    # Add a set of jobs (in a list) to the container.
    # If a job already exist, it will be replaced.
    @abstractmethod
    def add_jobs(self, jobs):
        pass

    # Remove all jobs from the container.
    # After calling this method, the container is completely empty.
    @abstractmethod
    def clear(self):
        pass

    # Remove a single job form the container.
    # If the job does not exist in the container, then nothing is done.
    @abstractmethod
    def remove_job(self, job):
        pass

    # Remove a set of jobs (in a list) from the container.
    # If a job does not exist in the container, then it is ignored.
    @abstractmethod
    def remove_jobs(self, jobs):
        pass

    # Remove a job (by job id) from the container.
    # If the job does not exist in the container, then nothing is done.
    @abstractmethod
    def remove_job_by_id(self, jobid):
        pass

    # Remove a set of jobs (by job ids, in a list) from the container.
    # If a job does not exist in the container, then it is ignored.
    @abstractmethod
    def remove_jobs_by_id(self, jobids):
        pass

    # Remove all jobs in the container that does not appear in a given set
    # of jobs (in a list).
    @abstractmethod
    def remove_all_not_in(self, jobs_to_keep):
        pass

    # Updates the status and remote host of a job (job.job_status attribute) 
    # in the container.
    # Returns True if the job was found in the container, False otherwise.
    @abstractmethod
    def update_job_status(self, jobid, status, remote):
        pass

    # Mark a job as being scheduled.
    # This will update the job's status attribute to "Scheduled".
    # Returns True if the job exist in the container and was previously unscheduled, returns False otherwise.
    @abstractmethod
    def schedule_job(self, job):
        pass

    # Mark a job as being unscheduled.
    # This will update the job's status attribute to "Unscheduled".
    # Returns True if the job exist in the container and was previously scheduled, returns False otherwise.
    @abstractmethod
    def unschedule_job(self, job):
        pass




    # Returns a list of users that the container holds jobs for, or [] if the container is empty.
    @abstractmethod
    def get_users(self):
        pass

    # Returns a list of all jobs in the container, in no particular order, or [] if the container is empty.
    @abstractmethod
    def get_all_jobs(self):
        pass

    # Get a job by job id.
    # Return the job with the given job id, or None if the job does not exist in the container.
    @abstractmethod
    def get_job_by_id(self, jobid):
        pass

    # Get a list of all jobs for a user.
    # Returns list of jobs for the user, or an empty list if the container has no jobs for the given user.
    # If prioritized is True, then the returned list of jobs will be sorted by job.priority, high to low.
    @abstractmethod
    def get_jobs_for_user(self, user, prioritized=False):
        pass

    # Get a list of all scheduled jobs in the container, or [] if there are no scheduled jobs.
    @abstractmethod
    def get_scheduled_jobs(self):
        pass
    
    # Get a list of all scheduled jobs in the container sorted by their job.id, or [] if no unscheduled jobs.
    @abstractmethod
    def get_scheduled_jobs_sorted_by_id(self):
        pass

    # Get a list of all scheduled jobs per user.
    # Returns dictionary where the items are:
    # (user, [list of scheduled jobs])
    # If a user does not have any scheduled jobs, then there will be no entry for that user.
    # If prioritized is True, then the returned lists of jobs will be sorted by job.priority, high to low.
    @abstractmethod
    def get_scheduled_jobs_by_users(self, prioritized=False):
        pass

    # Get a list of all scheduled jobs per type.
    # Returns dictionary where the items are:
    # {type, [list of scheduled jobs]}
    # If prioritized i True, then the returned lists of jobs will be sorted by job.priority, high to low.
    @abstractmethod
    def get_scheduled_jobs_by_type(self, prioritized=False):
        pass

    # Get a list of all unscheduled jobs in the container, or [] if there are no unscheduled jobs.
    @abstractmethod
    def get_unscheduled_jobs(self):
        pass
    
    # Get a list of all unscheduled jobs in the container sorted by their job.id, or [] if no unscheduled jobs.
    @abstractmethod
    def get_unscheduled_jobs_sorted_by_id(self):
        pass

    # Get a list of all unscheduled jobs per user.
    # Returns dictionary where the items are:
    # (user, [list of unscheduled jobs])
    # If a user does not have any unscheduled jobs, then there will be no entry for that user.
    # If prioritized is True, then the returned lists of jobs will be sorted by job.priority, high to low.
    @abstractmethod
    def get_unscheduled_jobs_by_users(self, prioritized=False):
        pass

    # Get a list of all unscheduled jobs per type.
    # Returns dictionary where the items are:
    # {type, [list of unscheduled jobs]}
    # If prioritized i True, then the returned lists of jobs will be sorted by job.priority, high to low.
    @abstractmethod
    def get_unscheduled_jobs_by_type(self, prioritized=False):
        pass

    # Get a list of all high priority jobs in the container, or [] if there are no high priority jobs.
    # A job is said to have high priority if job.high_priority != 0
    @abstractmethod
    def get_high_priority_jobs(self):
        pass

    # Get a list of all high priority jobs per user.
    # Returns dictionary where the items are:
    # (user, [list of high priority jobs])
    # If a user does not have any high priority jobs, then there will be no entry for that user.
    # If prioritized is True, then the returned lists of jobs will be sorted by job.priority, high to low.
    @abstractmethod
    def get_high_priority_jobs_by_users(self, prioritized=False):
        pass

    # Finds up to N jobs matching the requirements of the given job.
    # If N == 0, then all matching jobs are returned.
    @abstractmethod
    def find_unscheduled_jobs_with_matching_reqs(self, user, job, N=0):
        pass

    # Returns True if the container has no jobs, returns False otherwise.
    @abstractmethod
    def is_empty(self):
        pass

    # Returns a string containing human-readable information about this container.
    @abstractmethod
    def __str__(self):
        pass










#
# This class implements a job container based on hash tables.
#
class HashTableJobContainer(JobContainer):
    # class attributes
    all_jobs = None
    new_jobs = None
    sched_jobs = None
    jobs_by_user = None

    # constructor
    def __init__(self):
        JobContainer.__init__(self)
        self.all_jobs = {}
        self.new_jobs = {}
        self.sched_jobs = {}
        self.jobs_by_user = defaultdict(dict)
        log.verbose('HashTableJobContainer instance created.')

    # methods
    def __str__(self):
        return 'HashTableJobContainer [# of jobs: %d (unshed: %d sched: %d)]' % (len(self.all_jobs), len(self.new_jobs), len(self.sched_jobs))

    def has_job(self, jobid):
        return self.get_job_by_id(jobid) != None

    def add_job(self, job):
        with self.lock:
            self.all_jobs[job.id] = job
            self.jobs_by_user[job.user][job.id] = job

            # Update scheduled/unscheduled maps too:
            if(job.status == "Unscheduled"):
                self.new_jobs[job.id] = job
            else:
                self.sched_jobs[job.id] = job

            #log.debug('job %s added to job container' % (job.id))

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
            log.verbose('job container cleared')

    def remove_job(self, job):
        with self.lock:
            if job.id in self.all_jobs:
                del self.all_jobs[job.id]
            if job.user in self.jobs_by_user and (job.id in self.jobs_by_user[job.user]):
                del self.jobs_by_user[job.user][job.id]
                if len(self.jobs_by_user[job.user]) == 0:
                    del self.jobs_by_user[job.user]
            if job.id in self.new_jobs:
                del self.new_jobs[job.id]
            if job.id in self.sched_jobs:
                del self.sched_jobs[job.id]
            #log.debug('job %s removed from container' % job.id)

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
            removed_jobs = []
            for job in jobs_to_keep:
                jobs_to_keep_dict[job.id] = job

            for job in self.all_jobs.values():
                # If the job is not in the jobs to keep, simply remove it.
                if job.id not in jobs_to_keep_dict:
                    self.remove_job(job)
                    removed_jobs.append(job)
        return removed_jobs

    def get_users(self):
        return self.jobs_by_user.keys()

    def get_all_jobs(self):
        return self.all_jobs.values()

    def get_job_by_id(self, jobid):
        try:
            return self.all_jobs[jobid]
        except KeyError:
            return None

    def get_held_jobs(self):
        HELD = 5
        held_jobs = []
        for job in self.all_jobs.values():
            if job.job_status == HELD:
                held_jobs.append(job)
        return held_jobs
    
    def get_idle_jobs(self):
        IDLE = 1
        idle_jobs = []
        for job in self.all_jobs.values():
            if job.job_status == IDLE:
                idle_jobs.append(job)
        return idle_jobs

    def get_running_jobs(self):
        RUNNING = 2
        run_jobs = []
        for job in self.all_jobs.values():
            if job.job_status == RUNNING:
                run_jobs.append(job)
        return run_jobs

    def get_complete_jobs(self):
        COMPLETE = 4
        comp_jobs = []
        for job in self.all_jobs.values():
            if job.job_status == COMPLETE:
                comp_jobs.append(job)
        return comp_jobs

    def get_jobs_for_user(self, user, prioritized=False):
        with self.lock:
            if user not in self.jobs_by_user:
                return []

            if prioritized:
                jobs = self.jobs_by_user[user].values()
                # Sort jobs in order of priority. The list runs front to back, high to low priority.
                jobs.sort(key=lambda job: job.get_priority(), reverse=True)
                return jobs
            else:
                return self.jobs_by_user[user].values()

    def get_scheduled_jobs(self):
        return self.sched_jobs.values()
    
    def get_scheduled_jobs_sorted_by_id(self):
        return_value = []
        for jobid in sorted(self.sched_jobs.iteritems()):
            return_value.append(jobid[1])
        return return_value

    def get_scheduled_jobs_by_users(self, prioritized=False):
        with self.lock:
            return_value = defaultdict(list)
            for job in self.sched_jobs.values():
                return_value[job.user].append(job)
            # Now lets sort if needed.
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)
            return return_value

    def get_scheduled_jobs_by_type(self, prioritized=False):
        with self.lock:
            return_value = defaultdict(list)
            for job in self.sched_jobs.values():
                return_value[job.req_vmtype].append(job)
            # Now sort if needed.
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)
            return return_value

    def get_scheduled_jobs_by_usertype(self, prioritized=False):
        with self.lock:
            return_value = defaultdict(list)
            for job in self.sched_jobs.values():
                return_value[job.uservmtype].append(job)
            # Now sort if needed.
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)
            return return_value

    def get_unscheduled_jobs(self):
        return self.new_jobs.values()
    
    def get_unscheduled_jobs_sorted_by_id(self):
        return_value = []
        for jobid in sorted(self.new_jobs.iteritems()):
            return_value.append(jobid[1])
        return return_value
        
    def get_unscheduled_jobs_by_users(self, prioritized=False):
        with self.lock:
            return_value = defaultdict(list)
            for job in self.new_jobs.values():
                return_value[job.user].append(job)
            # Now lets sort if needed.
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)
            return return_value

    def get_unscheduled_jobs_by_type(self, prioritized=False):
        with self.lock:
            return_value = defaultdict(list)
            for job in self.new_jobs.values():
                return_value[job.req_vmtype].append(job)
            # Now sort if needed.
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)
            return return_value

    def get_unscheduled_jobs_by_usertype(self, prioritized=False):
        with self.lock:
            return_value = defaultdict(list)
            for job in self.new_jobs.values():
                return_value[job.uservmtype].append(job)
            # Now sort if needed.
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)
            return return_value

    def get_high_priority_jobs(self):
        jobs = []
        for job in self.all_jobs.values():
            if job.high_priority:
                jobs.append(job)
        return jobs

    def get_high_priority_jobs_by_users(self, prioritized=False):
        with self.lock:
            return_value = defaultdict(list)
            for job in self.get_high_priority_jobs():
                return_value[job.user].append(job)
            # Now lets sort if needed.
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)

            #log.verbose("(OUT) get_high_priority_jobs_by_users")

            return return_value

    def get_unscheduled_high_priority_jobs(self):
        jobs = []
        for job in self.all_jobs.values():
            if job.high_priority and job.status == 'Unscheduled':
                jobs.append(job)
        return jobs

    def get_unscheduled_high_priority_jobs_by_users(self, prioritized=False):
        with self.lock:
            return_value = defaultdict(list)
            for job in self.get_unscheduled_high_priority_jobs():
                return_value[job.user].append(job)
            # Now lets sort if needed.
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)

            #log.verbose("(OUT) get_unscheduled_high_priority_jobs_by_users")

            return return_value

    def is_empty(self):
        return len(self.all_jobs) == 0

    def update_job_status(self, jobid, status, remote, servertime, starttime):
        with self.lock:
            job = self.get_job_by_id(jobid)
        if job != None:
            if job.job_status != status and job.override_status != None:
                job.override_status = None
            if job.job_status != status:
                log.debug("Job %s status change: %s -> %s" % (job.id, self.job_status_list[job.job_status], self.job_status_list[status]))
            job.job_status = status
            job.remote_host = remote
            job.servertime = int(servertime)
            job.jobstarttime = int(starttime)
            if job.banned and job.ban_time:
                if (time.time() - job.ban_time) > config.job_ban_timeout:
                    job.banned = False
                    job.ban_time = None
                    job.override_status = None
            if len(job.blocked_clouds) > 0:
                if (time.time() - job.block_time) > config.job_ban_timeout:
                    job.blocked_clouds = []
                    job.block_time = None
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
                #log.verbose('Job %s marked as scheduled in the job container' % (jobid))
                return True
            else:
                return False
                

    def unschedule_job(self, jobid):
        with self.lock:
            if jobid in self.sched_jobs:
                job = self.sched_jobs[jobid]
                job.set_status("Unscheduled")
                self.new_jobs[jobid] = job
                del self.sched_jobs[jobid]
                #log.verbose('Job %s marked as unscheduled in the job container' % (jobid))
                return True
            else:
                return False

    def find_unscheduled_jobs_with_matching_reqs(self, user, job, N=0):
        with self.lock:
            counter = 0
            unscheduled_jobs_for_user = []
            try:
                unscheduled_jobs_for_user = self.get_unscheduled_jobs_by_users()[user]
            except KeyError:
                # User has no unscheduled jobs.
                # Simply return an empty list right away.
                return []

            matching_jobs = []
            for j in unscheduled_jobs_for_user:
                if j.has_same_reqs(job):
                    matching_jobs.append(j)
                    counter += 1
                    if counter > 0 and counter == N:
                        break

            return matching_jobs

    def get_unscheduled_user_jobs_by_type(self, user, prioritized=False):
        with self.lock:
            unsched = self.get_unscheduled_jobs_by_users()
            return_value = defaultdict(list)
            if user in unsched.keys():
                for job in unsched[user]:
                    return_value[job.req_vmtype].append(job)
            # Sort if needed
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)
        return return_value

    def get_unscheduled_user_jobs_by_usertype(self, user, prioritized=False):
        with self.lock:
            unsched = self.get_unscheduled_jobs_by_users()
            return_value = defaultdict(list)
            if user in unsched.keys():
                for job in unsched[user]:
                    return_value[job.uservmtype].append(job)
            # Sort if needed
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)
        return return_value
    
    def get_scheduled_user_jobs_by_type(self, user, prioritized=False):
        with self.lock:
            sched = self.get_scheduled_jobs_by_users()
            return_value = defaultdict(list)
            if user in sched.keys():
                for job in sched[user]:
                    return_value[job.req_vmtype].append(job)
            # Sort if needed
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)
        return return_value
    
    def get_scheduled_user_jobs_by_usertype(self, user, prioritized=False):
        with self.lock:
            sched = self.get_scheduled_jobs_by_users()
            return_value = defaultdict(list)
            if user in sched.keys():
                for job in sched[user]:
                    return_value[job.req_vmtype].append(job)
            # Sort if needed
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)
        return return_value