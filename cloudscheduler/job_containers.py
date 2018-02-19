"""
Job Container classes, with the abstract and implementation.
Done to help optimize access to the condor job data structure
and improve performance.
"""

from abc import ABCMeta, abstractmethod
from collections import defaultdict
import time
import threading
import logging
import cloudscheduler.config as config

# Use this global variable for logging.
log = None
config_val = config.config_options

class JobContainer(object):

    """
    This is an abstract base class; do not instantiate directly.

    API documentation should go in here, as opposed to writing specific
    documentation for each concrete subclasses.

    """
    __metclass__ = ABCMeta

    # Use this lock if you require to thread safe an operation.
    lock = None
    ## Condor Job Status mapping
    job_status_list = ['NEW', 'IDLE', 'RUNNING', 'REMOVED', 'COMPLETE', 'HELD', 'ERROR']
    def __init__(self):
        """
        Constructor for job container base class.
        """
        self.lock = threading.RLock()
        global log
        log = logging.getLogger("cloudscheduler")

    @abstractmethod
    def has_job(self, jobid):
        """
        Tests if the container has a specific job, by id.
        Returns True if the container has the given job, returns False otherwise.
        :param jobid:
        """
        pass

    @abstractmethod
    def add_job(self, job):
        """
        Add a job to the container.
        If the job already exist, it will be replaced.
        :param job:
        """
        pass


    @abstractmethod
    def add_jobs(self, jobs, add_type):
        """
        Add a set of jobs (in a list) to the container.
        If a job already exist, it will be replaced.
        :param jobs:
        """
        pass

    @abstractmethod
    def clear(self):
        """
        Remove all jobs from the container.
        After calling this method, the container is completely empty.
        """
        pass

    @abstractmethod
    def remove_job(self, job):
        """
        Remove a single job form the container.
        If the job does not exist in the container, then nothing is done.
        :param job:
        """
        pass

    @abstractmethod
    def remove_jobs(self, jobs):
        """
        Remove a set of jobs (in a list) from the container.
        If a job does not exist in the container, then it is ignored.
        :param jobs:
        """
        pass

    @abstractmethod
    def remove_job_by_id(self, jobid):
        """
        Remove a job (by job id) from the container.
        If the job does not exist in the container, then nothing is done.
        :param jobid:
        """
        pass

    @abstractmethod
    def remove_jobs_by_id(self, jobids):
        """
        Remove a set of jobs (by job ids, in a list) from the container.
        If a job does not exist in the container, then it is ignored.
        :param jobids:
        """
        pass

    @abstractmethod
    def remove_all_not_in(self, jobs_to_keep):
        """
        Remove all jobs in the container that does not appear in a given set
        of jobs (in a list).
        :param jobs_to_keep:
        """
        pass

    @abstractmethod
    def update_job_status(self, jobid, status, remote, servertime, starttime):
        """
        Updates the status and remote host of a job (job.job_status attribute)
        in the container.
        Returns True if the job was found in the container, False otherwise.
        :param jobid:
        :param status:
        :param remote:
        """
        pass

    @abstractmethod
    def schedule_job(self, job):
        """
        Mark a job as being scheduled.
        This will update the job's status attribute to "Scheduled".
        Returns True if the job exist in the container and was previously unscheduled
        returns False otherwise.
        :param job:
        """
        pass

    @abstractmethod
    def unschedule_job(self, job):
        """
        Mark a job as being unscheduled.
        This will update the job's status attribute to "Unscheduled".
        Returns True if the job exist in the container and was previously scheduled
        returns False otherwise.
        :param job:
        """
        pass

    @abstractmethod
    def get_users(self):
        """
        Returns a list of users that the container holds jobs for, or [] if the container is empty.
        """
        pass

    @abstractmethod
    def get_all_jobs(self):
        """
        Returns a list of all jobs in the container, in no particular order
        or [] if the container is empty.
        """
        pass

    @abstractmethod
    def get_job_by_id(self, jobid):
        """
        Get a job by job id.
        Return the job with the given job id, or None if the job does not exist in the container.
        :param jobid:
        """
        pass

    @abstractmethod
    def get_jobs_for_user(self, user, prioritized=False):
        """
        Get a list of all jobs for a user.
        Returns list of jobs for the user
        or an empty list if the container has no jobs for the given user.
        If prioritized is True, then the returned list of jobs
        will be sorted by job.priority, high to low.
        :param user:
        :param prioritized:
        """
        pass

    @abstractmethod
    def get_scheduled_jobs(self):
        """
        Get a list of all scheduled jobs in the container, or [] if there are no scheduled jobs.
        """
        pass

    @abstractmethod
    def get_scheduled_jobs_sorted_by_id(self):
        """
        Get a list of all scheduled jobs in the container sorted by their job.id
        or [] if no unscheduled jobs.
        """
        pass

    @abstractmethod
    def get_scheduled_jobs_by_users(self, prioritized=False):
        """
        Get a list of all scheduled jobs per user.
        Returns dictionary where the items are:
        (user, [list of scheduled jobs])
        If a user does not have any scheduled jobs, then there will be no entry for that user.
        If prioritized is True, then the returned lists of jobs
        will be sorted by job.priority, high to low.
        :param prioritized:
        """
        pass

    @abstractmethod
    def get_scheduled_jobs_by_type(self, prioritized=False):
        """
        Get a list of all scheduled jobs per type.
        Returns dictionary where the items are:
        {type, [list of scheduled jobs]}
        If prioritized is True, then the returned lists of jobs
        will be sorted by job.priority, high to low.
        :param prioritized:
        """
        pass

    @abstractmethod
    def get_unscheduled_jobs(self):
        """
        Get a list of all unscheduled jobs in the container, or [] if there are no unscheduled jobs.
        """
        pass

    @abstractmethod
    def get_unscheduled_jobs_sorted_by_id(self):
        """
        Get a list of all unscheduled jobs in the container sorted by their job.id
        or [] if no unscheduled jobs.
        """
        pass

    @abstractmethod
    def get_unscheduled_jobs_by_users(self, prioritized=False):
        """
        Get a list of all unscheduled jobs per user.
        Returns dictionary where the items are:
        (user, [list of unscheduled jobs])
        If a user does not have any unscheduled jobs, then there will be no entry for that user.
        If prioritized is True, then the returned lists of jobs
        will be sorted by job.priority, high to low.
        :param prioritized:
        """
        pass

    @abstractmethod
    def get_unscheduled_jobs_by_type(self, prioritized=False):
        """
        Get a list of all unscheduled jobs per type.
        Returns dictionary where the items are:
        {type, [list of unscheduled jobs]}
        If prioritized i True, then the returned lists of jobs
        will be sorted by job.priority, high to low.
        :param prioritized:
        """
        pass

    @abstractmethod
    def get_high_priority_jobs(self):
        """
        Get a list of all high priority jobs in the container
        or [] if there are no high priority jobs.
        A job is said to have high priority if job.high_priority != 0
        """
        pass

    @abstractmethod
    def get_high_priority_jobs_by_users(self, prioritized=False):
        """
        Get a list of all high priority jobs per user.
        Returns dictionary where the items are:
        (user, [list of high priority jobs])
        If a user does not have any high priority jobs, then there will be no entry for that user.
        If prioritized is True, then the returned lists of jobs will be sorted by job.priority, high to low.
        :param prioritized:
        """
        pass

    @abstractmethod
    def find_unscheduled_jobs_with_matching_reqs(self, user, job, num=0):
        """
        Finds up to N jobs matching the requirements of the given job.
        If N == 0, then all matching jobs are returned.
        :param user:
        :param job:
        :param num:
        """
        pass

    @abstractmethod
    def is_empty(self):
        """
        Returns True if the container has no jobs, returns False otherwise.
        """
        pass

    @abstractmethod
    def __str__(self):
        """
        Returns a string containing human-readable information about this container.
        """
        pass




class HashTableJobContainer(JobContainer):

    """
    This class implements a job container based on hash tables.

    """
    # class attributes
    all_jobs = None
    new_jobs = None
    sched_jobs = None
    jobs_by_user = None

    def __init__(self):
        """
        constructor
        """
        JobContainer.__init__(self)
        self.all_jobs = {}
        self.new_jobs = {}
        self.sched_jobs = {}
        self.jobs_by_user = defaultdict(dict)
        log.verbose('HashTableJobContainer instance created.')

    def __str__(self):
        return 'HashTableJobContainer [# of jobs: %d (unshed: %d sched: %d)]' %\
               (len(self.all_jobs), len(self.new_jobs), len(self.sched_jobs))

    def has_job(self, jobid):
        return self.get_job_by_id(jobid) != None

    def add_job(self, job):
        with self.lock:
            self.all_jobs[job.id] = job
            self.jobs_by_user[job.user][job.id] = job

            # Update scheduled/unscheduled maps too:
            if job.status == "Unscheduled":
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
        """
        get the jobs in held state.
        :return:
        """
        held = 5
        held_jobs = []
        for job in self.all_jobs.values():
            if job.job_status == held:
                held_jobs.append(job)
        return held_jobs

    def get_idle_jobs(self):
        """
        get the jobs in idle state.
        :return:
        """
        idle = 1
        idle_jobs = []
        for job in self.all_jobs.values():
            if job.job_status == idle:
                idle_jobs.append(job)
        return idle_jobs

    def get_running_jobs(self):
        """
        Gets the jobs in running state.
        :return:
        """
        running = 2
        run_jobs = []
        for job in self.all_jobs.values():
            if job.job_status == running:
                run_jobs.append(job)
        return run_jobs

    def get_complete_jobs(self):
        """
        Gets all the completed jobs.
        :return:
        """
        complete = 4
        comp_jobs = []
        for job in self.all_jobs.values():
            if job.job_status == complete:
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
        """
        get all scheduled jobs.
        :return:
        """
        return self.sched_jobs.values()

    def get_scheduled_jobs_sorted_by_id(self):
        """
        get scheduled jobs sorted by condor id.
        :return:
        """
        return_value = []
        for jobid in sorted(self.sched_jobs.iteritems()):
            return_value.append(jobid[1])
        return return_value

    def get_scheduled_jobs_by_users(self, prioritized=False):
        """
        get scheduled jobs grouped by user.
        :param prioritized:
        :return:
        """
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
        """
        get scheduled jobs grouped by vmtype.
        :param prioritized:
        :return:
        """
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
        """
        get scheduled jobs gruoped by user:type.
        :param prioritized:
        :return:
        """
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
        """
        Get all unscheduled jobs.
        :return:
        """
        return self.new_jobs.values()

    def get_unscheduled_jobs_sorted_by_id(self):
        """
        Get unscheduled jobs sorted by condor id.
        :return:
        """
        return_value = []
        for jobid in sorted(self.new_jobs.iteritems()):
            return_value.append(jobid[1])
        return return_value

    def get_unscheduled_jobs_by_users(self, prioritized=False):
        """
        Get the unscheduled jobs grouped by user.
        :param prioritized:
        :return:
        """
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
        """
        Get the unscheduled jobs grouped by vmtype.
        :param prioritized:
        :return:
        """
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
        """
        Get the unscheduled jobs grouped by user:type.
        :param prioritized:
        :return:
        """
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
        """
        Get all the jobs flagged high priority.
        :return:
        """
        jobs = []
        for job in self.all_jobs.values():
            if job.high_priority:
                jobs.append(job)
        return jobs

    def get_high_priority_jobs_by_users(self, prioritized=False):
        """
        Get all high priority flagged jobs group by user.
        :param prioritized:
        :return:
        """
        with self.lock:
            return_value = defaultdict(list)
            for job in self.get_high_priority_jobs():
                return_value[job.user].append(job)
            # Now lets sort if needed.
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)

            return return_value

    def get_unscheduled_high_priority_jobs(self):
        """
        Get the unscheduled jobs flagged high priority.
        :return:
        """
        jobs = []
        for job in self.all_jobs.values():
            if job.high_priority and job.status == 'Unscheduled':
                jobs.append(job)
        return jobs

    def get_unscheduled_high_priority_jobs_by_users(self, prioritized=False):
        """
        Return jobs flagged high priority grouped by user.
        :param prioritized:
        :return:
        """
        with self.lock:
            return_value = defaultdict(list)
            for job in self.get_unscheduled_high_priority_jobs():
                return_value[job.user].append(job)
            # Now lets sort if needed.
            if prioritized:
                for job_list in return_value.values():
                    job_list.sort(key=lambda job: job.get_priority(), reverse=True)

            return return_value

    def is_empty(self):
        """
        Check if container is empty.
        :return:
        """
        return len(self.all_jobs) == 0

    def update_job_status(self, jobid, status, remote, servertime, starttime):
        """
        Update the status of all jobs.
        :param jobid:
        :param status:
        :param remote:
        :param servertime:
        :param starttime:
        :return:
        """
        with self.lock:
            job = self.get_job_by_id(jobid)
        if job != None:
            if job.job_status != status and job.override_status != None:
                job.override_status = None
            if job.job_status != status:
                log.debug("Job %s status change: %s -> %s" % (job.id, self.job_status_list[job.job_status],
                                                              self.job_status_list[status]))
            job.job_status = status
            job.remote_host = remote
            job.servertime = int(servertime)
            job.jobstarttime = int(starttime)
            if job.banned and job.ban_time:
                if (time.time() - job.ban_time) > config_val.getint('global', 'job_ban_timeout'):
                    job.banned = False
                    job.ban_time = None
                    job.override_status = None
            if len(job.blocked_clouds) > 0:
                if (time.time() - job.block_time) > config_val.getint('global', 'job_ban_timeout'):
                    job.blocked_clouds = []
                    job.block_time = None
            return True
        else:
            return False

    def schedule_job(self, jobid):
        """
        Set a job's state to scheduled.
        :param jobid:
        :return:
        """
        with self.lock:
            if jobid in self.new_jobs:
                job = self.new_jobs[jobid]
                job.set_status("Scheduled")
                self.sched_jobs[jobid] = job
                del self.new_jobs[jobid]
                return True
            else:
                return False

    def unschedule_job(self, jobid):
        """
        Change a job's state to unscheduled.
        :param jobid:
        :return:
        """
        with self.lock:
            if jobid in self.sched_jobs:
                job = self.sched_jobs[jobid]
                job.set_status("Unscheduled")
                self.new_jobs[jobid] = job
                del self.sched_jobs[jobid]
                return True
            else:
                return False

    def find_unscheduled_jobs_with_matching_reqs(self, user, job, num=0):
        """
        Look for unscheduled jobs with the same requirements as job.
        :param user:
        :param job:
        :param num:
        :return:
        """
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
                    if counter > 0 and counter == num:
                        break

            return matching_jobs

    def get_unscheduled_user_jobs_by_type(self, user, prioritized=False):
        """
        Get unscheduled jobs for user by their vmtype.
        :param user:
        :param prioritized:
        :return:
        """
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
        """
        Get unscheduled jobs for user by their usertype.
        :param user:
        :param prioritized:
        :return:
        """
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
        """
        Get scheduled jobs belonging to user, by only their vmtype.
        I think this function isn't really used much.
        :param user:
        :param prioritized:
        :return:
        """
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
        """
        Get scheduled jobs belonging to user, by their user:type value.
        :param user:
        :param prioritized:
        :return:
        """
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

