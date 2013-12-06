import time
import logging

import gevent.queue
from .. import settings

LOGGER = logging.getLogger(__name__)

__all__ = [
    'serve_forever', 'new_job', 'get_outstanding_requests_count', 'shutdown',
    'enqueue_ready', 'enqueue_delayed', 'enqueue_quota_exceeded',
    'increase_processing_jobs_count', 'decrease_processing_jobs_count'
]

# TODO: handle queue full when put_nowait

DEFAULT_QUEUE_SIZE = 1024 * 64

# job will be in one of the following three kinds of queues
ready_job_queue = gevent.queue.PriorityQueue(DEFAULT_QUEUE_SIZE)
delayed_job_queue = gevent.queue.PriorityQueue(DEFAULT_QUEUE_SIZE)
quota_exceeded_job_queues = {} # job group => priority queue

# recheck quota
recheck_request_queue = gevent.queue.PriorityQueue(DEFAULT_QUEUE_SIZE)
recheck_time_cache = {} # job group => recheck time

# callback
schedule_job = None
check_quota = None
handle_ready_job = None

# global state
stopped = False
last_job_id = 0
ready_jobs_counts = {}
processing_jobs_counts = {}


def shutdown():
    global stopped
    LOGGER.info('exiting...')
    stopped = True


def serve_forever(**kwargs):
    global schedule_job
    global check_quota
    global handle_ready_job
    schedule_job = kwargs['schedule_job']
    check_quota = kwargs['check_quota']
    handle_ready_job = kwargs['handle_ready_job']
    gevent.spawn(handle_delayed_job_queue)
    gevent.spawn(handle_recheck_request_queue)
    handle_ready_job_queue()


def new_job():
    global last_job_id
    last_job_id += 1
    return settings.JOB_CLASS('#%s' % last_job_id)


class RecheckRequest(object):
    def __init__(self, job_group, recheck_time):
        self.job_group = job_group
        self.recheck_time = recheck_time

    def __cmp__(self, other):
        return cmp(self.recheck_time, other.recheck_time)


class PrioritizedItem(object):
    def __init__(self, job):
        self.job = job

    def __cmp__(self, other):
        return cmp(self.job.priority, other.job.priority)


class DelayedItem(object):
    def __init__(self, job, until):
        self.job = job
        self.until = until

    def __cmp__(self, other):
        return cmp(self.until, other.until)


def enqueue_ready(job):
    ready_jobs_counts[job.backend] = ready_jobs_counts.get(job.backend, 0) + 1
    ready_job_queue.put_nowait(PrioritizedItem(job))


def enqueue_delayed(job, until):
    delayed_job_queue.put_nowait(DelayedItem(job, until))


def enqueue_quota_exceeded(job, job_group): # returns True if quota exceeded
    recheck_time = recheck_time_cache.get(job_group)
    if recheck_time and recheck_time > time.time():
        quota_exceeded_job_queue = quota_exceeded_job_queues.setdefault(job_group,
                                                                        gevent.queue.PriorityQueue(DEFAULT_QUEUE_SIZE))
        job.on_quota_exceeded(job_group, quota_exceeded_job_queue.qsize)
        quota_exceeded_job_queue.put_nowait(PrioritizedItem(job))
        LOGGER.debug('[%s] skip recheck, back off to quota exceeded queue %s' % (job, job_group))
        return True
    recheck_time = check_quota(job_group)
    if recheck_time:
        quota_exceeded_job_queue = quota_exceeded_job_queues.setdefault(job_group,
                                                                        gevent.queue.PriorityQueue(DEFAULT_QUEUE_SIZE))
        job.on_quota_exceeded(job_group, quota_exceeded_job_queue.qsize)
        quota_exceeded_job_queue.put_nowait(PrioritizedItem(job))
        recheck_time_cache[job_group] = recheck_time
        recheck_request_queue.put_nowait(RecheckRequest(job_group, recheck_time))
        LOGGER.info('[%s] checked and back off to quota exceeded queue %s' % (job, job_group))
        return True
    else:
        return False # process now


def handle_recheck_request_queue():
    while not stopped:
        try:
            recheck_request = queue_pop(recheck_request_queue)
            job_group = recheck_request.job_group
            delta = recheck_request.recheck_time - time.time()
            if delta > 0:
                if delta < 0.1:
                    delta = 0.1
                gevent.sleep(delta)
            recheck_time = check_quota(job_group)
            if recheck_time and recheck_time > time.time():
                recheck_time_cache[job_group] = recheck_time
                recheck_request_queue.put_nowait(RecheckRequest(job_group, recheck_time))
                LOGGER.debug('%s quota exceeded, skip flush' % str(job_group))
            else:
                flushed_all, count = flush_job_group(job_group)
                LOGGER.info('%s flushed %s, all: %s' % (str(job_group), count, flushed_all))
                if not flushed_all:
                    # not flushed all might because other job group quota exceeded
                    # following recheck make sure current job group can flushed, eventually
                    if recheck_time_cache.get(job_group, 0) < time.time():
                        recheck_request_queue.put_nowait(RecheckRequest(job_group, time.time()))
        except QueueStopped:
            break
        except:
            LOGGER.exception('failed to handle recheck request')


def flush_job_group(job_group):
    quota_exceeded_job_queue = quota_exceeded_job_queues.get(job_group)
    if not quota_exceeded_job_queue:
        return True, 0
    count = 0
    try:
        while not stopped: # flush until over quota
            prioritized_item = quota_exceeded_job_queue.get(block=False)
            job = prioritized_item.job
            if schedule_job(job):
                count += 1
                enqueue_ready(job)
            else:
                return False, count
    except gevent.queue.Empty:
        return True, count # flushed all


def handle_delayed_job_queue():
    while not stopped:
        try:
            delayed_item = queue_pop(delayed_job_queue)
            job = delayed_item.job
            delta = delayed_item.until - time.time()
            if delta > 0:
                if delta < 0.1:
                    delta = 0.1
                gevent.sleep(delta)
            if schedule_job(job):
                enqueue_ready(job)
        except QueueStopped:
            break
        except:
            LOGGER.exception('[%s] failed to handle delayed job' % locals().get('job'))


def handle_ready_job_queue():
    while not stopped:
        try:
            job = queue_pop(ready_job_queue).job
            ready_jobs_counts[job.backend] = ready_jobs_counts.get(job.backend, 1) - 1
            handle_ready_job(job)
        except QueueStopped:
            break
        except:
            LOGGER.exception('[%s] failed to handle ready job' % locals().get('job'))


def queue_pop(queue):
    while not stopped:
        try:
            return queue.get(timeout=5)
        except gevent.queue.Empty:
            continue
    if stopped:
        raise QueueStopped()


class QueueStopped(Exception):
    pass


def get_outstanding_requests_count(backend_name):
    ready_jobs_count = ready_jobs_counts.get(backend_name, 0)
    processing_jobs_count = processing_jobs_counts.get(backend_name, 0)
    return ready_jobs_count + processing_jobs_count


def increase_processing_jobs_count(backend_name):
    processing_jobs_counts[backend_name] = processing_jobs_counts.get(backend_name, 0) + 1


def decrease_processing_jobs_count(backend_name):
    processing_jobs_counts[backend_name] = processing_jobs_counts.get(backend_name, 0) - 1