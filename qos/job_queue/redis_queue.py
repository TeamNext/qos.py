import time
import logging
import json

import gevent.queue

from .. import settings
from ..http_exception import TooManyRequestsHttpException


LOGGER = logging.getLogger(__name__)

__all__ = [
    'shutdown', 'serve_forever', 'new_job', 'get_outstanding_requests_count',
    'enqueue_ready', 'enqueue_delayed', 'enqueue_quota_exceeded',
    'increase_processing_jobs_count', 'decrease_processing_jobs_count'
]


class PriorityQueue(object):
    class Empty(Exception):
        pass

    def __init__(self, redis_server, key):
        self.redis_server = redis_server
        self.key = key

    def push(self, item, priority):
        with self.redis_server.pipeline() as pipe:
            pipe = pipe.persist(self.key)
            pipe = pipe.zadd(self.key, priority, item)
            pipe = pipe.delete('%s:pushed' % self.key)
            pipe = pipe.rpush('%s:pushed' % self.key, time.time())
            pipe = pipe.expire('%s:pushed' % self.key, 5)
            pipe.execute()

    def pop(self, block=True):
        while not stopped:
            with self.redis_server.pipeline() as pipe:
                pipe = pipe.zrange(self.key, 0, 0, withscores=True)
                pipe = pipe.zremrangebyrank(self.key, 0, 0)
                results, count = pipe.execute()
                if results:
                    return results[0]
            if block:
                self.redis_server.blpop('%s:pushed' % self.key, timeout=5)
                continue
            else:
                raise self.Empty()
        if stopped:
            raise QueueStopped()

    def expire(self, time):
        self.redis_server.expire(self.key, time)


    @property
    def count(self):
        return self.redis_server.zcard(self.key)


    def __len__(self):
        return self.redis_server.zcard(self.key)


# job will be in one of the following three kinds of queues
ready_job_queue = PriorityQueue(settings.REDIS_SERVER, 'ready_jobs')
delayed_job_queue = PriorityQueue(settings.REDIS_SERVER, 'delayed_jobs')

# recheck quota
recheck_request_queue = PriorityQueue(settings.REDIS_SERVER, 'recheck_requests')

# callback
schedule_job = None
check_quota = None
handle_ready_job = None

# global states
stopped = False
job_states = {} # keep in memory

# handle delay event waiting seconds limit
WAIT_LIMIT = 10


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
    job_id = settings.REDIS_SERVER.incr('last_job_id')
    return settings.JOB_CLASS('#%s' % job_id)


def ready_jobs_count_key(backend_name):
    return 'ready_jobs_count:%s' % backend_name


def processing_jobs_count_key(backend_name):
    return 'processing_jobs_count:%s' % backend_name


def enqueue_ready(job):
    settings.REDIS_SERVER.incr(ready_jobs_count_key(job.backend))
    ready_job_queue.push(serialize_job(job), job.priority)


def enqueue_delayed(job, until):
    delayed_job_queue.push(serialize_job(job), until)


def enqueue_quota_exceeded(job, job_group): # returns True if quota exceeded
    recheck_time = float(settings.REDIS_SERVER.get(rtc_key(job_group)) or 0)
    if recheck_time and recheck_time > time.time():
        do_enqueue_quota_exceeded(job, job_group)
        LOGGER.debug('[%s] skip recheck quota, back off to %s' % (job, job_group))
        return True
    recheck_time = check_quota(job_group)
    if recheck_time:
        do_enqueue_quota_exceeded(job, job_group)
        settings.REDIS_SERVER.set(rtc_key(job_group), recheck_time)
        settings.REDIS_SERVER.expire(rtc_key(job_group), 60)
        recheck_request_queue.push(serialize_job_group(job_group), recheck_time)
        LOGGER.info('[%s] quota exceeded, back off to %s' % (job, job_group))
        return True
    else:
        return False # process now


def do_enqueue_quota_exceeded(job, job_group):
    settings.REDIS_SERVER.sadd('job_groups', serialize_job_group(job_group))
    quota_exceeded_job_queue = get_quota_exceeded_job_queue(job_group)
    if job.on_quota_exceeded(job_group, len(quota_exceeded_job_queue)):
        quota_exceeded_job_queue.push(serialize_job(job), job.priority)
    else:
        raise TooManyRequestsHttpException()


def rtc_key(job_group):
    return 'quota_recheck_time:%s' % format_job_group(job_group)


def get_quota_exceeded_job_queue(job_group):
    key = 'quota_exceeded_jobs:%s' % format_job_group(job_group)
    return PriorityQueue(settings.REDIS_SERVER, key)


def format_job_group(job_group):
    return '|'.join(['%s=%s' % kv for kv in job_group])


def handle_recheck_request_queue():
    for serialized_job_group in settings.REDIS_SERVER.smembers('job_groups'):
        recheck_request_queue.push(serialized_job_group, time.time())
    while not stopped:
        try:
            serialized_job_group, recheck_time = recheck_request_queue.pop()
            job_group = deserialize_job_group(serialized_job_group)
            delta = recheck_time - time.time()
            if delta > 0:
                if delta < 0.1:
                    delta = 0.1
                gevent.sleep(delta)
            recheck_time = check_quota(job_group)
            if recheck_time:
                settings.REDIS_SERVER.set(rtc_key(job_group), recheck_time)
                settings.REDIS_SERVER.expire(rtc_key(job_group), 60)
                recheck_request_queue.push(serialize_job_group(job_group), recheck_time)
                LOGGER.debug('%s quota exceeded, skip flush' % serialized_job_group)
            else:
                flushed_all, count = flush_job_group(job_group)
                LOGGER.info('%s flushed %s, all: %s' % (serialize_job_group(job_group), count, flushed_all))
                if not flushed_all:
                    # not flushed all might because other job group quota exceeded
                    # following recheck make sure current job group can flushed, eventually
                    if float(settings.REDIS_SERVER.get(rtc_key(job_group)) or 0) < time.time():
                        recheck_request_queue.push(serialize_job_group(job_group), time.time())
        except QueueStopped:
            break
        except:
            LOGGER.exception('failed to handle recheck request')


def flush_job_group(job_group):
    quota_exceeded_job_queue = get_quota_exceeded_job_queue(job_group)
    count = 0
    try:
        while True:
            serialized_job, priority = quota_exceeded_job_queue.pop(block=False)
            job = deserialize_job(serialized_job)
            if schedule_job(job):
                count += 1
                enqueue_ready(job)
            else:
                return False, count
    except PriorityQueue.Empty:
        settings.REDIS_SERVER.srem('job_groups', serialize_job_group(job_group))
        quota_exceeded_job_queue.expire(5)
        return True, count # flushed all


def handle_delayed_job_queue():
    while not stopped:
        try:
            serialized_job, until = delayed_job_queue.pop() #block here
            delta = until - time.time()
            if delta > 0:
                wait = delta
                if wait < 0.1: wait = 0.1
                if wait > WAIT_LIMIT: wait = WAIT_LIMIT # don't sleep too long!
                delayed_job_queue.push(serialized_job, until) # push back
                gevent.sleep(wait)
                continue

            job = deserialize_job(serialized_job)
            if schedule_job(job):
                enqueue_ready(job)
        except QueueStopped:
            break
        except:
            LOGGER.exception('[%s] failed to handle delayed job' % locals().get('job'))


def handle_ready_job_queue():
    while not stopped:
        try:
            job = deserialize_job(ready_job_queue.pop()[0])
            settings.REDIS_SERVER.decr(ready_jobs_count_key(job.backend))
            remove_job(job)
            handle_ready_job(job)
        except QueueStopped:
            break
        except:
            LOGGER.exception('[%s] failed to handle ready job' % locals().get('job'))


def get_outstanding_requests_count(backend_name):
    ready_jobs_count = int(settings.REDIS_SERVER.get(ready_jobs_count_key(backend_name)) or 0)
    http_requests_count = int(settings.REDIS_SERVER.get(processing_jobs_count_key(backend_name)) or 0)
    return ready_jobs_count + http_requests_count


def serialize_job_group(job_group):
    return json.dumps(job_group)


def deserialize_job_group(serialized_job_group):
    return tuple(tuple(kv) for kv in json.loads(serialized_job_group))


def remove_job(job):
    if job.job_id in job_states:
        del job_states[job.job_id]


def serialize_job(job):
    if getattr(job, 'frontend_sock', None):
        job_states[job.job_id] = {
            'frontend_sock': job.frontend_sock,
            'peeked_data': job.peeked_data
        }
    return json.dumps({
        'job_id': job.job_id,
        'method': job.method,
        'path': job.path,
        'headers': job.headers,
        'payload': job.payload,
        'description': job.description
    })


def deserialize_job(serialized_job):
    d = json.loads(serialized_job)
    job = settings.JOB_CLASS(d['job_id'])
    job.method = d['method']
    job.path = d['path']
    job.headers = d['headers']
    job.payload = d['payload']
    job.description = d['description']
    in_memory_job = job_states.get(job.job_id)
    if in_memory_job:
        job.frontend_sock = in_memory_job['frontend_sock']
        job.peeked_data = in_memory_job['peeked_data']
    return job


class QueueStopped(Exception):
    pass


def increase_processing_jobs_count(backend_name):
    settings.REDIS_SERVER.incr(processing_jobs_count_key(backend_name))


def decrease_processing_jobs_count(backend_name):
    settings.REDIS_SERVER.decr(processing_jobs_count_key(backend_name))
