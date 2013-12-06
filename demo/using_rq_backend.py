import redis

QUEUE_TYPE = 'redis'
REDIS_SERVER = redis.StrictRedis()

FRONTEND_DEFAULT = {
    'type': 'http', # only support http right now
    'host': '', # bind to all ip
    'port': 10086
}

FRONTENDS = {
    'default': FRONTEND_DEFAULT
}

BACKEND_RQ = {
    'type': 'rq', # pass request in memory
    'connection': REDIS_SERVER,
    'queue': 'default',
    'outstanding_requests_limit': 64
}

BACKENDS = {
    'rq': BACKEND_RQ
}

SKIP_PAYLOAD = 0

ACTION_PROCESS = 'process'
ACTION_DELAY = 'delay'
ACTION_REJECT = 'reject'
ACTION_CHECK_QUOTA = 'check_quota'


class HttpJob(object):
    def __init__(self, job_id):
        self.job_id = job_id
        self.method = ''
        self.path = ''
        self.headers = {}
        self.payload = ''
        self.description = '%s => unknown' % job_id
        self.priority = 1

    def on_http_headers_received(self):
        return {
            'max_payload_len': SKIP_PAYLOAD,
            'payload_type': 'form'
        }

    def on_http_payload_received(self):
        return

    def on_scheduling(self):
        return ACTION_PROCESS

    def on_processing(self):
        return

    def on_quota_exceeded(self, job_group, queue_length):
        return True # queue up
        # return False # reject immediately

    @property
    def backend(self):
        return None

    def __repr__(self):
        return self.description


class CustomizedJob(HttpJob):
    def on_http_payload_received(self):
        assert 'default' == self.payload['queue']
        if 'root_task_id' in self.payload['job_group']:
            root_task_id = self.payload['job_group']['root_task_id']
            self.priority = REDIS_SERVER.incr('root_task:%s' % root_task_id)
            REDIS_SERVER.expire('root_task:%s' % root_task_id, 60)

    @property
    def backend(self):
        return 'rq'


JOB_CLASS = CustomizedJob


def check_quota(*args, **kwargs):
    return None


