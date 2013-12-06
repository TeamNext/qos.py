QUEUE_TYPE = 'in-memory'

FRONTEND_DEFAULT = {
    'type': 'http', # only support http right now
    'host': '', # bind to all ip
    'port': 10010
}

FRONTENDS = {
    'default': FRONTEND_DEFAULT
}

BACKEND_BK_COMP = {
    'type': 'http', # pass request in memory
    'host': 't.ied.com',
    'port': 80,
    'outstanding_requests_limit': 20
}

BACKENDS = {
    'bk-comp': BACKEND_BK_COMP
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
    def on_scheduling(self):
        return ACTION_PROCESS
        # return ACTION_CHECK_QUOTA, {'job_group': self.job_group}
        # we can also reject here
        # if get_recheck_time(**self.business_key):
        #     return ACTION_REJECT
        #     return ACTION_DELAY, {'until': time.time() + 1}
        # else:
        #     return ACTION_PROCESS

    @property
    def backend(self):
        return 'bk-comp'


JOB_CLASS = CustomizedJob


def check_quota(*args, **kwargs):
    return 0


