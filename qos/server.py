import gevent.monkey

gevent.monkey.patch_all()

import gevent.server
import gevent.pool
import logging
import signal
import os
from . import settings
import socket
import errno
import sys
import httplib
import time
import json
from . import queue

LOGGER = logging.getLogger(__name__)
HTTP_STATUS_TOO_MANY_REQUESTS = 429


def main(*argv):
    logging.basicConfig(level=logging.INFO)
    signal.signal(signal.SIGINT, lambda signum, fame: queue.shutdown())
    for frontend_name, frontend_config in settings.FRONTENDS.items():
        gevent.spawn(start_frontend, frontend_name, frontend_config)
    queue.serve_forever(
        schedule_job=schedule_job,
        check_quota=check_quota,
        handle_ready_job=handle_ready_job)
    gevent.sleep(5)


def start_frontend(frontend_name, frontend_config):
    assert 'http' == frontend_config['type']
    try:
        address = (frontend_config['host'], frontend_config['port'])
        server = gevent.server.StreamServer(address, handle_frontend_http)
        LOGGER.info('serving frontend %s on port %s:%s...'
                    % (frontend_name, frontend_config['host'], frontend_config['port']))
    except:
        LOGGER.exception('failed to start frontend %s' % frontend_name)
        os._exit(1)
    server.serve_forever()


def handle_frontend_http(frontend_sock, frontend_sock_address):
    job = queue.new_job()
    job.frontend_sock = frontend_sock
    job.peeked_data = ''
    try:
        try:
            recv_until_http_header_ended(job)
            parse_http_headers(job)
            payload_config = job.on_http_headers_received()
            max_payload_len = payload_config['max_payload_len']
            payload_type = payload_config['payload_type']
            backend_config = settings.BACKENDS[job.backend]
            if 'rq' == backend_config['type']:
                max_payload_len = -1 # receive all
                payload_type = 'json'
            recv_http_payload(job, max_payload_len)
            parse_http_payload(job, payload_type)
            job.on_http_payload_received()
            if 'rq' == backend_config['type']:
                assert job.payload['job_name']
                HttpException(httplib.OK, 'OK').send_error_response(job.frontend_sock)
                job.frontend_sock.close()
                job.frontend_sock = None
                if '/enqueue_at' == job.path:
                    queue.enqueue_delayed(job, job.payload['at'])
                    return
            if schedule_job(job):
                queue.enqueue_ready(job)
        except HttpException:
            raise
        except:
            LOGGER.exception('[%s] failed to parse request' % job)
            raise HttpException(httplib.INTERNAL_SERVER_ERROR, 'internal server error')
    except HttpException as e:
        e.send_error_response(job.frontend_sock)
        return


def schedule_job(job):
    should_process = _schedule_job(job)
    if should_process:
        if queue.enqueue_quota_exceeded(job, (('backend', job.backend),)):
            return False
    return should_process


def _schedule_job(job):
    action = job.on_scheduling()
    if isinstance(action, (list, tuple)):
        action_name, action_args = action
    else:
        action_name = action
        action_args = {}
    if settings.ACTION_PROCESS == action_name:
        return True
    elif settings.ACTION_REJECT == action_name:
        LOGGER.info('[%s] reject' % job)
        raise HttpException(HTTP_STATUS_TOO_MANY_REQUESTS, 'too many requests')
    elif settings.ACTION_DELAY == action_name:
        until = action_args['until']
        LOGGER.info('[%s] delay until %s' % (job, until))
        queue.enqueue_delayed(job, until)
    elif settings.ACTION_CHECK_QUOTA == action_name:
        job_group = action_args['job_group']
        job_group = tuple((k, job_group[k]) for k in sorted(job_group.keys()))
        return not queue.enqueue_quota_exceeded(job, job_group)
    else:
        LOGGER.error('[%s] unknown action' % job)
    return False


def check_quota(job_group):
    job_group = dict(job_group)
    if ['backend'] == job_group.keys(): # (('backend': name),)
        return check_outstanding_requests(job_group['backend'])
    return settings.check_quota(**job_group)


def check_outstanding_requests(backend_name):
    backend_config = settings.BACKENDS[backend_name]
    limit = backend_config['outstanding_requests_limit']
    if 'http' == backend_config['type']:
        count = queue.get_outstanding_requests_count(backend_name)
        if count > limit:
            return time.time() + 1
        else:
            return None
    elif 'rq' == backend_config['type']:
        import rq

        rq_queue = rq.Queue(name=backend_config['queue'], connection=backend_config['connection'])
        count = queue.get_outstanding_requests_count(backend_name) + rq_queue.count
        if count > limit:
            return time.time() + 1
        else:
            return None
    else:
        raise NotImplementedError('unsupported backend type: %s' % backend_config['type'])


def handle_ready_job(job):
    try:
        job.on_processing()
        spawn_job(job)
    except:
        LOGGER.exception('[%s] failed to handle ready item' % job)


def spawn_job(job):
    backend_name = job.backend
    backend_config = settings.BACKENDS[backend_name]
    if 'http' == backend_config['type']:
        gevent.spawn(process_http_backed_job, job, backend_config)
    elif 'rq' == backend_config['type']:
        import rq

        rq_queue = rq.Queue(name=backend_config['queue'], connection=backend_config['connection'])
        rq_queue.enqueue(
            job.payload['job_name'], result_ttl=job.payload['result_ttl'], timeout=job.payload['timeout'],
            args=job.payload['args'], kwargs=job.payload['kwargs'])
    else:
        raise NotImplementedError('unsupported backend type: %s' % backend_config['type'])


def process_http_backed_job(job, backend_config):
    try:
        queue.increase_processing_jobs_count(job.backend)
        try:
            job.backend_sock = socket.socket()
            job.backend_sock.connect((backend_config['host'], backend_config['port']))
        except HttpException:
            raise
        except:
            LOGGER.exception('[%s] failed to connect backend' % job)
            raise HttpException(httplib.INTERNAL_SERVER_ERROR, 'internal server error')
        forward(job)
    except HttpException as e:
        e.send_error_response(job.frontend_sock)
        return
    except:
        LOGGER.exception('[%s] failed to process' % job)
    finally:
        queue.decrease_processing_jobs_count(job.backend)
        try:
            job.frontend_sock.close()
        except:
            pass
        if job.backend_sock:
            try:
                job.backend_sock.close()
            except:
                pass


def orc_key(backend_name):
    return 'outstanding_requests_count:%s' % backend_name


def recv_http_payload(job, max_payload_len=0):
    if not max_payload_len:
        return
    if 'Content-Length' in job.headers:
        payload_len = int(job.headers.get('Content-Length', 0))
        if 0 < max_payload_len < payload_len:
            raise Exception('payload too large')
        _, _, partial_payload = job.peeked_data.partition(b'\r\n\r\n')
        more_payload_len = payload_len - len(partial_payload)
        more_payload = ''
        if more_payload_len > 0:
            more_payload = recv_sock_until_len(job.frontend_sock, more_payload_len)
            job.peeked_data += more_payload
        job.payload = partial_payload + more_payload
    else:
        raise Exception('does not support reading payload from no content length request')


def parse_http_payload(job, payload_type):
    if not job.payload:
        return
    if 'raw' == payload_type:
        return
    if 'json' == payload_type:
        job.payload = json.loads(job.payload)
        return
    raise NotImplementedError('unsupported payload type: %s' % payload_type)


def recv_sock_until_len(sock, until_len, buffer_size=8192):
    bytes = ''
    while until_len > 0:
        buffer = sock.recv(buffer_size)
        if not buffer:
            raise Exception('not enough data')
        until_len -= len(buffer)
        bytes += buffer
    return bytes


def recv_until_http_header_ended(job):
    # peeked_data are the bytes already read from the underlying sock
    for i in range(16):
        if job.peeked_data.find(b'\r\n\r\n') != -1:
            return
        more_data = job.frontend_sock.recv(8192)
        if not more_data:
            raise InvalidHttpRequest('http header incomplete')
        job.peeked_data += more_data
    raise InvalidHttpRequest('http header is too large')


def parse_http_headers(job):
    lines = job.peeked_data.splitlines()
    job.method, job.path = lines[0].split()[:2]
    job.headers = dict()
    for line in lines[1:]:
        if not line:
            break
        keyword, _, value = line.partition(b':')
        keyword = keyword.title() # abc: => Abc:
        value = value.strip()
        job.headers[keyword] = value
    job.description = '%s => %s %s%s' % (job.job_id, job.method, job.headers.get('Host'), job.path)


class InvalidHttpRequest(Exception):
    pass


def forward(job, buffer_size=8192):
    job.backend_sock.sendall(job.peeked_data)

    def from_backend_to_frontend():
        try:
            while True:
                data = job.backend_sock.recv(buffer_size)
                if data:
                    job.frontend_sock.sendall(data)
                else:
                    return
        except socket.error as e:
            if e[0] not in (10053, 10054, 10057, errno.EPIPE):
                return e
        except gevent.GreenletExit:
            return
        except:
            LOGGER.exception('forward u2d failed')
            return sys.exc_info()[1]

    def from_frontend_to_backend():
        try:
            while True:
                data = job.frontend_sock.recv(buffer_size)
                if data:
                    job.backend_sock.sendall(data)
                else:
                    return
        except socket.error as e:
            if e[0] not in (10053, 10054, 10057, errno.EPIPE):
                return e
        except gevent.GreenletExit:
            return
        except:
            LOGGER.exception('forward d2u failed')
            return sys.exc_info()[1]
        finally:
            job.backend_sock.close()

    u2d = gevent.spawn(from_backend_to_frontend)
    d2u = gevent.spawn(from_frontend_to_backend)
    try:
        e = u2d.join()
        if e:
            raise e
        try:
            job.backend_sock.close()
        except:
            pass
    finally:
        try:
            u2d.kill()
        except:
            pass
        try:
            d2u.kill()
        except:
            pass


class HttpException(Exception):
    def __init__(self, status, status_message=None, headers=None, body=None):
        self.status = status
        self.status_message = status_message
        self.headers = headers
        self.body = body

    def send_error_response(self, sock):
        try:
            sock.sendall('HTTP/1.1 %d %s\r\n' % (self.status, self.status_message or ''))
            for k, v in (self.headers or {}).items():
                sock.sendall('%s: %s\r\n' % (k, v))
            sock.sendall('\r\n')
            if self.body:
                sock.sendall(self.body)
        except:
            LOGGER.exception('failed to send error response')