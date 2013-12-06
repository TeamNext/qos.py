import logging

LOGGER = logging.getLogger(__name__)
HTTP_STATUS_TOO_MANY_REQUESTS = 429


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


class TooManyRequestsHttpException(HttpException):
    def __init__(self, headers=None, body=None):
        super(TooManyRequestsHttpException, self).__init__(
            HTTP_STATUS_TOO_MANY_REQUESTS, 'too many requests', headers, body)