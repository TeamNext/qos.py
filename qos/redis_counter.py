import time

def add(redis_server, counter_key, timestamp=None):
    if isinstance(counter_key, tuple):
        counter_key = '|'.join(counter_key)
    # timestamp since epoch in seconds, default to time.time()
    timestamp = timestamp or time.time()
    redis_server.zadd(counter_key, timestamp, timestamp)


def count(redis_server, counter_key, time_frame_length):
    if isinstance(counter_key, tuple):
        counter_key = '|'.join(counter_key)
    # time_frame_length in seconds
    time_frame_end = time.time()
    time_frame_begin = time_frame_end - time_frame_length
    redis_server.zremrangebyscore(counter_key, 0, time_frame_begin)
    return redis_server.zcard(counter_key)