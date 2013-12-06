import time

import Queue


counters = {}


def add(counter_key, timestamp=None):
    # timestamp since epoch in seconds, default to time.time()
    if counter_key in counters:
        counter = counters[counter_key]
    else:
        counter = Queue.PriorityQueue(maxsize=1024 * 64)
        counters[counter_key] = counter
    timestamp = timestamp or time.time()
    counter.put_nowait(timestamp)


def count(counter_key, time_frame_length):
    # time_frame_length in seconds
    if counter_key in counters:
        counter = counters[counter_key]
    else:
        return 0
    lower_bound = time.time() - time_frame_length
    while True:
        try:
            timestamp = counter.get_nowait()
        except Queue.Empty:
            break
        if timestamp > lower_bound:
            counter.put_nowait(timestamp)
            break
    return counter.qsize()