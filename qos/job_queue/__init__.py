from .. import settings

if 'redis' == settings.QUEUE_TYPE:
    from .redis_queue import *
elif 'in-memory' == settings.QUEUE_TYPE:
    from .in_memory_queue import *
else:
    raise NotImplementedError('unknown queue type: %s' % settings.QUEUE_TYPE)