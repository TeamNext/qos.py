import sys
import importlib
import logging
import logging.handlers

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s: %(message)s')

try:
    if len(sys.argv) > 1:
        config_name = sys.argv[1]
    else:
        config_name = 'default_settings'
    LOGGER.info('load config from etc/%s.py' % config_name)
    module = importlib.import_module('qos.etc.%s' % config_name)
except ImportError:
    LOGGER.critical('config file not found')
    sys.exit(1)

reload(module)
for member_name in dir(module):
    if not member_name.startswith('_'):
        locals()[member_name] = getattr(module, member_name)

qos_logger = logging.getLogger('qos')
if 0 == len(qos_logger.handlers):
    qos_logger.propagate = False
    if 'LOGGER_CONFIG' in locals():
        config = locals()['LOGGER_CONFIG']
        level = getattr(logging, config['level'])
        qos_logger.setLevel(level)
        file_handler = logging.handlers.RotatingFileHandler(
            config['path'], maxBytes=config['max_bytes'], backupCount=config['backup_count'])
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
        qos_logger.addHandler(file_handler)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
        qos_logger.addHandler(stream_handler)
    elif 'config_logger' in locals():
        locals()['config_logger'](qos_logger)
    else:
        qos_logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
        qos_logger.addHandler(handler)
