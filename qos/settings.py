import sys
import importlib
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

try:
    if len(sys.argv) > 1:
        config_name = sys.argv[1]
    else:
        config_name = 'default_settings'
    LOGGER.info('load config from etc/%s.py' % config_name)
    module = importlib.import_module('qos.etc.%s' % config_name)
except ImportError:
    print('config file not found')
    sys.exit(1)

reload(module)
for member_name in dir(module):
    if not member_name.startswith('_'):
        locals()[member_name] = getattr(module, member_name)