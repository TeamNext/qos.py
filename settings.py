import sys
if len(sys.argv) > 1:
    import importlib

    module = importlib.import_module('qos.etc.%s' % sys.argv[1])

    for member_name in dir(module):
        if not member_name.startswith('_'):
            locals()[member_name] = getattr(module, member_name)