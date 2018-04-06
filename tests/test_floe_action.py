import sys
import re
import os

if __name__ == '__main__' and __package__ is None:
    from os import path
    sys.path.append(path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import Config
from plugin import Plugin

# invoke a floe action handler with command-line argument string

def usage():
    print('usage: python test_action.py action config src argstr')
    print('  config = config file to use')
    print('  src = path/name of source file')
    print('  argstr = argument string in format expected by plugin')
    sys.exit(1)

use_argv = False
if use_argv:
    if len(sys.argv) < 5:
        usage()
    (action, config_file, src, argstr) = sys.argv[1:6]
else:
    action = 'smoke_test'
    config_file = '/home/ubuntu/floe/configs/tests/tests.config'
    src = '/data/floe_data/configs/jang/input/MODEL_OUTPUT2017/REB.2017-07-27.nc'
    argstr = "dest: %s " % (
        '/home/floeuser/floe/temp/smoke_test_out.nc'
    )

config = Config(config_file)

def interpret(val, finfo=None, symbols=None, options=None):
    if finfo and val in finfo:
        return finfo[val]
    if symbols and val in symbols:
        return symbols[val]
    return None

try:
    config.plugin = Plugin()
    plugin_root = config.get('local', 'plugins', return_none=True)
    if plugin_root:
        Config.log("loading plugins from '%s'" % plugin_root)
        config.plugin.import_folder(plugin_root)
        Config.log("plugins loaded:\n%s" % config.plugin.list(pretty=True))
    else:
        raise Exception('plugins in [local] section not configured')

    finfo = {'full': src}
    plug = config.plugin.get(action)
    if not plug:
        print("action '%s' not found" % action)
    ret = plug(interpret, finfo, argstr)
    print("return value:\n" + str(ret))
    sys.exit(0)
except Exception as exc:
    Config.log("outer exception: '%s" % str(exc))
    sys.exit(1)
