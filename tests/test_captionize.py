import sys
import re
import os

if __name__ == '__main__' and __package__ is None:
    from os import path
    sys.path.append(path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import util as u
from config import Config
from plugin import Plugin

# invoke a floe action handler with command-line argument string

def usage():
    print('usage: python test_action.py config src argstr')
    print('  config = config file to use')
    print('  src = path/name of source file')
    print('  argstr = argument string in format expected by plugin')
    sys.exit(1)

use_argv = False
if use_argv:
    if len(sys.argv) < 4:
        usage()
    (config_file, src, argstr) = sys.argv[1:4]
else:
    config_file = 'tests.config'
    src = '/home/floeuser/floe/temp/frame0s/output/daily/2017-09-02/frame0/Arctic10.gif_frame_00000.gif'
    argstr = "dest: %s, where: %s, font_size: %s, bar_size: %s, " % (
        '/home/floeuser/floe/temp/captionized.gif',
        'top',
        '25',
        '60'
    )
    argstr = argstr + 'pad_x: %s, pad_y: %s, text: "%s", ' % (
        '20', '20', "all's well that ends well"
    )
    argstr = argstr + 'background_color: %s, text_color: %s, font: %s' % (
        'teal', 'white', 'Palatino-Roman'
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
    captionize = config.plugin.get('captionize')
    ret = captionize(interpret, finfo, argstr)
    print("return value:\n" + str(ret))
    sys.exit(0)
except Exception as exc:
    Config.log("outer exception: '%s" % str(exc))
    sys.exit(1)
