import sys
import logging
from datetime import datetime

from config import Config
from ftp_mgr import FtpMgr
from tree_processor import TreeProcessor
from webmaker import WebMaker
import util as u
from plugin import Plugin

def load_config():
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = 'tests.config'
    return Config(config_file)

"""TODO - S3 is now a plugin
def s3_speed_test():
    config = load_config()
    s3 = S3(config)
    s3.speed_test(1024)
    s3.speed_test(1024 * 1024)
"""

def daynum_test():
    nau = datetime.now()
    print(u.datetime_to_daynum(nau))
    print(u.datetime_to_daynum('2017-10-16', options={'std_string':True}))
    print(u.daynum_to_datestr(17456))

def plugin_test():
    plugin = Plugin()
    plugin.import_folder('/home/floeuser/floe/dev/plugins')
    
    for name in plugin.list():
        print(name)
    print(plugin.call('func1'))
    plugin.call('func2', 'pos arg 1', 'pos arg 2', kwarg1='kwarg1', kwarg2='kwarg2', kwarg3='kwarg3')
    print(plugin.call('func3'))
    print(plugin.call('func4'))

def includes_test():
    file = u.process_includes('parent.txt')
    lines = file.read().splitlines()
    print('\n'.join(lines))

def file_dest_speed_test(dest_mgr, size):
    name = "tempfile.text"
    with open(name, "wb") as fh:
        fh.write(b"\0" * size)

    start = u.timestamp_now()
    dest_mgr.upload_finfo(".", name, name)
    elapsed = u.timestamp_now() - start
    rate = size / elapsed
    logging.debug("FileDest object uploaded, key = '%s', %i bytes, %f sec, %f bytes/sec" %
                  (name, size, elapsed, rate))

def test_age_file():
    test = '/home/floeuser/test_age_file'
    dest = test + '/' + 'aged'
    u.ensure_path(dest)
    src = test + '/' + 'foo.bar'
    with open(src, 'w') as fh:
        fh.write('foo.bar')
    for i in range(1, 510):
        start = u.timestamp_now()
        u.age_file(src, dest)
        print("iter %i took %f seconds" % (i, u.timestamp_now() - start))

def test_join_lines_comma():
    test = [
        'this, ',
        'is a test',
        'of the ,',
        'requested,',
        'operation,'
    ]
    print(u.join_lines_comma(test))


#plugin_test()
includes_test()