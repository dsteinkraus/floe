import os
import errno
import re
import shutil
import tarfile
import logging
import hashlib
import math
import calendar
import time
from datetime import datetime, timedelta
import json
import subprocess
import inspect
import tempfile
# import urllib - this is unstable, machine-dependent, see e.g.:
# https://stackoverflow.com/questions/37042152/python-3-5-1-urllib-has-no-attribute-request
import urllib.request

_re = {}
_re['split_white'] = re.compile(r'\s+')
_re['bite'] = re.compile(r'^(([^"]|"[^"]*")*?)\s*(,)\s*(.*)')
_re['colonsep'] = re.compile(r'\s*:\s*')
_re['bracketed'] = re.compile(r'(\[[^\]]*)\]')
_re['std_date_str'] = re.compile(r'(\d{4})-(\d{2})-(\d{2})')

# launch a process, and wait for it to finish (not for long-running
# processes or ones that return vast amounts of data to stdout).
def run_command(args, working_dir='.', **kwargs):
    proc = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=working_dir,
        **kwargs)
    stdout, stderr = proc.communicate()
    return proc.returncode, stdout, stderr

def unpack_marker():
    return str('/__UNPACKED__')

# isolate this in a fn because most solutions have at least some caveat
def ensure_path(dir_):
    try:
        os.makedirs(dir_)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

# use this when you know you have a path + filename
def ensure_path_for_file(full):
    path, _ = os.path.split(full)
    ensure_path(path)

# safely join items into a path regardless of forward slashes already present.
# os.path.join is almost what we want, but if it encounters a leading slash on
# a component, it treats it as an absolute path and throws away all preceding
# elements.
def pathify(*args):
    if not len(args):
        return ''
    init = args[0].startswith('/')
    clean = [item.strip('/') for item in args]
    ret = '/'.join(clean)
    if init:
        ret = '/' + ret
    return ret

# get list of plain files in dir matching spec. Path not included in output.
def plain_files(dir_, wanted_expr):
    if not dir_.endswith('/'):
        dir_ += '/'
    regex = re.compile(wanted_expr)
    return [f for f in os.listdir(dir_) if os.path.isfile(dir_ + f) and re.search(regex, f)]

# empty a folder of all files and subfolders - nothing built in for this
def clear_folder(path):
    for the_file in os.listdir(path):
        file_path = os.path.join(path, the_file)
        if os.path.isfile(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

# remove metadata from a finfo that should not be blindly copied
# TODO maintain, unify with tp.copy_metadata
def remove_no_copy_metadata(finfo):
    # this python idiom deletes key if present
    finfo.pop('groups', None)
    finfo.pop('source_im', None)

# get UTC unix timestamp and other facts about local file
# TODO should accept either path + name, or full
def local_metadata(path, name):
    full = path + '/' + name
    st = os.stat(full)
    ret = {'size': st.st_size,
           'name': name,
           'path': path,
           'full': full,
           'modified': int(st.st_mtime)}
    return ret

# extract metadata from FTP LIST entry
# see https://files.stairways.com/other/ftp-list-specs-info.txt for how
# much of a mess this is. This one works for some servers
def ftp_metadata(entry):
    parts = _re['split_white'].split(entry)
    ret = {'perms': parts[0],
        'size': int(parts[4]),
        'month': parts[5],
        'day': int(parts[6]),
        'time': parts[7],
        'name': parts[8]}
    ret['isdir'] = ret['perms'][0] == 'd'
    return ret

# get md5 hash of file (used to avoid redundant uploads)
# todo: why such small chunk? any effect on perf?
def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# if passed a bound method, just return it. Otherwise, assume
# object and return its 'interpret' method.
def interpret_method(thing):
    if inspect.ismethod(thing):
        return thing
    if callable(thing):
        return thing
    return thing.interpret

# shorthand for presence and truth of key
def is_true(test_dict, key):
    return key in test_dict and test_dict[key]

# this does not keep empty strings (caused by two or more adjacent commas) unless
# 'keep_empty' specified
def comma_split(st, options=None):
    if options is None:
        options = {}
    ret = []
    st = st.strip()
    if not st:
        return ret
    # remove leading comma if any
    st = re.sub(r'^\s*,\s*', '', st)

    # comma separated, but commas inside "" ignored
    while True:
        match = re.search(_re['bite'], st)
        if match:
            chunk = match.group(1)
            st = match.group(4)
        else:
            chunk = st
            st = None
        if chunk or is_true(options, 'keep_empty'):
            ret.append(chunk)
        if not st:
            break
    return ret

# for strings like mykey: myval or mykey: "myval1 myval2 myval3", return the key
# and list of values (or if option 'merge' is set, a scalar value)
# TODO ignore : if inside "
def keyval_split(st, options=None):
    if options is None:
        options = {}
    if ':' not in st:
        if 'allow_bare_keys' in options:
            key = st.strip(' ')
            val = True
        else:
            raise Exception("can't accept '%s' when allow_bare_keys is False" % st)
    else:
        key, val = _re['colonsep'].split(st, 1)
        val = val.strip('"')
    if 'merge' in options:
        return key, val
    vals = _re['split_white'].split(val)
    return key, vals

# combine comma_split and keyval_split to turn string like
# key1: val1, key2: "val two", ... into a dict of key:opt pairs
# option 'merge' returns opt as scalar instead of list
# option 'allow_bare_keys' allows key with no value, treated as True
def parse_arg_string(st, options=None):
    if options is None:
        options = {'merge': True, 'allow_bare_keys': True}
    ret = {}
    try:
        chunks = comma_split(st)
        for chunk in chunks:
            (key, vals) = keyval_split(chunk, options)
            ret[key] = vals
        return ret
    except Exception as exc:
        logging.error("exception '%s' parsing string '%s'" % (str(exc), st))

def parse_subargs(value, interpret, config, finfo=None):
    if not value.startswith('['):
        logging.warning("parse_subargs: '%s' not a nested arg string" % value)
        return value
    ret = ''
    value = value[1:-1]
    value = debracket(value, interpret, finfo=finfo)
    subargs = parse_arg_string(value)
    if not 'key' in subargs:
        raise Exception("parse_subargs: argument 'key' is required")
    subkey = debracket(subargs['key'], interpret, finfo=finfo)
    if not 'sym' in subargs:
        raise Exception(
            "parse_subargs without 'sym' option not currently supported")
    symbol_set = subargs['sym']
    if not symbol_set in config.symbols:
        raise Exception("invalid 'sym' value '%s'" % symbol_set)
    if subkey in config.symbols[symbol_set]:
        return config.symbols[symbol_set][subkey]
    else:
        # TODO implement multi symbol sets/fallback rules. For now, fall back to
        # using the key_val as the metadata value
        logging.warning("subkey '%s' not in symbol set '%s'" % (subkey, symbol_set))
        return subkey

# split a string based on square brackets (no escaping for literal ones)
# and return array of tokens marked either literal or symbol.
def bracket_parse(st):
    ar = _re['bracketed'].split(st)
    ret = []
    for item in ar:
        if item.startswith('['):
            ret.append({'is_symbol': True, 'value': item[1:]})
        else:
            if item:  # omit empty strings
                ret.append({'is_symbol': False, 'value': item})
    return ret

def fill_brackets(ar, interp, finfo=None, symbols=None, options=None):
    if options is None:
        options = {'allow_verbatim': True}
    ret = ''
    interpret = interpret_method(interp)
    for item in ar:
        val = item['value']
        if item['is_symbol']:
            newval = interpret(val, finfo=finfo, symbols=symbols, options=options)
            # note that empty string is a valid return, but return of None means
            # that the interpret function found no replacement value (in which case
            # the original text is left intact).
            if newval is not None:
                ret += newval
            else:
                if 'allow_verbatim' in options:
                    ret += '[' + val + ']'  # failsafe if square brackets used literally
                else:
                    err = "symbol '%s' not found" % val
                    logging.error(err)
                    raise Exception(err)
        else:
            ret += val
    return ret

def debracket(st, interpret, finfo=None, symbols=None, options=None):
    return fill_brackets(bracket_parse(st), interpret, finfo, symbols, options)

def parens_to_brackets(st):
    return st.replace('(', '[').replace(')', ']')

def equals_to_colons(st):
    return st.replace('=', ':')

def fix_alt_arg(st):
    return parens_to_brackets(equals_to_colons(st))

def get_display_name(finfo, work_item):
    display_name = ''
    got_display_name = False
    if 'item_args' in work_item and 'display_name' in work_item['item_args']:
        display_name = work_item['item_args']['display_name']
        got_display_name = True
    elif 'display_name' in finfo:
        display_name = finfo['display_name']
        got_display_name = True
    return display_name, got_display_name

# like make_rel_path but generic, and not a Python built-in
def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    raise Exception(text + " does not start with " + prefix)

# remove leading string. used to turn abs into relative path
# arg can be filename or directory
# turning strict off returns None instead of throwing on error
def make_rel_path(base, path, strict=True, no_leading_slash=False):
    if not base.startswith('/'):
        raise Exception("make_rel_path base '%s' is not absolute" %base)
    if not path.startswith(base):
        err = "path '%s' does not start with '%s'" % (path, base)
        if not strict:
            return None
        raise Exception(err)
    ret = path[len(base):]
    if not ret:
        return ''
    if no_leading_slash and ret.startswith('/'):
        ret = ret.lstrip('/')
    if ret.endswith('/'):
        ret = ret.rstrip('/')
    return ret

# given a full path, return relative path plus bare name
def full_to_rel_and_name(base, full):
    if not os.path.isfile(full):
        raise Exception("full name '%s' is not a file" % full)
    path, file = os.path.split(full)
    return make_rel_path(base, path), file

# make a path + filename into a suitable dest key (relative
# path without leading slash)
def make_key(path, name):
    key = name
    if path:
        if path.startswith('/'):
            path = path[1:]
        if path.endswith('/'):
            key = path + name
        else:
            key = path + '/' + name
    return key

def full_to_key(base, full):
    rel, name = full_to_rel_and_name(base, full)
    return make_key(rel, name)

# given a full filename which is under one root, return the equivalent full filename
# under a different root
def reroot_file(full_src, src_path, dest_path):
    rel = make_rel_path(src_path, full_src, strict=False)
    if not rel:
        raise Exception("'%s' is not a subpath of '%s'" % (src_path, full_src))
    return pathify(dest_path, rel)

# compare two dictionaries - see
# https://stackoverflow.com/questions/4527942/comparing-two-dictionaries-in-python
def dict_compare(d1, d2):
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    intersect_keys = d1_keys.intersection(d2_keys)
    added = d1_keys - d2_keys
    removed = d2_keys - d1_keys
    modified = {o : (d1[o], d2[o]) for o in intersect_keys if d1[o] != d2[o]}
    same = set(o for o in intersect_keys if d1[o] == d2[o])
    return added, removed, modified, same

# return unix timestamp of most recent file modification in dir
def dir_last_modified(path):
    ret = 0
    for dir_name, subdirs, files in os.walk(path):
        for file_name in files:
            full = dir_name + '/' + file_name
            modified = os.path.getmtime(full)
            ret = max(ret, modified)
    return ret

#---------------------------------------------------------------------------
# have to go to insane lengths to use json with datetime
# see https://gist.github.com/abhinav-upadhyay/5300137; added timezone awareness

class DateTimeDecoder(json.JSONDecoder):
    def __init__(self, *args, **kargs):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object,
                             *args, **kargs)

    def dict_to_object(self, d):
        if '__type__' not in d:
            return d

        type = d.pop('__type__')
        try:
            dateobj = datetime(**d)
            # TODO verify working!
            tzone = datetime.timezone(d['tzinfo'])
            loc_dt = tzone.localize(dateobj)
            return loc_dt
        except:
            d['__type__'] = type
            return d


class DateTimeEncoder(json.JSONEncoder):
    """ Instead of letting the default encoder convert datetime to string,
        convert datetime objects into a dict, which can be decoded by the
        DateTimeDecoder
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return {
                '__type__': 'datetime',
                'year': obj.year,
                'month': obj.month,
                'day': obj.day,
                'hour': obj.hour,
                'minute': obj.minute,
                'second': obj.second,
                'microsecond': obj.microsecond,
                'tzinfo': str(obj.tzinfo)
            }
        else:
            return json.JSONEncoder.default(self, obj)

# convert utc datetime to local see:
# https://stackoverflow.com/questions/4563272/convert-a-python-utc-datetime-to-a-local-datetime-using-only-python-standard-lib
def utc_to_local(utc_dt):
    # get integer timestamp to avoid precision lost
    timestamp = calendar.timegm(utc_dt.timetuple())
    local_dt = datetime.fromtimestamp(timestamp)
    assert utc_dt.resolution >= timedelta(microseconds=1)
    return local_dt.replace(microsecond=utc_dt.microsecond)

def timestamp_now():
    return time.time() # float seconds

# convert datetime, or YYYY-MM-DD string, to day of unix epoch
def datetime_to_daynum(when, options=None):
    if options is None:
        options = {}
    if 'std_string' in options:
        date_time = datetime.strptime(when, '%Y-%m-%d')
    else:
        date_time = when
    seconds = date_time.strftime("%s")  # seconds since epoch
    return math.ceil(float(seconds) / 86400)  # days since epoch

def daynum_now():
    return datetime_to_daynum(datetime.utcnow())

def datestr_to_daynum(when):
    return datetime_to_daynum(when, options={'std_string': True})

def timestamp_to_datestr(seconds):
    dt = datetime.fromtimestamp(seconds)
    return dt.strftime('%Y-%m-%d')

# convert day of epoch to date in YYYY-MM-DD format
def daynum_to_datestr(daynum):
    seconds = daynum * 86400
    return timestamp_to_datestr(seconds)

# get year, month and day strings
def daynum_to_ymd(daynum):
    datestr = daynum_to_datestr(daynum)
    match = _re['std_date_str'].match(datestr)
    if not match:
        raise Exception("can't parse datestr '%s'" % datestr)
    return match.group(1), match.group(2), match.group(3)

# unpack a compressed archive. files_wanted should be a dict whose keys are
# pathed file names wanted (if omitted, keep all). Or pass a regex, matching
# pathed file names will be extracted.
def unpack_file(src_path, dest_path, file_name, files_wanted=None, regex_wanted=None):
    # this should be a .tar.gz file, make dest folder that is the
    # same name minus .tar.gz (TODO adapt to handle zips as well)
    if not file_name.endswith('.tar.gz'):
        err = "can't unpack - doesn't end with .tar.gz"
        logging.error(err)
        raise Exception(err)
    try:
        if not dest_path.endswith('/'):
            dest_path += '/'
        dest = dest_path + file_name[:-7]
        ensure_path(dest)
        tar = tarfile.open(src_path + '/' + file_name)
        members = tar.getmembers()
        if files_wanted:
            # this syntax avoids making a copy of the list
            members[:] = [x for x in members if x.name in files_wanted]
        elif regex_wanted:
            regex = re.compile(regex_wanted)
            members[:] = [x for x in members if re.search(regex, x.name)]
        tar.extractall(path=dest, members=members)
        tar.close()
    except Exception as exc:
        msg = "unpack of %s/%s failed with exc '%s'" % (src_path, file_name, str(exc))
        logging.error(msg)
        return None
    return dest

# given a regex with a single group, search search_path for .tar.gz files
# matching that regex, extract the group, and extract the files_wanted
def extract_time_series(search_path, archive_re, dest_path, regex_wanted):
    regex = re.compile(archive_re)
    for dir_name, subdirs, files in os.walk(search_path):
        for file_name in files:
            if not file_name.endswith('.tar.gz'):
                continue
            full = dir_name + '/' + file_name
            match = re.search(regex, full)
            if not match:
                continue
            tag = match.group(1)
            unpack_file(dir_name, dest_path, file_name, regex_wanted=regex_wanted)

# given file foo/bar.ext, move it to dest_dir/bar.ext.N where N is
# the smallest integer that avoids a collision
def age_file(full, dest_dir, move=False):
    if not os.path.exists(full):
        return
    src_dir, src_file = os.path.split(full)
    if not dest_dir.endswith('/'):
        dest_dir += '/'
    dest = ''
    dest_base = dest_dir + src_file
    suffix = ''
    dot = ''
    maxfiles = 500
    while True:
        dest = dest_base + dot + str(suffix)
        if not os.path.exists(dest):
            break
        if suffix == '':
            suffix = 0
            dot = '.'
        else:
            suffix += 1
            if suffix > maxfiles:
                break
    if suffix != '' and suffix > maxfiles:
        err = "can't age file '%s', too many files in '%s'" % (full, dest_dir)
        logging.error(err)
        raise Exception(err)
    if move:
        os.rename(full, dest)
    else:
        shutil.copy(full, dest)

# search a set of root paths for a file, return root path where found
# set option 'full' to return full filename, not just root
def find_in_paths(file_name, rel_path, roots, options=None):
    if options is None:
        options = {}
    for root in roots:
        full = root + '/' + rel_path + '/' + file_name
        if os.path.isfile(full):
            if 'full' in options and options['full']:
                return full
            return root
    return None

def have_required(coll, *args):
    for arg in args:
        if arg not in coll or not coll[arg]:
            return False
    return True

# given list of lines, if any line ends with comma, concat next line to it.
def join_lines_comma(lines):
    ret = []
    for line in lines:
        if ret and ret[-1].rstrip().endswith(','):
            ret[-1] = ret[-1] + line
        else:
            ret.append(line)
    return ret

_reInclude = re.compile(r'^\s*<<include\s+(.*)$')
# read a file, look for lines consisting of "<<include filespec", replace
# them with contents of file mentioned, write to temp file and return its path
def process_includes(src):
    lines = open(src).read().splitlines()
    new_lines = []
    ret = tempfile.NamedTemporaryFile(mode='r+') # read and write
    for line in lines:
        match = re.search(_reInclude, line)
        if match:
            try:
                new_lines.extend(open(match.group(1)).read().splitlines())
            except Exception as exc:
                msg = "error reading included file '%s' requested in '%s': %s" % (
                    match.group(1), src, str(exc))
                raise Exception(msg)
        else:
            new_lines.append(line)
    ret.write('\n'.join(new_lines))
    ret.seek(0) # ready to read from BOF
    return ret

# rsync one tree with another. TODO: currently assumes local (no-compress);
# also doesn't delete from dest. add options
def deploy_tree(src, dest, options=None):
    if options is None:
        options = {}
    if not os.path.isdir(src) or not os.path.isdir(dest):
        raise Exception("bad arg src ('%s') or dest ('%s')" % (src, dest))
    if not src.endswith('/'):
        src = src + '/'
    # see: https://serverfault.com/questions/43014/copying-a-large-directory-tree-locally-cp-or-rsync
    flags = []
    if 'verbose' in options:
        flags.append('-avhW')
    else:
        flags.append('-ahW')
    if 'delete' in options:
        flags.append('--delete')
    flags.append('--no-compress')
    args = ['rsync'] + flags + [src, dest]
    (returncode, stdout, stderr) = run_command(args)
    if returncode == 0:
        if 'verbose' in options:
            logging.info(stdout)
        return
    raise Exception("rsync failed with rc '%i', stderr = '%s'" % (returncode, stderr))

def file_search_replace(full, search_regex, replacement, options=None):
    if options is None:
        options = {}
    if 'encoding' not in options:
        options['encoding'] = None
    with open(full, mode='r+', encoding=options['encoding']) as fh:
        contents = fh.read()
        if re.search(search_regex, contents):
            modified = search_regex.sub(replacement, contents)
            fh.seek(0)
            fh.write(modified)
            # without this, if file got shorter it will have old data at the end:
            fh.truncate()
            return True
        return False

# perform a global regex search & replace on all files in a folder.
# optional file_filter is a function that is passed each file name, and returns True
# if the file should be opened (in text mode) and searched.
# TODO - could extend to accept N search/replace ops and maybe gain some efficiency.
def folder_search_replace(root, search_expr, replacement, file_filter=None, options=None):
    if options is None:
        options = {}
    if 'encoding' not in options:
        options['encoding'] = None
    n_seen = 0
    n_skipped = 0
    n_unchanged = 0
    n_changed = 0
    regex = re.compile(search_expr)
    for dir_name, subdirs, files in os.walk(root):
        for file_name in files:
            full = dir_name + '/' + file_name
            n_seen += 1
            if file_filter and not file_filter(full):
                n_skipped += 1
                continue
            if file_search_replace(full, regex, replacement, options):
                n_changed += 1
            else:
                n_unchanged += 1
    return n_seen, n_skipped, n_unchanged, n_changed

def force_sign(x):
    if not x.startswith('+') and not x.startswith('-'):
        return '+' + x
    return x

def fetch_page(url):
    try:
        response = urllib.request.urlopen(url)
        meta = response.info()
        content_type = meta.get('content-type')
        return response.status, response.length, response.msg, response.read(), content_type, meta
    except Exception as exc:
        logging.error("exception '%s' fetching url '%s'" % (str(exc), url))
        return 404, 0, '', '', '', {}

def time_page(url):
    curl_fmt = """
{
"speed_download": %{speed_download},\n
"size_download": %{size_download},\n
"time_namelookup":  %{time_namelookup},\n
"time_connect":  %{time_connect},\n
"time_appconnect":  %{time_appconnect},\n
"time_pretransfer":  %{time_pretransfer},\n
"time_redirect":  %{time_redirect},\n
"time_starttransfer":  %{time_starttransfer},\n
"time_total":  %{time_total}\n
}\n"""
    try:
        call_args = ['curl', '-w', curl_fmt, '-o', '/dev/null', '-s', url]
        (returncode, stdout, stderr) = run_command(call_args)
        if returncode == 0:
            return json.loads(stdout.decode('utf-8'))
        else:
            raise Exception("rc %i, stderr = '%s'" % (returncode, stderr))
    except Exception as exc:
        logging.error("time_page exception '%s' from url '%s'" % (str(exc), url))
        return {}
