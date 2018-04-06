__author__ = 'dsteinkraus'

import os
import re
import logging
import json
import shutil
from urllib.parse import urlparse
import time

from config import Config
import util as u

#===================== class FTP =========================================================

class UrlMgr(object):
    def __init__(self, config, url_patterns):
        self.config = config
        u.ensure_path(config.input)
        self._list_file_name = '_url_list.txt'
        # TODO should be able to control dir for persist files (e.g. for git convenience)
        self._list_file = self.config.admin + '/' + self._list_file_name
        self._download_count = 0
        self._download_limit = self.config.get_int('input', 'url_download_limit', default=0)
        self._raw_url_patterns = url_patterns
        self._url_patterns = {}
        self._daynum_today = u.daynum_now()
        # metadata for downloaded files (key is product_tag/file_name):
        self._local_files = {}
        self._re = {}
        self._re['tag'] = re.compile(r'^[a-zA-Z0-9_]+')

        self._parse_url_patterns()

        # this is used to store/persist metadata about directly downloaded files.
        self._local_files = {}
        # this is populated by TreeProcessor
        self.rules_run_files = []

    # allow url patterns to be multi-line by recombining here
    def _assemble_url_patterns(self):
        options = {'keep_empty': True}
        ret = []
        st = ''
        for chunk in self._raw_url_patterns:
            st += chunk
            if not chunk.endswith(','):
                ret.append(st)
                st = ''
        return [u.comma_split(x, options) for x in ret]

    def _parse_url_patterns(self):
        for ar in self._assemble_url_patterns():
            tag, pat, dest, start, idle, mode = ar
            if tag in self._url_patterns:
                msg = "ignoring duplicate url tag '%s'" % tag
                Config.log(msg, tag='URL_DUPLICATE')
                continue
            if not self._re['tag'].match(tag):
                Config.log(tag, tag='URL_INVALID_TAG')
                raise Exception('URL_INVALID_TAG')
            if not self._valid_pattern(pat):
                Config.log(pat, tag='URL_INVALID_PATTERN')
                raise Exception('URL_INVALID_PATTERN')
            daynum = None
            try:
                daynum = u.datestr_to_daynum(start)
            except Exception as exc:
                Config.log(start, tag='URL_INVALID_START')
                raise
            try:
                int(idle) # TODO check for nonneg and not too huge
            except ValueError:
                Config.log(idle, tag='URL_INVALID_IDLE')
                raise
            if mode != 'snap' and mode != 'fetch':
                Config.log(mode, tag='URL_INVALID_MODE')
                raise Exception('URL_INVALID_MODE')
            # this is the base path under which all files for this pattern are stored -
            # in this dir if no dest, but if dest is nonempty they may go in a subdir
            path = os.path.join(self.config.input, tag)
            u.ensure_path(path)
            self._url_patterns[tag] = {
                'tag': tag,
                'pattern': pat,
                'start': daynum,
                'dest': dest,
                'current_daynum': daynum,
                'idle': int(idle),
                'mode': mode,
                'path': path
            }

    # todo: specialize to check both url and dest patterns more exactly
    def _valid_pattern(self, pat):
        chunks = u.bracket_parse(pat)
        errs = ''
        for item in chunks:
            if item['is_symbol']:
                val = item['value']
                if val == 'YYYY': continue
                if val == 'MM': continue
                if val == 'DD': continue
                # TODO more
                errs += "invalid symbol '%s'\n" % val
        if errs:
            Config.log(errs, tag='URL_INVALID_PATTERN_SYMBOL')
            return False
        # TODO validate URL after replacing bracket exprs with innocuous chars
        return True

    def interpret(self, val, finfo=None, symbols=None, options=None):
        if val in symbols:
            return symbols[val]
        raise Exception("can't interpret '%s'" % val)

    # these methods return both a url to fetch, and a filename to save to
    def _get_first_url_and_file(self, settings):
        settings['current_daynum'] = settings['start']
        return self._get_cur_url_and_file(settings)

    def _get_cur_url_and_file(self, settings):
        if settings['current_daynum'] > self._daynum_today:
            return None, None
        yyyy, mm, dd = u.daynum_to_ymd(settings['current_daynum'])
        symbols = {'YYYY': yyyy, 'MM': mm, 'DD': dd}
        options = {'allow_verbatim': False}
        url = u.debracket(settings['pattern'], self.interpret, symbols=symbols, options=options)
        if settings['dest']:
            fname = u.debracket(settings['dest'], self.interpret, symbols=symbols, options=options)
        else:
            parsed = urlparse(url)
            fname = os.path.basename(parsed.path)
        return url, fname

    def _get_next_url_and_file(self, settings):
        settings['current_daynum'] += 1
        return self._get_cur_url_and_file(settings)

    def update_products(self):
        Config.log("", tag='URL_UPDATE_START')

        # remove or modify persisted file metadata if full path matches passed regex
        if self.config.do('ftp_remove') or self.config.do('ftp_rerun'):
            regex = re.compile(self.config.special_mode_args[1])

            def test(fi):
                return re.search(regex, fi['full'])

            self.read_metadata(test=test, action=self.config.special_mode_args[0])
            self.write_metadata()
            return 0

        self.read_metadata()
        self._download_count = 0
        # first, find first "missing" file in metadata
        for tag, settings in self._url_patterns.items():
            url, file_name = self._get_first_url_and_file(settings)
            while url:
                # avoid the name 'key' as that is reserved for dest mgr purposes
                file_key = os.path.join(settings['tag'], file_name)
                if file_key in self._local_files:
                    url, file_name = self._get_next_url_and_file(settings)
                    continue
                break # settings['current_daynum'] is now set to correct value

        # now download files round-robin until current date
        hit_limit = False
        for tag, settings in self._url_patterns.items():
            if hit_limit:
                break
            url, file_name = self._get_cur_url_and_file(settings)
            while url:
                file_key = os.path.join(settings['tag'], file_name)
                if file_key in self._local_files:
                    url, file_name = self._get_next_url_and_file(settings)
                    continue
                full = os.path.join(settings['path'], file_name)
                u.ensure_path_for_file(full)
                if settings['mode'] == 'fetch':
                    try:
                        status, length, message, content, content_type, meta = u.fetch_page(url)
                        if status == 200:
                            with open(full, 'wb') as fh:
                                fh.write(content)
                            self._local_files[file_key] = {
                                'file_key': file_key,
                                'full': full,
                                'file_name': file_name,
                                'size': length,
                                'url': url,
                                'content-type': content_type
                            }
                            self._download_count += 1
                            Config.log(url, tag='URL_DOWNLOADED_FILE')
                            time.sleep(2)  # TODO use idle, but don't busy wait
                        else:
                            msg = "status '%i' fetching url '%s'" % (status, url)
                            Config.log(msg, tag='URL_FETCH_ERROR')
                    except Exception as exc:
                        msg = "exception '%s' fetching url '%s'" % (str(exc), url)
                        Config.log(msg, tag='URL_FETCH_ERROR')
                        raise
                elif settings['mode'] == 'snap':
                    raise Exception('nyi')
                    self._download_count += 1
                else:
                    raise Exception("unknown mode '%s'" % settings['mode'])
                if self._download_limit > 0 and self._download_count >= self._download_limit:
                    msg = "downloaded limit of %i files, ending download phase" % self._download_count
                    self.config.add_to_final_summary(msg)
                    Config.log(msg, tag='URL_DOWNLOAD_LIMIT')
                    hit_limit = True
                    break
                url, file_name = self._get_next_url_and_file(settings)
            if not url:
                msg = "product %s is caught up to today." % settings['tag']
                Config.log(msg, tag='URL_DOWNLOAD_CAUGHT_UP')
        self.write_metadata()
        return self._download_count

    # test is a fn that returns bool based on finfo
    def read_metadata(self, test=None, action=None):
        self._local_files.clear()
        try:
            with open(self._list_file) as fh:
                for line in fh:
                    line = line.rstrip()
                    finfo = json.loads(line)
                    if test:
                        if test(finfo):
                            Config.log(finfo['full'], tag='URL_' + action)
                            if action == 'ftp_remove':
                                continue # remove whole finfo by not loading it
                            if action == 'ftp_rerun':
                                if 'rules_run' in finfo:
                                    del(finfo['rules_run'])
                        self._local_files[finfo['name']] = finfo
                    else:
                        self._local_files[finfo['file_key']] = finfo
        except Exception as exc:
            msg = "file '%s' not found/openable: '%s'" % (self._list_file, str(exc))
            Config.log(msg, tag='URL_INVALID_METADATA')

    def write_metadata(self):
        try:
            shutil.copyfile(self._list_file, self._list_file + '.bak')
        except Exception as exc:
            msg = "ignoring exception '%s' making metadata backup" % str(exc)
            Config.log(msg, tag='URL_INVALID_METADATA')
        with open(self._list_file, 'w') as fh:
            for fname, finfo in self._local_files.items():
                fh.write(json.dumps(finfo, cls=u.DateTimeEncoder) + '\n')

    # fix up loaded metadata (assumed current) by marking files with
    # 'rules_run', and write out to disk.
    def update_rules_run_files(self):
        if len(self.rules_run_files):
            for full in self.rules_run_files:
                file_key = self.full_to_file_key(full)
                if file_key not in self._local_files:
                    err = "BUG: file '%s' is not in metadata" % full
                    Config.log(err, tag='URL_INVALID_RULES_RUN_FILE')
                    continue
                finfo = self._local_files[file_key]
                if 'rules_run' not in finfo or not finfo['rules_run']:
                    finfo['rules_run'] = True
            del self.rules_run_files[:]
        # have to do this every time, metadata may have been out of date already
        self.write_metadata()

    # return a deep copy of the requested finfo (so caller doesn't mess up ours)
    # TODO above comment is false!
    # return none if no such root-level file
    def get_root_finfo_copy(self, full):
        path, filename = os.path.split(full)
        file_key = self.full_to_file_key(full)
        if not file_key:
            return None  # not a file we downloaded
        for tag, settings in self._url_patterns.items():
            # we are only looking for root-level files, not unpacked files
            if path == settings['path']:
                if file_key in self._local_files:
                    return self._local_files[file_key]
                msg = "possible logic error, full file '%s' in root but not in metadata" % full
                Config.log(msg, tag='URL_ROOT_FILE_NOT_IN_META')
                return None
        return None

    def full_to_file_key(self, full):
        path, filename = os.path.split(full)
        p = u.make_rel_path(self.config.input, path, strict=False, no_leading_slash=True)
        if not p:
            return None
        return os.path.join(p, filename)

    def update_folder(self):
        # TODO - refactor, use name that makes sense for all IM types
        # TODO also refactor phase name 'ftp'
        return self.update_products()