__author__ = 'dsteinkraus'

import ftplib
import os
import re
import logging
import copy
import json
from datetime import datetime
import shutil

from config import Config
import util as u

#===================== class FtpMgr =========================================================

class FtpMgr(object):
    def __init__(self, config, label, site, login, password, folder, dest_root):
        self.config = config
        self._label = label
        self._site = site
        self._login = login
        self._password = password
        self._folder = folder
        self._dest_root = dest_root
        u.ensure_path(self._dest_root)
        self._ftp = None
        self._splt = re.compile('\s+')
        self._list_file_name = '_ftp_dirlist.txt'
        self._dir_list = self.config.admin + '/' + self._label + self._list_file_name
        self._download_count = 0
        self._download_limit = self.config.get_int('input', 'download_limit', default=0)

        # this is used to store/persist metadata about directly downloaded files.
        # key has no path.
        self._local_files = {}
        # this is populated by TreeProcessor
        self.rules_run_files = []

        self._re_include = []
        self._re_exclude = []
        re_include_files = self.config.get_multi('input', 'include_files')
        for str in re_include_files:
            self._re_include.append(re.compile(str))
        re_exclude_files = self.config.get_multi('input', 'exclude_files')
        for str in re_exclude_files:
            self._re_exclude.append(re.compile(str))

    # return count of files downloaded
    def update_folder(self):
        logging.info("starting FTP update from %s" % self._label)
        start = u.timestamp_now()
        self._download_count = 0
        self._ftp = ftplib.FTP(self._site, self._login, self._password)
        self._ftp.cwd(self._folder)

        # catchup mode means get the file names and metadata for the FTP folder,
        # and write it to _ftp_dirlist.txt, but don't copy any files. then, on the
        # next normal run, there will be no work to do unless you modify _ftp_dirlist.txt.
        # this allows forcing only specific files to be downloaded.
        if self.config.do('ftp_catchup'):
            cur_ftp_files = []
            self._local_files.clear()
            self._ftp.retrlines('LIST', cur_ftp_files.append)
            for entry in cur_ftp_files:
                finfo = u.ftp_metadata(entry)
                if finfo['isdir']:
                    continue
                finfo['modified'] = self.ftp_modified(finfo['name'])
                finfo['path'] = self._dest_root
                finfo['full'] = self._dest_root + '/' + finfo['name']
                self._local_files[finfo['name']] = finfo
            self.write_metadata()
            return 0

        # remove or modify persisted file metadata if full path matches passed regex
        if self.config.do('ftp_remove') or self.config.do('ftp_rerun'):
            regex = re.compile(self.config.special_mode_args[1])

            def test(fi):
                return re.search(regex, fi['full'])

            self.read_metadata(test=test, action=self.config.special_mode_args[0])
            self.write_metadata()
            return 0

        # build metadata file from what's in input dir
        if self.config.do('ftp_meta_from_local'):
            self.metadata_from_local(clear_first=True)
            return 0

        # normal operation:
        self.read_metadata()
        self.metadata_from_local(clear_first=False)
        cur_ftp_files = []
        self._ftp.retrlines('LIST', cur_ftp_files.append)
        for entry in cur_ftp_files:
            if self.config.signalled():
                logging.info("signal set, leaving ftp.update_folder")
                break
            finfo = u.ftp_metadata(entry)
            file_name = finfo['name']
            if finfo['isdir']:
                continue
            # test include/exclude rules
            found = False
            for reg in self._re_include:
                if re.search(reg, file_name):
                    found = True
                    break
            if not found:
                Config.log("skipping file '%s'" % file_name, tag='FTP_INCLUDE_FILES')
                continue
            found = False
            for reg in self._re_exclude:
                if re.search(reg, file_name):
                    found = True
                    break
            if found:
                Config.log("skipping file '%s'" % file_name, tag='FTP_EXCLUDE_FILES')
                continue
            finfo['modified'] = self.ftp_modified(finfo['name'])
            local_full = self._dest_root + '/' + file_name
            grabit = True
            if file_name in self._local_files:
                local_finfo = self._local_files[file_name]
                # download if ftp version is newer than our last download
                localmod = local_finfo['modified']
                remotemod = finfo['modified']
                grabit = remotemod > localmod
            if grabit:
                logging.info("grabbing new/changed file %s" % file_name)
                fh = open(local_full, "wb")
                self._ftp.retrbinary('RETR ' + file_name, fh.write)
                fh.close()
                finfo['path'] = self._dest_root
                finfo['full'] = local_full
                self._local_files[file_name] = finfo
                self._download_count += 1
                msg = "FTP downloaded file %i of limit %i" % (self._download_count, self._download_limit)
                logging.info(msg)
                if self._download_limit > 0 and self._download_count >= self._download_limit:
                    msg = "downloaded limit of %i files, ending download phase" % self._download_count
                    self.config.add_to_final_summary(msg)
                    logging.warning(msg)
                    break
        self._ftp.quit()
        self._ftp = None
        self.write_metadata()
        elapsed = u.timestamp_now() - start
        logging.info("FTP.update_folder finished in %f seconds, downloaded %i files"
                     % (elapsed, self._download_count))
        return self._download_count

    def ftp_modified(self, file_name):
        resp = self._ftp.sendcmd('MDTM '+ file_name).split()
        # resp = self._ftp.sendcmd('MLST '+ file_name)
        if resp[0] != '213':
            raise Exception("invalid MDTM response '%s'" % resp[0])
        dt = datetime.strptime(resp[1], '%Y%m%d%H%M%S')
        timestamp = (dt - datetime(1970, 1, 1)).total_seconds()
        return int(timestamp)

    # test is a fn that returns bool based on finfo
    def read_metadata(self, test=None, action=None):
        self._local_files.clear()
        try:
            with open(self._dir_list) as fh:
                for line in fh:
                    line = line.rstrip()
                    finfo = json.loads(line)
                    if test:
                        if test(finfo):
                            logging.debug("apply action '%s' to '%s'" % (action, finfo['full']))
                            if action == 'ftp_remove':
                                continue # remove whole finfo by not loading it
                            if action == 'ftp_rerun':
                                if 'rules_run' in finfo:
                                    del(finfo['rules_run'])
                        self._local_files[finfo['name']] = finfo
                    else:
                        self._local_files[finfo['name']] = finfo  # quiet
        except Exception as exc:
            logging.info("file '%s' not found/openable: '%s'" % (self._dir_list, str(exc)))

    def write_metadata(self):
        try:
            shutil.copyfile(self._dir_list, self._dir_list + '.bak')
        except Exception as exc:
            logging.warning("ignoring exception '%s' writing ftp metadata" % str(exc))
        with open(self._dir_list, 'w') as fh:
            for fname, finfo in self._local_files.items():
                fh.write(json.dumps(finfo, cls=u.DateTimeEncoder) + '\n')

    # update existing, or build a new metadata file from contents of input folder
    def metadata_from_local(self, clear_first=False):
        if clear_first:
            self._local_files.clear()
        # ftp does not look at subfolders
        for file_name in u.plain_files(self._dest_root, '.'):
            if file_name in self._local_files:
                continue    # no overwrite
            finfo = u.local_metadata(self._dest_root, file_name)
            self._local_files[finfo['name']] = finfo
        self.write_metadata()

    # fix up loaded metadata (assumed current) by marking files with
    # 'rules_run', and write out to disk.
    def update_rules_run_files(self):
        if len(self.rules_run_files):
            for full in self.rules_run_files:
                path, filename = os.path.split(full)
                if filename not in self._local_files:
                    err = "BUG: file '%s' is not in _local_files" % full
                    Config.log(err, tag='FTP_INVALID_RULES_RUN_FILE')
                    continue
                finfo = self._local_files[filename]
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
        if path == self._dest_root:
            if filename in self._local_files:
                return self._local_files[filename]
            raise Exception("possible logic error, full file '%s' in root but not in metadata" % full)
        return None
