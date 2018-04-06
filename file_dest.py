__author__ = 'dsteinkraus'

import os
import io
import logging
import re
import copy
import json
import shutil
import mimetypes

from config import Config
import util as u

#===================== class FileDest =========================================================

class FileDest(object):
    def __init__(self, config, config_section):
        self.config = config
        if not config_section:
            raise Exception('FileDest: config_section required')
        self.config_section = config_section
        self._file_dest_root = self.config.get(config_section, 'file_dest_root')
        self.tree_info = {}
        self._tree_last_modified = 1e99
        # TODO REVIEW - config_section is not sufficient uniquifier, need to verify
        # no other FileDest instance is using same uniquifier. (still true after postgres btw)
        # file_dest_root had also better be unique. Check both dynamically with a class variable.
        self._tree_info_file = config.admin + '/_' + config_section + '_file_dest_tree_info.txt'
        self._synced_tree_info = False
        self._max_upload_size = 1024 * 1024 * self.config.get_int(config_section, 'max_upload_size', default=-1)
        self._upload_count = 0
        self.local_root = config.output
        self._default_sync_options = {}
        if self.config.is_true(self.config_section, 'refresh_dest_meta', absent_means_no=True):
            self._default_sync_options['refresh_dest_meta'] = True
        if config.is_true(self.config_section, 'skip_refresh_if_tree_unchanged', absent_means_no=True):
            self._default_sync_options['skip_refresh_if_tree_unchanged'] = True

    # upload a file with bare name srcName, in folder srcPath, to the destination.
    # this internal method does not deal with tree metadata
    def _upload(self, src_path, src_name, key, bucket=None, content_type=None, extra_args=None):
        full = u.pathify(src_path, src_name)
        if extra_args is None:
            extra_args = {
                'Metadata': {
                    'md5': u.md5(full)
                }
            }
        if not content_type:
            content_type = mimetypes.MimeTypes().guess_type(src_name)[0]
            extra_args['ContentType'] = content_type
        dest_full = self._file_dest_root + '/' + key
        dest_dir, dest_name = os.path.split(dest_full)
        u.ensure_path(dest_dir)
        shutil.copyfile(full, dest_full)

    # use this to upload a file AND update local metadata
    def upload_finfo(self, finfo, content_type=None, extra_args=None):
        key = finfo['key']
        rel_path, _ = os.path.split(key)
        if extra_args is None:
            extra_args = {
                'Metadata': {
                    'md5': finfo['md5']
                }
            }
        if not content_type:
            content_type = mimetypes.MimeTypes().guess_type(finfo['name'])[0]
            extra_args['ContentType'] = content_type
        dest_full = self._file_dest_root + '/' + key
        dest_dir, dest_name = os.path.split(dest_full)
        info = {
            'new': True,
            'name': finfo['name'],
            'rel_path': rel_path,
            'key': key,
            'size': finfo['size'],
            'modified': finfo['modified'],
            'md5': finfo['md5']
        }
        # transfer other metadata
        self.transfer_metadata(finfo, local_root=self.local_root, dest=info)
        self.tree_info[key] = info
        u.ensure_path(dest_dir)
        shutil.copyfile(finfo['full'], dest_full)

    # upload some bytes as a new object in bucket
    def upload_obj(self, key, objBytes, contentType, bucket=None):
        file_like = io.BytesIO(objBytes)
        raise Exception('todo')

    def transfer_metadata(self, finfo, local_root, dest):
        # TODO this can't be hard-coded for thumbs! generalize
        # transfer thumbnail info if exists
        if 'parent_full' in finfo:
            parent_full_path, parent_name = os.path.split(finfo['parent_full'])
            rel_path_parent = u.make_rel_path(local_root, parent_full_path, strict=True)
            dest['parent_key'] = u.make_key(rel_path_parent, parent_name)
        if 'thumb_full' in finfo:
            thumb_full_path, thumb_name = os.path.split(finfo['thumb_full'])
            rel_path_thumb = u.make_rel_path(local_root, thumb_full_path, strict=True)
            dest['thumb_key'] = u.make_key(rel_path_thumb, thumb_name)
        # copy other useful metadata
        if 'width' in finfo and 'height' in finfo:
            dest['width'] = finfo['width']
            dest['height'] = finfo['height']

    # return count of files uploaded
    # convention is that anything in the /tmp tree does not get uploaded
    def upload_tree(self, local_root, remote_root='', options=None, local_tree_meta=None):
        logging.info("starting FileDest upload")
        if not options:
            options = {'use_md5': True}
        start = u.timestamp_now()
        self._upload_count = 0
        # refresh and save data for files already on dest
        self.sync_tree_info(options=self._default_sync_options)

        for dir_name, subdirs, files in os.walk(local_root):
            rel_path = u.make_rel_path(local_root, dir_name)
            if not rel_path.startswith('/tmp'):
                for file_name in files:
                    local_file = dir_name + '/' + file_name
                    key = u.make_key(rel_path, file_name)
                    local_md5 = u.md5(local_file)
                    local_meta = None
                    if local_tree_meta and local_file in local_tree_meta:
                        local_meta = local_tree_meta[local_file]
                    else:
                        local_meta = u.local_metadata(dir_name, file_name)
                    size = local_meta['size']
                    cached_info = None
                    if key in self.tree_info:
                        cached_info = self.tree_info[key]
                    do_upload = True
                    if 'use_md5' in options:
                        if cached_info and not self.is_pending(key):
                            if 'md5' in self.tree_info[key]:
                                remote_md5 = self.tree_info[key]['md5']
                                do_upload = do_upload and remote_md5 != local_md5
                            else:
                                err = "no md5 value for existing key '%s' (old version?)" % key
                                logging.error(err)
                        else:
                            Config.log("file '%s' is not in FileDest" % key, tag='DEST_NO_EXISTING_FILE')
                        if self._max_upload_size >= 0 and size > self._max_upload_size:
                            logging.debug("file '%s' size (%i) > limit (%i), won't upload" %
                                          (key, size, self._max_upload_size))
                            do_upload = False
                    if do_upload:
                        extra_args = {
                            'Metadata': {'md5': local_md5}
                        }
                        logging.debug("FileDest object upload starting, key = '%s', %i bytes" %
                                      (key, size))
                        start = u.timestamp_now()
                        self._upload(dir_name, file_name, key, extra_args=extra_args)
                        rate = size / (u.timestamp_now() - start)
                        Config.log("key = '%s', %f bytes/sec" % (key, rate), tag='FILE_DEST_UPLOAD_OK')
                        # add metadata to our repos
                        info = {
                            'new': True,
                            'name': file_name,
                            'rel_path': rel_path,
                            'key': key,
                            'size': local_meta['size'],
                            'modified': local_meta['modified'],
                            # 'mod_dt': last_mod,
                            # 'e_tag': obj.e_tag,
                            'md5': local_md5
                        }
                        # transfer meta (e.g. thumbnail info) if exists - TODO review
                        if local_tree_meta and local_file in local_tree_meta:
                            self.transfer_metadata(local_tree_meta[local_file], local_root=self.local_root, dest=info)

                        self.tree_info[key] = info
                        self._upload_count += 1
                    else:
                        Config.log("key = '%s'" % key, tag='FILE_DEST_UPLOAD_NO_CHANGE')
        self.write_tree_info()
        elapsed = u.timestamp_now() - start
        logging.info("FileDest.upload_tree finished in %f seconds, uploaded %i files" %
                     (elapsed, self._upload_count))
        return self._upload_count

    # walk the FileDest tree and get/store metadata for all objects
    def get_tree_info(self, bucket=None, remote_root=''):
            self.tree_info.clear()
            for dir_name, subdirs, files in os.walk(self._file_dest_root):
                for file_name in files:
                    full_src = dir_name + '/' + file_name
                    rel_path = u.make_rel_path(self._file_dest_root, dir_name, strict=False, no_leading_slash=True)
                    local_meta = u.local_metadata(dir_name, file_name)
                    local_meta['key'] = u.make_key(rel_path, file_name)
                    local_meta['md5'] = u.md5(full_src)

                    # important: must never expose 'full' outside this class - it's a private
                    # implementation detail. Same for 'path'. Only 'key' is public
                    del local_meta['full']
                    del local_meta['path']

                    self.tree_info[local_meta['key']] = local_meta

    # return iterator for current tree_info
    def tree_info_items(self):
        self.sync_tree_info()
        return self.tree_info.items()

    def write_tree_info(self):
        try:
            shutil.copyfile(self._tree_info_file, self._tree_info_file + '.bak')
        except Exception as exc:
            logging.warning("ignoring exception '%s' persisting FileDest tree info" % str(exc))
        try:
            self._tree_last_modified = u.dir_last_modified(self._file_dest_root)
            with open(self._tree_info_file, 'w') as fh:
                fh.write('tree_last_modified ' + str(self._tree_last_modified) + '\n')
                for key in self.tree_info:
                    finfo = self.tree_info[key]
                    # clean out stuff we shouldn't persist
                    if 'groups' in finfo:
                        del finfo['groups']
                    fh.write(json.dumps(finfo, cls=u.DateTimeEncoder) + '\n')
        except Exception as exc:
            logging.info("writing file '%s, error %s" % (self._tree_info_file, str(exc)))
            raise
        Config.log('', tag='FILE_DEST_META_WRITTEN')

    def read_tree_info(self, dest=None):
        if dest is None:  # as opposed to falsy, like an empty dict
            dest = self.tree_info
        dest.clear()
        try:
            with open(self._tree_info_file) as fh:
                for line in fh:
                    line = line.rstrip()
                    if line.startswith('tree_last_modified'):
                        self._tree_last_modified = float(re.split(r'\s+', line)[1])
                        continue
                    info = json.loads(line, cls=u.DateTimeDecoder)
                    dest[info['key']] = info
        except Exception as exc:
            msg = "reading file '%s, error '%s'" % (self._tree_info_file, str(exc))
            Config.log(msg, tag='FILE_DEST_META_FILE_ERROR')
            if self.config.is_true(self.config_section, 'ignore_missing_persist_file'):
                msg = "rebuilding tree info from files system and continuing"
                Config.log(msg, tag='FILE_DEST_META_REBUILD')
                self.get_tree_info()
                return
            raise

    # read info about remote tree; compare to locally persisted version; report
    # diffs; and bring local version up to date.
    def sync_tree_info(self, options=None):
        if self._synced_tree_info:
            Config.log(self.config_section, tag='FILE_DEST_META_ALREADY_SYNCED')
            return
        start = u.timestamp_now()
        logging.info("starting FileDest metadata sync")
        if options is None:
            options = self._default_sync_options
        # need to read persisted file, as that's the only place non-file-system metadata can live
        self.read_tree_info()
        # determine whether to force full refresh, only do it if tree has changed, on simply trust metadata:
        do_full_refresh = False
        if 'refresh_dest_meta' in options:
            do_full_refresh = options['refresh_dest_meta']
        # computing all those md5s takes a long time, so optionally skip it if the most
        # recent modification time for _file_dest_root is unchanged since we last did it
        if 'skip_refresh_if_tree_unchanged' in options:
            last_mod = u.dir_last_modified(self._file_dest_root)
            expected_last_mod = self._tree_last_modified
            do_full_refresh = last_mod != expected_last_mod
            msg = "last_mod do_full_refresh = '%s', last_mod = '%f', expected_last_mod = '%f'" % (
                do_full_refresh, last_mod, expected_last_mod)
            Config.log(msg, tag='FILE_DEST_META_TREE_UNCHANGED_TEST')

        if do_full_refresh:
            # physically walk the tree as it might not match persisted data
            for dir_name, subdirs, files in os.walk(self._file_dest_root):
                for file_name in files:
                    full_src = dir_name + '/' + file_name
                    setit = False
                    rel_path = u.make_rel_path(self._file_dest_root, dir_name, strict=False, no_leading_slash=True)
                    key = u.make_key(rel_path, file_name)
                    local_meta = u.local_metadata(dir_name, file_name)
                    local_meta['md5'] = u.md5(full_src)
                    if key in self.tree_info:
                        saved_meta = self.tree_info[key]
                        if 'md5' not in saved_meta:
                            saved_meta['md5'] = 'ERROR! md5 MISSING FROM tree_info!'
                        if local_meta['md5'] == saved_meta['md5']:
                            # sanity check
                            if local_meta['size'] != saved_meta['size']:
                                msg = "key '%s', saved: size %i, read: size %i" % (
                                    key, saved_meta['size'], local_meta['size'])
                                Config.log(msg, tag='FILE_DEST_META_ERROR_NONFATAL')
                            # otherwise file is perfect, continue
                        else:
                            msg = "key '%s', md5 mismatch. saved: '%s', read: '%s'" % (
                                    key, saved_meta['md5'], local_meta['md5'])
                            Config.log(msg, tag='FILE_DEST_META_ERROR_FATAL')
                            setit = True
                    else:
                        msg = "key '%s' not found in saved, adding" % key
                        Config.log(msg, tag='FILE_DEST_META_NEW_FILE')
                        setit = True

                    if setit:
                        local_meta['key'] = key
                        self.tree_info[key] = local_meta
                        # important: must never expose 'full' outside this class - it's a private
                        # implementation detail. Same for 'path'. Only 'key' is public
                        del local_meta['full']
                        del local_meta['path']
                        self.tree_info[key] = local_meta
                    self.tree_info[key]['_found_file_'] = True

            missing = []
            for key in self.tree_info:
                if '_found_file_' in self.tree_info[key]:
                    del self.tree_info[key]['_found_file_']
                else:
                    missing.append(key)
                    msg = "no file matching key '%s', deleting" % key
                    Config.log(msg, tag='FILE_DEST_META_MISSING')
            for key in missing:
                del self.tree_info[key]

            self.write_tree_info()
            act = "completed"
        else:
            # trust the persisted file (faster)
            act = "bypassed"
        elapsed = u.timestamp_now() - start
        msg = "%s confirmation of tree info in %f seconds" % (act, elapsed)
        Config.log(msg, tag='FILE_DEST_SYNC')
        self._synced_tree_info = True

    def is_pending(self, key):
        return key in self.tree_info and 'pending' in self.tree_info[key] and self.tree_info[key]['pending']

    # record info about a file that exists in output dir but hasn't been uploaded yet.
    def track_file(self, finfo):
        self.sync_tree_info() # should always be a no-op
        key = finfo['key'] # must exist
        # don't overwrite info about non-pending (physically present) dest file.
        # exception is metadata, which might be new
        if key in self.tree_info and not self.is_pending(key):
            self.transfer_metadata(finfo, local_root=self.local_root, dest=self.tree_info[key])
            return
        if 'md5' not in finfo:
            msg = "logic error in FileDest.track_file, no md5 for key '%s'" % finfo['key']
            logging.exception(msg)
            return

        info = {
            'pending': True,
            'name': finfo['name'],
            'rel_path': finfo['rel_path'],
            'key': key,
            'size': finfo['size'],
            'modified': finfo['modified'],
            'md5': finfo['md5']
        }
        self.transfer_metadata(finfo, local_root=self.local_root, dest=info)
        self.tree_info[key] = info

    # sync our file tree to the file tree of an upstream dest_mgr (for now, must be another FileDest).
    # fixer is a function that will do any needed rewriting of the passed filename, in-place.
    def sync_to_upstream_dest_mgr(self, upstream, refresh_me, refresh_upstream, tmp_folder, fixer=None):
        msg = "refresh_me = %s, refresh_upstream = %s, upstream root = '%s', my root = '%s', tmp_folder = %s" % \
              (refresh_me, refresh_upstream, upstream._file_dest_root, self._file_dest_root, tmp_folder)
        Config.log(msg, tag='FILE_DEST_SYNC_TO_UPSTREAM_STARTING')
        do_refresh = {'refresh_dest_meta': True}
        dont_refresh = {'refresh_dest_meta': False}
        smart_refresh = {'refresh_dest_meta': True, 'skip_refresh_if_tree_unchanged': True}
        if refresh_me == 'full':
            self.sync_tree_info(options=do_refresh)
        elif refresh_me == 'smart':
            self.sync_tree_info(options=smart_refresh)
        else:
            self.sync_tree_info(options=dont_refresh)
        if refresh_upstream == 'full':
            upstream.sync_tree_info(options=do_refresh)
        elif refresh_upstream == 'smart':
            upstream.sync_tree_info(options=smart_refresh)
        else:
            upstream.sync_tree_info(options=dont_refresh)

        u.clear_folder(tmp_folder)
        start = u.timestamp_now()
        for key, finfo in upstream.tree_info_items():
            src = os.path.join(upstream._file_dest_root, key)
            if not os.path.exists(src):
                msg = "file '%s' does not exist" % src
                Config.log(msg, tag='FILE_DEST_SYNC_TO_UPSTREAM_METADATA ERROR')
                continue
            if self.config.is_template_type(finfo['name']) and fixer:
                # copy and fix up
                dest = os.path.join(tmp_folder, key)
                u.ensure_path_for_file(dest)
                shutil.copyfile(src, dest)
                fixer(dest)
                # make new metadata and copy to self
                newfi = copy.deepcopy(finfo)
                path, file = os.path.split(dest)
                local_meta = u.local_metadata(path, file)
                newfi['size'] = local_meta['size']
                newfi['modified'] = local_meta['modified']
                newfi['md5'] = u.md5(dest)
                self.tree_info[key] = newfi
                src = dest
                dest = os.path.join(self._file_dest_root, key)
                u.ensure_path_for_file(dest)
                shutil.copyfile(src, dest)
                # we could remove fixed-up file now, but clear_folder at end probably faster
            else:
                # file not subject to fixup. just copy if missing/older/diff size
                copyit = False
                if key in self.tree_info:
                    # compare metadata and see whether to copy
                    myfi = self.tree_info[key]
                    if myfi['md5'] != finfo['md5'] or \
                            myfi['modified'] < finfo['modified'] or \
                            myfi['size'] != finfo['size']:
                        copyit = True
                else:
                    copyit = True
                if copyit:
                    # REVIEW - deepcopy probably safe here because we're copying from
                    # one dest mgr to another
                    self.tree_info[key] = copy.deepcopy(finfo)
                    dest = os.path.join(self._file_dest_root, key)
                    u.ensure_path_for_file(dest)
                    shutil.copyfile(src, dest)

        # delete from me if not in upstream
        to_delete = {}
        for key, finfo in self.tree_info_items():
            if key not in upstream.tree_info:
                to_delete[key] = os.path.join(self._file_dest_root, finfo['key'])
        for key, full in to_delete.items():
            os.remove(full)
            del self.tree_info[key]

        self.write_tree_info()

        # this is a space-saving move, but should be small, and might
        # be handy to have files around for debug. could be a config option.
        # u.clear_folder(tmp_folder)

        elapsed = u.timestamp_now() - start
        msg = "done, elapsed %f seconds" % elapsed
        Config.log(msg, tag='FILE_DEST_SYNC_TO_UPSTREAM_FINISHED')
