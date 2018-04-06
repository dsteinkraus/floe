__author__ = 'dsteinkraus'

import boto3
import botocore
import os
import io
import logging
import json
import shutil
import calendar
import mimetypes

from config import Config
import util as u

# destination manager using Amazon S3. impleented as a plugin primarily so that
# floe core doesn't have a dependency on boto.

def register():
    return {
        'S3': get_s3
    }

def get_s3(config, config_section):
    return S3(config, config_section)

#===================== class S3 =========================================================

class S3(object):
    def __init__(self, config, config_section):
        self.config = config
        self.config_section = config_section
        self.bucket_name = self.config.get(config_section, 'bucket_name')
        self.region = self.config.get(config_section, 'region')
        self.bucket = None
        self.tree_info = {}
        self._tree_info_file = config.admin + '/' + '_s3_tree_info.txt'
        self._synced_tree_info = False
        self._max_upload_size = 1024 * 1024 * self.config.get_int(config_section, 'max_upload_size', default=-1)
        self._upload_count = 0
        self.local_root = config.output
        self._default_sync_options = {}
        if self.config.is_true(self.config_section, 'refresh_dest_meta', absent_means_no=True):
            self._default_sync_options['refresh_dest_meta'] = True
        # turn down boto log noise
        boto3.set_stream_logger('botocore', logging.ERROR)
        boto3.set_stream_logger('boto3', logging.ERROR)
        boto3.set_stream_logger('s3transfer', logging.ERROR)

        # Create an S3 client and resource (they have different APIs, resource is "higher level")
        self._resource = boto3.resource('s3')
        self._client = boto3.client('s3')

    def get_default_bucket(self):
        if not self.bucket:
            # expose default bucket (code can still work with other buckets if needed)
            self.bucket = self.get_bucket(self.bucket_name)
        return self.bucket

    def get_bucket(self, bucket_name):
        bucket = self._resource.Bucket(bucket_name)
        try:
            self._resource.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                err = "bucket '%s' does not exist" % bucket_name
            else:
                err = str(e)
            raise Exception(err)
        return bucket

    # upload a file with bare name srcName, in folder srcPath, to the bucket.
    # destPath is the folder name in the bucket where the file should be placed (TODO).
    # uses bare file name as key, and makes the object public.

    def upload(self, src_path, src_name, key, bucket=None, content_type=None, extra_args=None):
        if not bucket:
            bucket = self.get_default_bucket()
        full = src_path + '/' + src_name
        if extra_args is None:
            extra_args = {
                'ACL': 'public-read',
                'Metadata': {
                    'md5': u.md5(full)
                }
            }
        if not content_type:
            content_type = mimetypes.MimeTypes().guess_type(src_name)[0]
            extra_args['ContentType'] = content_type
        bucket.upload_file(full, key, ExtraArgs=extra_args)

    # upload some bytes as a new object in bucket
    # TODO non-default bucket should be optional
    # TODO currently forcing user to provide content type. could use magic:
    # https://stackoverflow.com/questions/43580/how-to-find-the-mime-type-of-a-file-in-python
    def upload_obj(self, key, objBytes, contentType, bucket=None):
        if not bucket:
            bucket = self.get_default_bucket()
        file_like = io.BytesIO(objBytes)
        bucket.upload_fileobj(file_like, key, ExtraArgs={'ContentType': contentType, 'ACL': 'public-read'})

    # return count of files uploaded
    # convention is that anything in the /tmp tree does not get uploaded
    def upload_tree(self, local_root, remote_root='', options=None, local_tree_meta=None):
        logging.info("starting S3 upload")
        if not options:
            options = {'use_md5': True}
        start = u.timestamp_now()
        self._upload_count = 0
        # refresh and save data for files already on S3
        self.sync_tree_info()

        for dir_name, subdirs, files in os.walk(local_root):
            rel_path = u.make_rel_path(local_root, dir_name)
            if not rel_path.startswith('/tmp'):
                for file_name in files:
                    local_file = dir_name + '/' + file_name
                    key = u.make_key(rel_path, file_name)
                    local_md5 = u.md5(local_file)
                    if local_tree_meta and local_file in local_tree_meta:
                        local_meta = local_tree_meta[local_file]
                    else:
                        local_meta = u.local_metadata(dir_name, file_name)
                    size = local_meta['size']
                    do_upload = True
                    if 'use_md5' in options:
                        if key in self.tree_info:
                            if 'md5' in self.tree_info[key]:
                                remote_md5 = self.tree_info[key]['md5']
                                do_upload = do_upload and remote_md5 != local_md5
                            else:
                                err = "no md5 value for existing key '%s' (old version?)" % key
                                logging.error(err)
                        else:
                            logging.debug("file '%s' is not in S3" % key)
                        if self._max_upload_size >= 0 and size > self._max_upload_size:
                            logging.debug("file '%s' size (%i) > limit (%i), won't upload" %
                                          (key, size, self._max_upload_size))
                            do_upload = False
                    if do_upload:
                        extra_args = {
                            'ACL': 'public-read',
                            'Metadata': {'md5': local_md5}
                        }
                        logging.debug("S3 object upload starting, key = '%s', %i bytes" %
                                      (key, size))
                        start = u.timestamp_now()
                        self.upload(dir_name, file_name, key, extra_args=extra_args)
                        rate = size / (u.timestamp_now() - start)
                        logging.debug("S3 object uploaded, key = '%s', %f bytes/sec" %
                                      (key, rate))
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
                        logging.debug("S3 object not uploaded, key = '%s'" % key)
        self.write_tree_info()
        elapsed = u.timestamp_now() - start
        logging.info("S3.upload_tree finished in %f seconds, uploaded %i files" %
                     (elapsed, self._upload_count))
        return self._upload_count

    # given an S3 object, return length in bytes, and last modified time as UTC unix timestamp
    def get_object_info(self, obj, bucket):
        last_mod = obj.last_modified
        # ObjectSummary (returned by objects.all()) doesn't have content_length, does have size
        if 'size' in dir(obj):
            obj_len = obj.size
        else:
            obj_len = obj.content_length
        # e_tag = md5, unless file was multipart-uploaded. see:
        # https://stackoverflow.com/questions/26415923/boto-get-md5-s3-file
        # so need to store/retrieve our own md5 metadata value.
        # py 2.x: s3obj = self._resource.Object(bucket.name, obj.key.encode('utf-8'))
        s3obj = self._resource.Object(bucket.name, str(obj.key))
        if 'md5' in s3obj.metadata:
            md5 = s3obj.metadata['md5']
        else:
            logging.warning("no md5 metadata for key '%s'. foreign upload?" % obj.key)
            md5 = ''
        timestamp = calendar.timegm(last_mod.timetuple())
        (path, name) = os.path.split(obj.key)
        return {
            'key': obj.key,
            'name': name,
            'size': obj_len,
            'modified': timestamp,
            'mod_dt': last_mod,
            'e_tag': obj.e_tag,
            'md5': md5
        }

    # walk the S3 tree and get/store metadata for all objects
    # todo implement filtering based on remote_root (or scrap it)
    def get_tree_info(self, bucket=None, remote_root=''):
            if not bucket:
                bucket = self.get_default_bucket()
            self.tree_info.clear()
            for obj in bucket.objects.all():
                info = self.get_object_info(obj, bucket)
                self.tree_info[info['key']] = info

    # return iterator for current tree_info
    def tree_info_items(self):
        self.sync_tree_info()
        return self.tree_info.items()

    # TODO do this for other classes that have persist files, too
    def write_tree_info(self):
        try:
            shutil.copyfile(self._tree_info_file, self._tree_info_file + '.bak')
        except Exception as exc:
            logging.warning("ignoring exception '%s' persisting S3 tree info" % str(exc))
        try:
            with open(self._tree_info_file, 'w') as fh:
                for key in self.tree_info:
                    fh.write(json.dumps(self.tree_info[key], cls=u.DateTimeEncoder) + '\n')
        except Exception as exc:
            logging.info("writing file '%s, error %s" % (self._tree_info_file, str(exc)))
            raise

    def read_tree_info(self, dest=None):
        if dest is None:  # as opposed to falsy, like an empty dict
            dest = self.tree_info
        dest.clear()
        try:
            with open(self._tree_info_file) as fh:
                for line in fh:
                    line = line.rstrip()
                    info = json.loads(line, cls=u.DateTimeDecoder)
                    dest[info['key']] = info
        except Exception as exc:
            msg = "reading file '%s, error '%s'" % (self._tree_info_file, str(exc))
            Config.log(msg, tag='S3_DEST_META_FILE_ERROR')
            if self.config.is_true(self.config_section, 'ignore_missing_persist_file'):
                msg = "rebuilding tree info from files system and continuing"
                Config.log(msg, tag='S3_DEST_META_REBUILD')
                self.get_tree_info()
                return
            raise

    # read info about remote tree; compare to locally persisted version; report
    # diffs; and bring local version up to date.
    def sync_tree_info(self, options=None):
        if self._synced_tree_info:
            logging.debug("skipping sync_tree_info, already done")
            return
        if options is None:
            options = self._default_sync_options
        if 'refresh_dest_meta' in options:
            do_full_refresh = options['refresh_dest_meta']
        if do_full_refresh:
            start = u.timestamp_now()
            logging.info("starting s3 metadata sync")
            if not options:
                options = {}
            self.get_tree_info()
            self.write_tree_info()
            elapsed = u.timestamp_now() - start
            logging.info("S3.sync_tree_info finished in %f seconds" % elapsed)
            self._synced_tree_info = True
        else:
            # read local S3 tree info (much faster)
            start = u.timestamp_now()
            if not options:
                options = {}
            self.read_tree_info()
            elapsed = u.timestamp_now() - start
            logging.info("S3 tree metadata loaded from local file in %f seconds" % elapsed)
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

    def speed_test(self, size):
        name = "tempfile.text"
        with open(name, "wb") as fh:
            # py2 fh.write("\0" * size)
            fh.write(b"\0" * size)

        start = u.timestamp_now()
        self.upload(".", name, name)
        elapsed = u.timestamp_now() - start
        rate = size / elapsed
        logging.debug("S3 object uploaded, key = '%s', %i bytes, %f sec, %f bytes/sec" %
                      (name, size, elapsed, rate))
