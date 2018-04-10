__author__ = 'dsteinkraus'

import os
import re
import logging
import copy
import shutil
from config import Config

import util as u


# =================================== class TreeProcessor ========================================

class TreeProcessor(object):
    def __init__(self, config, dest_mgr=None):
        self.config = config
        # key is rule type, value is array of rules.
        # only rule types so far:
        # self_tree: rules run against TP's own trees (input and output)
        # dest: rules run against tree of files deployed on dest
        self._rule_funcs = {'self_tree': [], 'dest': []}
        self.file_info = {}
        self.symbols = {}
        self._dest_mgr = dest_mgr
        self._track_file_callback = None
        if self._dest_mgr:
            self._track_file_callback = self._dest_mgr.track_file
        self.input_mgrs = []
        self._files_processed = 0
        self.PASSES = 20  # max iterations to process all new files
        self._pass = 0
        self._root_file_limit = config.get_int('process', 'root_file_limit', default=1000000)
        self._root_files_processed = 0

        # rule parsing and other regexes
        self._re = {}
        self._re['regex'] = re.compile(r'(.*) like (.*)')
        self._re['copy'] = re.compile(r'copy to (.*)')
        self._re['group'] = re.compile(r'^\$(\d+)$')
        self._re['cond'] = re.compile(r'^(\s*)if\s+(.*):\s*$')
        self._re['action'] = re.compile(r'^(\s*)(.*)$')
        self._re['header'] = re.compile(r'^(\s*)\[(.*)\]$')
        self._re['comment'] = re.compile(r'^\s*(#.*)?$')
        self._re['arb_fn'] = re.compile(r'(\S+)\s+(.*)')
        self._re['define'] = re.compile(r'^\s*([a-zA-Z][_a-zA-Z0-9]*)\s*=\s*(.*)$')

        unpack_filter = config.get('process', 'unpack_files_wanted', return_none=True)
        if unpack_filter:
            self._re['unpack_files_wanted'] = re.compile(unpack_filter)

        self._always_unpack = self.config.is_true('process', 'always_unpack', absent_means_yes=True)
        self.unpack_root = self.config.input + u.unpack_marker()
        u.ensure_path(self.unpack_root)
        self.clear_first = self.config.is_true('process', 'clear_first',
                                               absent_means_yes=True)
        self.make_md5 = self.config.is_true('process', 'make_md5',
                                            absent_means_no=True)
        self._parse_rules()

    # parse the rules config file into multi-line rules
    def _get_rules(self):
        try:
            rules = []
            cur_rule = {'condition': {}, 'actions': []}
            rule_file = self.config.get('process', 'rule_file', return_none=True)
            if rule_file:
                tmp_fh = u.process_includes(rule_file)
                rule_lines = tmp_fh.read().splitlines()
                tmp_fh.close()
                logging.info("using rules from file %s" % rule_file)
            else:
                raise Exception("required config value 'rule_file' is missing")

            # TODO add support for reformatting split lines

            for line in rule_lines:
                match = re.search(self._re['define'], line)
                if match:
                    key = match.group(1)
                    val = u.debracket(match.group(2), self.interpret)
                    Config.log("'%s' = '%s'" % (key, val), tag='RULE_DEFINE')
                    self.symbols[key] = val
                    continue
                if re.search(self._re['comment'], line):
                    continue  # allow commented or blank lines
                match = self._re['header'].search(line)
                if match:
                    if cur_rule['condition']:
                        rules.append(cur_rule)
                        cur_rule = {'condition': {}, 'actions': []}
                    cur_rule['props'] = u.comma_split(match.group(2))
                    continue
                match = self._re['cond'].search(line)
                if match:
                    if cur_rule['condition']:
                        rules.append(cur_rule)
                        cur_rule = {'condition': {}, 'actions': []}
                    indent = match.group(1)  # not used at present
                    cur_rule['condition']['text'] = match.group(2)
                    continue
                match = self._re['action'].search(line)
                if match:
                    if not cur_rule['condition']:
                        raise Exception("Logic error 1  in rule line '%s'" % line)
                    cur_rule['actions'].append({'text': match.group(2)})
                    continue
                raise Exception("Logic error 2 in rule line '%s'" % line)
            if cur_rule['condition']:
                if not cur_rule['actions']:
                    err = "Rule has condition '%s' but no actions" % cur_rule['condition']['text']
                    raise Exception(err)
                rules.append(cur_rule)
            return rules
        except Exception as exc:
            Config.log(str(exc), tag='TP_RULES')
            raise

    # read rules and build a function to apply them in order
    def _parse_rules(self):
        del self._rule_funcs['self_tree'][:]
        del self._rule_funcs['dest'][:]
        # need global symbols and those defined in rules file now, as they can be used in conditions
        self.make_symbols()
        rules = self._get_rules()
        for rule in rules:
            rule['label'] = None # ensure key exists
            rule['run_by_default'] = True
            rule_type = 'self_tree'  # the default
            if 'props' in rule:
                for prop in rule['props']:
                    (key, vals) = u.keyval_split(prop)
                    if key == 'type':
                        if len(vals) != 1:
                            raise Exception('rule type must be single-valued')
                        if vals[0] == 'dest':
                            rule_type = 'dest'
                        elif vals[0] == 'self_tree':
                            rule_type = 'self_tree'
                        else:
                            raise Exception("unsupported rule type '%s'" % vals[0])
                    elif key == 'label':
                        rule['label'] = vals[0]
                    elif key == 'run_by_default':
                        if vals[0].lower() == 'false':
                            rule['run_by_default'] = False
                    else:
                        raise Exception("unsupported rule property key '%s'" % key)
            cond = rule['condition']['text']
            match = self._re['regex'].search(cond)
            if match:
                rule['condition']['target'] = match.group(1)
                rule['condition']['regex'] = match.group(2)
            elif False:  # todo check for other, non-regex conditions
                pass
            else:
                raise Exception("bad rule condition specification '%s'" % cond)
            for action in rule['actions']:
                match = self._re['copy'].search(action['text'])
                if match:
                    action['op'] = 'copy'
                    action['more'] = match.group(1)
                    continue
                if action['text'] == 'stop':
                    action['op'] = 'stop'
                    continue
                if action['text'] == 'delete':
                    action['op'] = 'delete'
                    continue
                match2 = self._re['arb_fn'].search(action['text'])
                if match2:
                    # see if first token is a plugin name
                    if self.config.plugin.have(match2.group(1)):
                        action['op'] = match2.group(1)
                        action['other_func'] = self.config.plugin.get(match2.group(1))
                        action['more'] = match2.group(2)
                        continue
                raise Exception("bad rule action specification '%s'" % action['text'])
            self._rule_funcs[rule_type].append(self._make_rule_impl(rule))

    def _make_rule_impl(self, rule):
        regex = None
        testrule = None
        tp = self
        label = rule['label']

        def test_regex_wrapper(target_name, regex):
            def test_regex(finfo):
                # local and dest finfos have different props. 'full', the full local path,
                # should only be present in local finfos. 'key', a relative path to a content
                # item, should only exist in dest finfos.
                if target_name == 'full':
                    if 'full' not in finfo:
                        raise Exception("test_regex: target_name 'full' not available, finfo = %s" % str(finfo))
                    target = finfo['full']
                    # for easier rule-writing, get rid of unpacked suffix
                    if u.unpack_marker() in target:
                        target = target.replace(u.unpack_marker(), '', 1)
                elif target_name == 'key':
                    if 'key' not in finfo:
                        raise Exception("test_regex: target_name 'key' not available, finfo = %s" % str(finfo))
                    target = finfo['key']
                else:
                    raise Exception("invalid target_name '%s'" % target_name)
                _ = label   # for conditional breakpoint
                match = re.search(regex, target)
                if match:
                    _ = label  # for conditional breakpoint
                    msg = "target '%s', groups '%s'" % (target, str(match.groups()))
                    Config.log(msg, tag='RULE_MATCH_' + label)
                    finfo['groups'] = match.groups()
                    return True
                return False

            return test_regex

        def apply_stop(finfo):
            # logging.info("stop, file %s in dir %s" % (finfo['name'], finfo['path']))
            if 'key' in finfo:
                msg = "stop, key %s" % finfo['key']
            else:
                msg = "stop, file %s" % finfo['full']
            Config.log(msg, tag='RULE_STOP')
            # returning false means don't continue with more actions.
            # setting finfo prevents later rules from firing.
            finfo['stop'] = True
            return False

        def apply_copy_wrapper(more):
            def apply_copy(finfo):
                if 'stop' in finfo:
                    del finfo['stop']
                    return False
                # source is assumed to be [full]
                # more is assumed for now to only contain dest expr
                dest = u.debracket(more, tp, finfo=finfo)

                newfi = self.copy_with_metadata(finfo, dest)
                if newfi:
                    # note that full path is the key here, not bare filename
                    self.file_info[dest] = newfi
                return True

            return apply_copy

        def apply_delete(finfo):
            if 'stop' in finfo:
                del finfo['stop']
                return False
            full = finfo['full']
            if not full in self.file_info:
                logging.warning("can't delete file '%s', not in output tree!" % full)
                return True
            logging.info("deleting file '%s'" % full)
            os.remove(full)
            del self.file_info[full]
            return True

        def apply_other_wrapper(other_func, more):
            def apply_other(finfo):
                if 'stop' in finfo:
                    del finfo['stop']
                    return False
                ret = other_func(tp, finfo, more)
                msg = "func '%s' finfo '%s'" % (str(other_func), finfo['name'])
                Config.log(msg, tag='RULE_ACTION_' + label)
                if ret is not None and type(ret) is dict:
                    if 'new_finfo' in ret:
                        self.track_file(ret['new_finfo'])
                    if 'source_changed' in ret:
                        self.track_file(finfo)
                return True

            return apply_other

        # parse condition:
        if rule['condition']['regex']:
            t = rule['condition']['target']
            if t != '[full]' and t != '[key]':
                raise Exception("can't handle target expression '%s'" % t)
            target_name = t.lstrip('[').rstrip(']')
            ar = u.bracket_parse(rule['condition']['regex'])
            fixed_re = u.fill_brackets(ar, self.interpret)
            regex = re.compile(fixed_re)
            testrule = test_regex_wrapper(target_name, regex)
        else:
            raise Exception("can't handle non-regex condition '%s'" % rule['condition']['text'])

        # parse action(s):
        funcs = []
        for action in rule['actions']:
            # internally implemented:
            if action['op'] == 'copy':
                funcs.append(apply_copy_wrapper(action['more']))
            elif action['op'] == 'stop':
                funcs.append(apply_stop)
            elif action['op'] == 'delete':
                funcs.append(apply_delete)
            else:
                # external tools: (name is vetted later)
                funcs.append(
                    apply_other_wrapper(action['other_func'], action['more']))

        def apply_funcs(finfo):
            for f in funcs:
                if not f(finfo):
                    break

        return {
            'label': rule['label'],
            'test': testrule,
            'apply': apply_funcs,
            'run_by_default': rule['run_by_default']
        }

    # symbols for bracket interpolation, mostly in rules. add others as needed
    def make_symbols(self):
        self.symbols['input_root'] = self.config.input
        self.symbols['output_root'] = self.config.output
        self.symbols['admin_root'] = self.config.admin
        self.symbols['archive_root'] = self.config.archive + '/output'

    def _walk_files(self, root_name):
        rule_type = 'self_tree'
        for dir_name, subdirs, files in os.walk(root_name):
            for file_name in files:
                if self.config.signalled():
                    logging.info("signal set, leaving tp._walk_files")
                    return
                done_with_file = False
                finfo = None
                processed_im_root_file = False
                full = dir_name + '/' + file_name
                if self._pass == 0:
                    # might have metadata from an input mgr download
                    if not dir_name.startswith(self.config.input):
                        raise Exception("logic error pass 0")
                    i_im = 0
                    for im in self.input_mgrs:
                        finfo = im.get_downloaded_finfo(full)
                        if finfo:
                            if 'rules_run' in finfo and finfo['rules_run']:
                                done_with_file = True
                            else:
                                processed_im_root_file = True
                                finfo['source_im'] = i_im
                                self.track_file(finfo)
                            break
                        i_im += 1
                    if not finfo:
                        # this should be a file unpacked from a downloaded file
                        if full in self.file_info:
                            finfo = self.file_info[full]
                        else:
                            finfo = u.local_metadata(dir_name, file_name)
                            self.track_file(finfo)
                elif full in self.file_info:
                    finfo = self.file_info[full]
                else:
                    # new file in output, created by an action
                    finfo = u.local_metadata(dir_name, file_name)
                    self.track_file(finfo)
                if self._always_unpack:
                    self._unpack_if_archive(finfo)
                self._file_action(rule_type, finfo)
                if processed_im_root_file:
                    Config.log(finfo['full'], tag='TP_IM_ROOT_FILE_PROCESSED')
                    self._root_files_processed += 1
                    if self._root_files_processed >= self._root_file_limit:
                        msg = "limit on root files processed per run (%i) reached." % self._root_file_limit
                        logging.info(msg)
                        self.config.add_to_final_summary(msg)
                        return

    def _unpack_if_archive(self, finfo):
        if 'rules_run' in finfo and finfo['rules_run']:
            return
        if finfo['full'].endswith('.tar.gz'):
            src_path = finfo['path']
            name = finfo['name']
            if self.unpack_root in src_path:
                rel_path = u.make_rel_path(self.unpack_root, src_path)
            else:
                rel_path = u.make_rel_path(self.config.input, src_path)
            dest_path = self.unpack_root + rel_path
            msg = "unpacking '%s' to '%s" % (name, dest_path)
            regex = None
            if 'unpack_files_wanted' in self._re:
                regex = self._re['unpack_files_wanted']
                msg = "selectively " + msg + " (unpack_files_wanted = '%s')" % regex.pattern
            logging.info(msg)
            self._files_processed += 1 # this counts
            unpack_dir = u.unpack_file(src_path, dest_path, name, regex_wanted=regex)
            self.file_info[finfo['full']]['rules_run'] = True
            if not unpack_dir:
                Config.log(finfo['full'], tag='TP_IGNORE_BAD_ARCHIVE')
                return
            self._walk_files(unpack_dir)

    def _file_action(self, rule_type, finfo):
        if ('rules_run' in finfo) and finfo['rules_run']:
            return  # already handled this file
        if self.config.is_special_file(finfo['name']):
            return
        self._apply_file_rules(rule_type, finfo)

    def _apply_file_rules(self, rule_type, finfo):
        matched = False
        for rule in self._rule_funcs[rule_type]:
            if not self.config.run_rule(rule):
                continue
            if 'stop' in finfo:
                break
            if rule['test'](finfo):
                matched = True
                rule['apply'](finfo)
        finfo['rules_run'] = True
        # NOTE: we did work even if we matched no rules, or only 'ignore' or 'stop' rules,
        # because setting 'rules_run' will prevent us from looking at this file again.
        self._files_processed += 1
        Config.log(finfo['full'], tag='WORK_DONE_TP')

    def process(self, do_clear_info=True):
        logging.info("starting tree processing")
        start = u.timestamp_now()
        u.ensure_path(self.config.output)
        self._root_files_processed = 0
        if self.clear_first and do_clear_info:
            u.clear_folder(self.config.output)
        # do this at start in case last run didn't clean up properly
        self.remove_unpacked_files()
        if do_clear_info:
            self.file_info.clear()
        self._pass = 0  # pass number
        self._files_processed = 0
        # make one pass over the input files. if you need to know whether this is
        # the input pass, check for self._pass == 0.
        self._walk_files(self.config.input)
        if self.config.signalled():
            logging.info("signal set, leaving tp.process")
            return False
        # then make passes over the output files until no new files are encountered
        work_done = self._files_processed > 0
        Config.log('tp._files_processed = %i' % self._files_processed, tag='WORK_DONE_PASS_0')
        # do NOT look at _root_files_processed after pass 0 - we want to fully
        # process any files created during pass 0
        while self._pass < self.PASSES:
            self._files_processed = 0
            self._pass += 1
            self._walk_files(self.config.output)
            if self.config.signalled():
                logging.info("signal set, leaving tp.process after pass %i" % self._pass)
                work_done = False
                break
            Config.log('tp._files_processed = %i' % self._files_processed, tag='WORK_DONE_PASS_%i' % self._pass)
            if self._files_processed > 0:
                work_done = True
            else:
                break
        if self._pass >= self.PASSES:
            raise Exception("completed %i passes and still not done. failing" % self.PASSES)
        self.update_input_mgr_metadata()
        elapsed = u.timestamp_now() - start
        Config.log("tp completed in %i passes, %f seconds, work_done %s" % (self._pass, elapsed, work_done), tag='WORK_DONE')
        return work_done

    # walk the tree of dest metadata and apply the set of applicable rules to each file.
    # unlike local processing, this is inherently single-pass and does not support
    # creating any new files (we don't even have access to the dest file content at this
    # point, only metadata). So less to keep track of.
    # also adds tp's file metadata to dest's.
    def dest_process(self):
        rule_type = 'dest'
        logging.debug("dest_process: %i rules of type dest" % len(self._rule_funcs[rule_type]))
        for key, finfo in self._dest_mgr.tree_info_items():
            self.copy_metadata(finfo)
            for rule in self._rule_funcs[rule_type]:
                if not self.config.run_rule(rule):
                    continue
                if 'stop' in finfo:
                    break
                if rule['test'](finfo):
                    rule['apply'](finfo)

    # copy our metadata to dest finfo, but don't overwrite any values.
    # TODO: need a more sophisticated, configurable system for metadata copying
    def copy_metadata(self, fi_dest):
        fi_src = None
        if 'full' in fi_dest and fi_dest['full'] in self.file_info:
            fi_src = self.file_info[fi_dest['full']]
        elif 'key' in fi_dest:
            full = self.config.output + '/' + fi_dest['key']
            if full in self.file_info:
                fi_src = self.file_info[full]
        if not fi_src:
            # logging.debug("copy_metadata can't find tp finfo for fi_dest '%s'" % str(fi_dest))
            return
        for key, value in fi_src.items():
            # todo consolidate with is_protected_metadata
            if key == 'full' or key == 'name' or key == 'path' or key == 'rules_run' or key == 'stop':
                continue
            if key in fi_dest:
                continue
            fi_dest[key] = value

    # TODO this needs streamlining; any other places we do similar?
    def will_upload(self, full):
        path, file = os.path.split(full)
        rel_path = u.make_rel_path(self.config.output, path, strict=False)
        return rel_path is not None and not rel_path.startswith('/tmp')

    # modifies new or changed file info
    def track_file(self, finfo):
        full = finfo['full']
        Config.log(full, tag='TP_TRACK_FILE')
        if 'md5' not in finfo:
            finfo['md5'] = u.md5(finfo['full'])
        if full in self.file_info:
            # don't replace finfo unless file has actually changed
            old_finfo = self.file_info[full]
            if finfo['md5'] == old_finfo['md5']:
                Config.log(full, tag='TP_TRACK_FILE_UNCHANGED')
                return
        self.file_info[full] = finfo
        if self._track_file_callback and self.will_upload(finfo['full']):
            if 'rel_path' not in finfo:
                finfo['rel_path'] = u.make_rel_path(self.config.output, finfo['path'], no_leading_slash=True)
            if 'key' not in finfo:
                finfo['key'] = u.make_key(finfo['rel_path'], finfo['name'])
            if ('size' not in finfo) or ('modified' not in finfo):
                logging.error('track_file (%s): finfo missing size and/or modified' % finfo['full'])
                tmp = u.local_metadata(finfo['path'], finfo['name'])
                finfo['size'] = tmp['size']
                finfo['modified'] = tmp['modified']
            self._track_file_callback(finfo)

    def interpret(self, val, finfo=None, symbols=None, options=None):
        if symbols is None:
            symbols = self.symbols
        match = re.search(self._re['group'], val)
        if match:
            if not finfo:
                raise Exception("illegal to use group markers if no finfo")
            gnum = int(match.group(1))
            if not 'groups' in finfo or len(finfo['groups']) < gnum:
                raise Exception("symbol '%s' val not compatible with groups" % val)
            return finfo['groups'][gnum - 1]
        if finfo and (val in finfo):
            return finfo[val]
        if val in symbols:
            return symbols[val]
        return None

    # copy a file and its metadata - return new metadata
    def copy_with_metadata(self, finfo, dest):
        (dest_dir, dest_file) = os.path.split(dest)
        # prohibit destinations outside our output root
        rel_path = u.make_rel_path(self.config.output, dest_dir, strict=False)
        if not rel_path:
            logging.warning(
                "disallowing copy dest '%s', outside of output_root '%s'" %
                (dest_dir, self.config.output))
        # no self-copy
        if dest == finfo['full']:
            logging.warning("not copying file '%s' onto itself!" % dest)
            return None
        u.ensure_path(dest_dir)
        Config.log("%s to %s" % (finfo['full'], dest), tag='COPY_WITH_METADATA')
        if not os.path.exists(finfo['full']):
            msg = "file '%s' does not exist" % finfo['full']
            Config.log(msg, tag='COPY_WITH_METADATA_ERROR')
            return None
        shutil.copyfile(finfo['full'], dest)
        # set metadata for new file
        local = u.local_metadata(dest_dir, dest_file)
        newfi = copy.deepcopy(finfo) # TODO replace with unified metadata-copy system
        newfi['name'] = dest_file
        newfi['path'] = dest_dir
        newfi['full'] = dest
        newfi['size'] = local['size']
        newfi['modified'] = local['modified']
        # clear transient metadata not applicable to new file
        u.remove_no_copy_metadata(newfi)
        newfi['rules_run'] = False
        return newfi

    # at end of run, call this to tell any input mgr objects that we ran rules
    # against their downloadable files, so they can persist that. TODO horrible inefficiency
    def update_input_mgr_metadata(self):
        for full, finfo in self.file_info.items():
            if 'source_im' in finfo and 'rules_run' in finfo and finfo['rules_run']:
                im = self.input_mgrs[finfo['source_im']]
                # need to pass full name for disambiguation
                im.rules_run_files.append(finfo['full'])
                del finfo['source_im'] # don't persist this flag
        for im in self.input_mgrs:
            im.update_rules_run_files()

    def remove_unpacked_files(self):
        u.clear_folder(self.unpack_root)

    # back up the output tree to another location, using rsync. this will overwrite
    # existing files with newer version. intentionally, does not delete files found
    # in dest but not in source. Do not archive tmp subtree.
    def archive(self, options=None):
        if options is None:
            options = {}
        if not self.config.archive:
            logging.error("can't archive, no archive_root configured")
            return
        try:
            u.ensure_path(self.config.archive + '/output')
            u.deploy_tree(self.config.output, self.config.archive + '/output', options=options)
        except Exception as exc:
            err = "archive: exception '%s' running rsync" % str(exc)
            logging.error(err)
            raise
        Config.log('', tag='TP_ARCHIVE_COMPLETE')

    # given a regex, copy all archive files that match it back into the output
    # directory. used with clear_first turned off to reprocess some output files
    # that were there in a past run.
    def restore_from_archive(self, wanted, options=None):
        if options is None:
            options = {}
        if 'verbose' in options and options['verbose']:
            logging.info("restore_from_archive starting")
        re_wanted = re.compile(wanted)
        archive_root = self.config.archive + '/output'
        for dir_name, subdirs, files in os.walk(archive_root):
            for file_name in files:
                full_src = dir_name + '/' + file_name
                if re.search(re_wanted, full_src):
                    full_dest = u.reroot_file(full_src, archive_root, self.config.output)
                    (dest_full_path, dest_name) = os.path.split(full_dest)
                    u.ensure_path(dest_full_path)
                    shutil.copyfile(full_src, full_dest)
                    if 'verbose' in options and options['verbose']:
                        logging.info("restore_from_archive %s -> %s" % (full_src, full_dest))
        if 'verbose' in options and options['verbose']:
            logging.info("restore_from_archive completed")
