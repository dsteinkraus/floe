__author__ = 'dsteinkraus'

import os
import re
import logging
import shutil
import collections

from config import Config
import util as u

#=================================== class WebMaker ========================================

class WebMaker(object):
    def __init__(self, config, dest_mgr=None, track_file_callback=None):
        self.config = config
        self.dest_mgr = dest_mgr
        self._track_file_callback = track_file_callback
        # site root is the tree of fixed assets that are always built and replicated
        # on the remote site (e.g. index.html)
        self.site_root = config.template_root + '/site'
        self.generate_root = config.template_root + '/generated'
        u.ensure_path(self.site_root)
        self.output_root = self.config.output
        self._worklists = {}
        self._re = {}
        self._re['worklist'] = re.compile(r'^worklist\s+(.*)$', re.MULTILINE|re.DOTALL)
        self._re['get_config'] = re.compile(r'^get_config\s+([a-z_]+)\s*:\s*(.*)')
        self._re['part'] = re.compile(r'^part\s+(.*)$')
        self._files_created = []
        self.page_context = None
        self._default_worklist = None
        self._mode_plugins = {}

    def ensure_worklist(self, worklist_name):
        if not worklist_name:
            raise Exception("logic error, empty worklist_name")
        if not worklist_name in self._worklists:
            self._worklists[worklist_name] = {}
        return self._worklists[worklist_name]

    # add an finfo, in a particular role, to a list entry on a particular worklist.
    # the entry and the worklist are created if not there.
    # if the role is default, new finfos will accumulate on a list.
    def add_file(self, finfo, argstr):
        args = u.parse_arg_string(argstr)
        if not 'worklist' in args:
            raise Exception("worklist_name argument is required")
        worklist_name = args['worklist']
        if 'role' in args:
            role = args['role']
        else:
            role = 'default'
        the_list = self.ensure_worklist(worklist_name)
        if 'dest_key' in args:
            key = args['dest_key']
            finfo['dest_key'] = key
        else:
            if 'key' in finfo:
                key = finfo['key']
            else:
                key = finfo['full']
        if not key:
            raise Exception("no dest_key available")
        Config.log("key '%s' worklist '%s'" % (key, worklist_name), tag='WEBMAKER_WL_ADD')

        if not key in the_list:
            the_list[key] = {
                'roles': {'default': []},
                'index': len(the_list)
            }
        # save any extra args passed
        # todo review - what if this overwrites old args?
        the_list[key]['item_args'] = args
        if role in the_list[key]['roles']:
            if role == 'default':
                the_list[key]['roles'][role].append(finfo)
            else:
                msg = "replacing existing '%s' role in worklist item (key = '%s', worklist = '%s')" \
                    % (role, key, worklist_name)
                logging.warning(msg)
                the_list[key]['roles'][role] = finfo

    def track_file(self, finfo, check_ext=True):
        self._files_created.append(finfo)
        if self._track_file_callback:
            self._track_file_callback(finfo)

    def _link(self, file_name):
        return "<a href='" + file_name + "'>" + file_name + "</a>"

    def _para(self, str):
        return "<p>" + str + "</p>"

    def _get_mode_plugin(self, name):
        if not name in self._mode_plugins:
            f = self.config.plugin.get(name)
            if not f:
                raise Exception("mode plugin named '%s' not found" % name)
            self._mode_plugins[name] = f
        return self._mode_plugins[name]

    def _render_worklist(self, argstr=''):
        ret = ''
        list_name = 'UNDEFINED'
        try:
            argstr = argstr.replace('\n', ' ').replace('\t', ' ')
            list_args = u.parse_arg_string(argstr)
            if not 'name' in list_args or not list_args['name']:
                raise Exception("list name not specified in list_args '%s'" % argstr)
            list_name = list_args['name']
            if self.page_context:
                list_key = self.page_context + '.' + list_name
                if not list_key in self._worklists:
                    # logging.info("no worklist '%s', falling back to '%s'." % (list_key, list_name))
                    list_key = list_name
            else:
                list_key = list_name
                Config.log(list_key, tag='RENDER_WORKLIST_' + list_key)
            if not list_key in self._worklists or not len(self._worklists[list_key]):
                msg = "worklist " + list_key + " is empty."
                Config.log(msg, tag='RENDER_WORKLIST_EMPTY_' + list_key)
                if 'if_empty' in list_args:
                    #  put special work_item on list to render in empty case
                    the_list = self.ensure_worklist(list_key)
                    the_list['if_empty'] = {
                        'index': 0,
                        'roles': {'default': []},
                        'item_args': {}
                    }
                    Config.log(msg, tag='RENDER_WORKLIST_HANDLE_IF_EMPTY_' + list_key)
                else:
                    return "<p>%s</p>" % msg
            if 'mode' in list_args and list_args['mode']:
                mode = list_args['mode']
            else:
                msg = "worklist '%s' has no mode specified. using default." % list_key
                logging.debug(msg)
                mode = 'default'
            keyname = 'value'
            if mode == 'literal' and 'keyname' in list_args:
                keyname = list_args['keyname']
            max_items = 1000000
            if 'max_items' in list_args:
                max_items = int(list_args['max_items'])
            rel_path = ''
            work = None
            wl = self._worklists[list_key]
            # attach symbols
            if 'symbols' in list_args:
                skip_if_no_symbols = self.config.is_true('symbols', 'skip_if_no_symbols', absent_means_yes=True)
                symbol_set_name = list_args['symbols']
                for key, work_item in wl.items():
                    if key != 'if_empty':
                        # TODO how to handle lookup key?
                        # TODO want option to add directly rather than to item_args??
                        symbol_key = work_item['item_args']['symbol_key']
                        found = self.config.attach_symbols(symbol_set_name, symbol_key, work_item['item_args'])
                        if not found and skip_if_no_symbols:
                            work_item['skip'] = True

            wl = {k: wl[k] for k in wl if 'skip' not in wl[k] or not wl[k]['skip']}

            if len(wl) < 2:
                # sorting meaningless and might not work, so bypass it
                work = collections.OrderedDict(wl)
            else:
                reverse = 'reverse' in list_args
                if 'order' in list_args:
                    order = list_args['order']
                    if order == 'key':
                        # case-insensitive alpha sort of key
                        work = collections.OrderedDict(
                            sorted(wl.items(), key=lambda item: item[0], reverse=reverse))
                    else:
                        work = collections.OrderedDict(
                            sorted(wl.items(), key=lambda item: item[1]['item_args'][order], reverse=reverse))
                else:
                    # default ordering is as they were added
                    # TODO not needed if used OrderedDict in the first place
                    work = collections.OrderedDict(
                        sorted(wl.items(), key=lambda item: item[1]['index'], reverse=reverse))

            #--------------- render it -----------------------------
            msg = "list has %i items, rendering %i." % (len(work), min(len(work), max_items))
            Config.log(msg, tag='RENDER_WORKLIST_' + list_key)
            rendered = 0
            for key, work_item in work.items():
                if 'skip' in work_item and work_item['skip']:
                    Config.log(key, tag='RENDER_WORKLIST_SKIP_KEY')
                    continue
                if mode == 'literal':
                    ret += work_item[keyname]
                    continue
                # check for plugin matching mode name
                f = self._get_mode_plugin('render_mode_' + mode)
                ret += f(self, key, work_item, list_args)
                rendered += 1
                if rendered >= max_items:
                    break
                continue
            return ret
        except Exception as exc:
            msg = "exception '%s' rendering worklist '%s' (context '%s')" % (str(exc), list_name, self.page_context)
            Config.log(msg, tag='RENDER_WORKLIST_ERR')
            ret += 'An error has occurred.'
            return ret

    def render_part(self, argstr):
        # NOTE: could do bracket sub here if needed
        args = u.parse_arg_string(argstr, options={'merge': True})
        return self.render_part_args(args)

    def render_part_args(self, args):
        if not 'name' in args:
            raise Exception("name not specified in args '%s'" % str(args))
        part_file = self.config.template_root + '/parts/' + args['name'] + '.html'
        part = open(part_file).read()
        return u.debracket(part, self.interpret, symbols=args)

    def interpret(self, val, finfo=None, symbols=None, worklist=None, options=None):
        if val == 'update_time':
            return self.config.start_time.strftime("%b %d %Y %H:%M:%S")
        # get_config expressions
        match = re.search(self._re['get_config'], val)
        if match:
            ret = self.config.get(match.group(1), match.group(2), return_none=True)
            if ret is None:
                msg = "key '%s' in section '%s' not in config" % (match.group(2), match.group(1))
                Config.log(msg, tag='WM_INTERPRET_GET_CONFIG_NOT_FOUND')
                if 'get_config_ignore_not_found' in options:
                    return ''
                raise Exception(msg)
            return ret
        # worklist expressions
        match = re.search(self._re['worklist'], val)
        if match:
            return self._render_worklist(argstr=match.group(1))
        # part expressions
        match = re.search(self._re['part'], val)
        if match:
            return self.render_part(argstr=match.group(1))
        if finfo and val in finfo:
            return finfo[val]
        if symbols and val in symbols:
            return symbols[val]
        # use either a passed worklist or the object member
        if worklist is None:
            worklist = self._default_worklist
        if worklist:
            # note that if list has >1 item, this will visit them
            # in arbitrary order. Also, we choose to only look at the
            # first default finfo if any.
            for key, work_item in worklist.items():
                if 'item_args' in work_item and val in work_item['item_args']:
                    return work_item['item_args'][val]
                if 'default' in work_item['roles'] and len(work_item['roles']['default']):
                    first_finfo = work_item['roles']['default'][0]
                    if val in first_finfo:
                        return first_finfo[val]
        return None

    # if you don't specify dest path and name, they're inferred.
    # if worklist is passed, it's treated as a symbol source for the page debracketing.
    def default_template_file_action(self, dir_name, file_name, dest_rel_path=None, dest_name=None):
        template_full = dir_name + '/' + file_name
        Config.log("default_template_file_action '%s'" % template_full, tag='DEFAULT_TEMPLATE_FILE_ACTION')
        if dest_name:
            rel_path = dest_rel_path
            dest_path = u.pathify(self.output_root, dest_rel_path)
        else:
            rel_path = u.make_rel_path(self.site_root, dir_name)
            dest_path = u.pathify(self.output_root, rel_path)
            dest_name = file_name
        u.ensure_path(dest_path)
        dest_full = u.pathify(dest_path, dest_name)
        info = {
            'name': dest_name,
            'path': dest_path,
            'rel_path': rel_path,
            'full': dest_full,
            'key': u.make_key(rel_path, dest_name)
        }
        if self.config.is_template_type(file_name):
            template = open(template_full).read()
            output = u.debracket(template, self.interpret)
            if not self.config.is_special_file(info['key']):
                open(dest_full, 'w').write(output)
                local = u.local_metadata(dest_path, dest_name)
                info['size'] = local['size']
                info['modified'] = local['modified']
                info['md5'] = u.md5(dest_full)
                self.track_file(info)
        else:
            shutil.copyfile(template_full, dest_full)
            local = u.local_metadata(dest_path, dest_name)
            info['size'] = local['size']
            info['modified'] = local['modified']
            info['md5'] = u.md5(dest_full)
            self.track_file(info)

    # unconditionally upload web assets to dest
    def upload_files(self):
        for info in self._files_created:
            key = info['key']
            if self.config.is_special_file(key):
                continue
            self.dest_mgr.upload_finfo(info)
            Config.log(key, tag='WEBMAKER_UPLOAD_OK')

    def render_special_file(self, file_tag):
        if not self.config.is_special_file(file_tag):
            raise Exception("invalid special file tag '%s'" % file_tag)
        if os.path.isfile(self.site_root + '/' + file_tag):
            self.default_template_file_action(self.site_root, file_tag)

    def _walk_templates(self, root_name):
        for dir_name, subdirs, files in os.walk(root_name):
            # logging.info("WebMaker visiting template folder %s" % dir_name)
            Config.log(dir_name, 'WEBMAKER_VISITING_TEMPLATE_FOLDER')
            for file_name in files:
                if not self.config.is_special_file(file_name):
                    self.default_template_file_action(dir_name, file_name)

    # make all web assets in output folder
    def process(self):
        del self._files_created[:]
        self.render_special_file('run_pre_web')
        self._walk_templates(self.site_root)
        self.upload_files()
        self._worklists.clear()

