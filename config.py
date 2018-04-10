__author__ = 'dsteinkraus'

import os
import sys
import re
import configparser
import logging
import json
import time
from datetime import datetime

import util as u


#===================== class Config =========================================================

class Config(object):
    main = None # singleton instance

    @classmethod
    def log(cls, message, tag=None):
        if not Config.main:
            raise Exception("Config.main not set")
        Config.main.ilog(message, tag)

    def __init__(self, configFile):
        Config.main = self # singleton
        self.configFile = configFile
        self._config = None
        self.plugin = None
        self.start_time = datetime.utcnow()
        self.special_mode_args = []
        self.final_summary = ''
        self.iteration = None
        self._re = {}
        self._re['n'] = re.compile(r'\n+')
        self._re['symbol'] = re.compile(r'\s*=\s*')
        self._re['comment'] = re.compile(r'^\s*(#.*)?$')
        self.symbols = {}
        self._rules_mask = {}
        self._debug_tags = {}
        self._template_extensions = {}
        self.log_to_console = False
        # special modes available on command line, and info about them
        self._special_modes = {
            'ftp_catchup' : {},
            'ftp_remove': {},
            'im_rerun': {},
            'ftp_clear': {},
            'im_meta_from_local': {},
            'restore_from_archive': {},
            'test_deploy_test': {}
        }
        # TODO allow override via config
        self._special_file_tags = {
            'run_post_tree': True,
            'run_pre_web': True
        }

        if configFile:
            self.load_config()
        self.input = self.get('local', 'input')
        u.ensure_path(self.input)
        self.output = self.get('local', 'output')
        u.ensure_path(self.output)
        self.admin = self.get('local', 'admin')
        u.ensure_path(self.admin)
        self.archive = self.get('local', 'archive')
        u.ensure_path(self.archive)

        # template root is where all templates, of all types, are stored
        self.template_root = self.get('process', 'template_root', return_none=True)
        if self.template_root is None:
            self.template_root = self.admin + '/templates'

        # Config is in charge of logging options
        loglevel = logging.INFO
        self._logfile = self.get('local', 'logfile', return_none=True)
        if self._logfile:
            log_dir = self.admin + '/logs'
            u.ensure_path(log_dir)
            full = log_dir + '/' + self._logfile
            u.age_file(full, log_dir, move=True)
            # see https://stackoverflow.com/questions/1943747/python-logging-before-you-run-logging-basicconfig:
            # if someone tried to log something before basicConfig is called, Python creates a default handler that
            # goes to the console and will ignore further basicConfig calls. Remove the handler if there is one.
            root = logging.getLogger()
            if root.handlers:
                for handler in root.handlers:
                    root.removeHandler(handler)
            logging.basicConfig(
                filemode='w',
                filename=full,
                format='%(asctime)s %(message)s',
                datefmt='(%b %d %Y %H:%M:%S)',
                level=loglevel)
        logging.info("starting at %s" % self.display_times(self.start_time))
        logging.info("configFile is %s" % self.configFile)
        logging.info("working directory is %s" % os.getcwd())
        if self.is_true('local', 'log_config', absent_means_no=True):
            self.log_config()
        self._load_symbols()
        self._load_rules_to_run()
        self._load_debug_tags()
        self._load_template_extensions()
        self.log_to_console = self.is_true('local', 'log_to_console', absent_means_no=True)

        # allow user to bail out on run by creating a signal file
        self._signal_file = self.get('actions', 'signal_file', return_none=True)
        if self._signal_file:
            logging.info("will watch for signal file '%s'" % self._signal_file)

        logging.info("sys.path:\n" + '\n'.join([p for p in sys.path]))

    def signalled(self):
        return self._signal_file and os.path.isfile(self._signal_file)

    def load_config(self):
        print("configFile is %s" % self.configFile)
        self._config = configparser.RawConfigParser(allow_no_value=True)
        tmp_fh = u.process_includes(self.configFile)
        self._config.read(tmp_fh.name)
        tmp_fh.close()

    def _load_symbols(self):
        for name in self._config.options('symbols'):
            if name == 'skip_if_no_symbols':
                continue  # this is a regular option, not a symbol file
            if not name in self.symbols:
                self.symbols[name] = {}
            value = self.get('symbols', name)
            if value is None:
                value = name
            if value.endswith('.py'):
                raise Exception("TODO handle .py symbol files")
            sym_fname = self.admin + '/symbols/' + value
            try:
                with open(sym_fname) as fh:
                    for line in fh:
                        if self._re['comment'].search(line):
                            continue
                        ar = re.split(self._re['symbol'], line.rstrip(), 1)
                        if len(ar) == 2:
                            if len(ar[0]):
                                # value can be plain string, or JSON
                                if ar[1].startswith('{'):
                                    self.symbols[name][ar[0]] = json.loads(ar[1])
                                else:
                                    self.symbols[name][ar[0]] = ar[1]
                        else:
                            logging.error("bad line '%s' in symbol file '%s', ignoring" % (line, sym_fname))
            except Exception as exc:
                logging.error("error reading symbol file '%s', ignoring" % sym_fname)
                continue

    def _load_rules_to_run(self):
        self._rules_mask.clear()
        val = self.get('actions', 'rules_to_run', return_none=True)
        if val:
            for rule_name in u.comma_split(val):
                self._rules_mask[rule_name] = True

    def _load_debug_tags(self):
        self._debug_tags.clear()
        tags = self.get_multi('local', 'debug_tags')
        for tag in tags:
            self._debug_tags[tag] = True

    def _load_template_extensions(self):
        self._template_extensions.clear()
        exts = self.get_multi('process', 'template_extensions', remove_quotes=True, return_none=True)
        if exts:
            exts = ['.' + ext if len(ext) else ext for ext in exts]
        else:
            # use defaults (web-centric)
            exts = ('', '.html', '.css', '.js', 'txt', '.json')
        for ext in exts:
            self._template_extensions[ext.lower()] = True

    def have_rule_list(self):
        return self._rules_mask

    def rule_on_list(self, rule_name):
        return self._rules_mask and rule_name in self._rules_mask

    def run_rule(self, rule):
        if (rule['run_by_default'] and not self.have_rule_list()) or self.rule_on_list(rule['label']):
            return True
        return False

    # run program in a special mode
    def set_special_mode(self, args):
        del self.special_mode_args[:]
        mode = args[0].lower()
        if mode not in self._special_modes:
            err = "special mode '%s' not supported" % mode
            logging.error(err)
            raise Exception(err)
        self.special_mode_args = list(args)
        self.ilog(' '.join(self.special_mode_args), tag='CONFIG_SPECIAL_MODE_SET')

    def in_special_mode(self):
        return len(self.special_mode_args) > 0

    # check special mode and [actions] section - the latter default to true
    def do(self, mode):
        if len(self.special_mode_args) > 0 and self.special_mode_args[0] == mode:
            return True
        if mode not in ['ftp', 'process', 'upload', 'web', 'archive']:
            return False
        return self.is_true('actions', mode, absent_means_yes=True)

    # for config item that is a multi-line list of items, split it on newlines,
    # and optionally remove quotes. return list. optionally, further
    # split each line on commas.
    # TODO - about ready for an options object.
    def get_multi(self, section, option, return_none = False, remove_quotes=False, split_commas=False):
        val = None
        try:
            val = self._config.get(section, option)
        except configparser.NoOptionError:
            if return_none:
                return None
            raise
        if not val:
            return None
        ret = filter(bool, self._re['n'].split(val))
        ret = [x.strip() for x in ret if not x.startswith('#')]
        if remove_quotes:
            ret = [x.strip('"') for x in ret]
        if split_commas:
            ret = [u.comma_split(x) for x in ret]
        return ret

    def get(self, section, option, return_none=False):
        try:
            val = self._config.get(section, option)
        except configparser.NoOptionError:
            if return_none:
                return None
            raise
        except KeyError:
            if return_none:
                return None
            raise
        except configparser.NoSectionError:
            if return_none:
                return None
            raise
        return val

    # case insensitive bool check
    def is_true(self, section, option, absent_means_no=False, absent_means_yes=False):
        val = self.get(section, option, return_none=True)
        if absent_means_no and not val:
            return False
        if absent_means_yes and not val:
            return True
        return val.lower() == 'true'

    def get_int(self, section, option, default=None):
        val = self.get(section, option, return_none=True)
        if val is None:
            if default is not None:
                return default
            err = "section '%s', option '%s': not present and no default" % (section, option)
            logging.critical(err)
            raise Exception(err)
        try:
            return int(val)
        except Exception as exc:
            err = "'%s' not a valid integer" % val
        logging.critical(err)
        raise Exception(err)

    # return formatted UTC and local time string (not for displaying browser's
    # local time, use browser_times for that)
    def display_times(self, date_time=None):
        if not date_time:
            date_time = datetime.utcnow()
        disp_utc = date_time.strftime("%b %d %Y %H:%M:%S")
        disp_local = u.utc_to_local(date_time).strftime("%b %d %Y %H:%M:%S")
        local_tzone_name = time.tzname[time.localtime().tm_isdst]
        return "%s UTC (%s %s)" % (disp_utc, disp_local, local_tzone_name)

    def browser_times(self, date_time):
        raise Exception('todo')

    def log_config(self):
        lines = []
        lines.append("config contents:")
        for section in self._config.sections():
            lines.append("Section: %s" % section)
            for options in self._config.options(section):
                lines.append("\t%s = %s" % (options,
                                          self._config.get(section, options)))
        logging.info("\n".join(lines))
        logging.info("-----------------")

    def add_to_final_summary(self, msg):
        if self.iteration is not None:
            self.final_summary += "(iter %i)" % self.iteration
        self.final_summary += msg + '\n'

    # REVIEW - this matches prefixes so you can specify multiple
    # tags you want with one line in config. Could be extended to
    # regexes, include/exclude rules, etc. if needed.
    def want_log(self, tag):
        if 'ALL' in self._debug_tags:
            return True
        for tag_prefix in self._debug_tags:
            if tag.startswith(tag_prefix):
                return True
        return False

    # most logging is done via class method. Only need this if you have
    # multiple config instances.
    def ilog(self, message, tag=None):
        if tag is None:
            logging.info(message)
            if self.log_to_console:
                print(message)
        elif self.want_log(tag):
            msg = "<%s> %s" % (tag, message)
            logging.info(msg)
            if self.log_to_console:
                print(msg)

    # return true if extension indicates file that can be templatized
    def is_template_type(self, file_name):
        _, ext = os.path.splitext(file_name)
        return ext.lower() in self._template_extensions

    def attach_symbols(self, symbol_set_name, symbol_key, dict):
        if symbol_set_name not in self.symbols or symbol_key not in self.symbols[symbol_set_name]:
            self.ilog("'%s' %s'" % (symbol_set_name, symbol_key), tag='SYMBOLS_not_found')
            return False
        symbols = self.symbols[symbol_set_name][symbol_key]
        # TODO: python or jetbrains bug? this returns false even
        # when symbols is plainly a dict object.
        # if symbols is None or not (type(symbols) is dict):
        if symbols is None:
            raise Exception("should never happen")
        for key, val in symbols.items():
            dict[key] = str(val)
        return True

    def is_special_file(self, file_tag):
        return file_tag in self._special_file_tags

    def default_page_wl_name(self):
        ret = self.get('process', 'default_page_wl_name', return_none=True)
        if not ret:
            ret = 'page_wl'
        return ret