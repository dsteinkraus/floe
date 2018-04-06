import logging
import os
import copy

import util as u
from config import Config

def register():
    return {
        'web_handle': web_handle,
        'set_file_info': set_file_info
    }

# save file information for use in building web pages or other assets.
def web_handle(tp, finfo, argstr):
    # bracket notation allowed in args
    argstr = u.debracket(argstr, tp.interpret, finfo=finfo)
    # logging.info("web_handle called, args = '%s'" % argstr)
    Config.log("args = '%s'" % argstr, tag='WEB_HANDLE_CALLED')
    tp.web_maker.add_file(finfo, argstr)

def is_protected_metadata(key):
    if key == 'full' or key == 'path' or key == 'name' or key == 'rules_run' or key == 'stop':
        return True
    if key == 'groups' or key == 'rules_run' or key == 'source_im':
        return True
    return False

# add file metadata. argstr might be:
# display_name: "[sym: daily, key: [name]]"
def set_file_info(tp, finfo, argstr):
    try:
        logging.info("set_file_info called, args = '%s'" % argstr)
        # this has its own peculiar syntax, so don't blindly debracket
        # argstr = u.debracket(argstr, tp.interpret, finfo=finfo)
        args = u.parse_arg_string(argstr)
        for key, value in args.items():
            if is_protected_metadata(key) or not len(key):
                logging.warning("metadata key '%s' is protected or invalid" % key)
                continue
            if value.startswith('"'):
                value = value.strip('"')
            if value.startswith('['):
                finfo[key] = u.parse_subargs(value, tp.interpret, tp.config, finfo=finfo)
            else:
                finfo[key] = value
    except Exception as exc:
        logging.error("set_file_info exception '%s'" % str(exc))
