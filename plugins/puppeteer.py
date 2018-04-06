import logging
import os
import copy

import util as u
from config import Config

def register():
    return {
        'get_screenshot': get_screenshot
    }

def get_screenshot(interp, finfo, argstr):
    try:
        interpret = u.interpret_method(interp)
        argstr = u.debracket(argstr, interpret, finfo=finfo)
        args = u.parse_arg_string(argstr)
        Config.log(argstr, tag='GET_SCREENSHOT')
        if not u.have_required(args, 'url', 'dest', 'height', 'width'):
            raise Exception("get_screenshot incomplete args '%s'" % argstr)
        dest = u.debracket(args['dest'], interpret, finfo=finfo)
        (dest_dir, dest_file) = os.path.split(dest)
        u.ensure_path(dest_dir)
        puppeteer_templates = Config.main.template_root + '/puppeteer'
        template_file = puppeteer_templates + '/get_screenshot.js'
        try:
            template = open(template_file).read()
        except Exception as exc:
            Config.log("'%s' opening template file '%s" %
                          (str(exc), template_file), tag='GET_SCREENSHOT_ERROR')
            raise
        symbols = {
            'url': args['url'],
            'width': args['width'],
            'height': args['height'],
            'dest': dest
        }
        script = u.debracket(template, interpret, symbols=symbols)
        script_path = Config.main.output + '/tmp'
        u.ensure_path(script_path)
        script_file = script_path + '/get_screenshot.js'
        open(script_file, 'w').write(script)

        working_dir = Config.main.get('tools', 'node_workdir')
        if not os.path.isdir(working_dir):
            err = "get_screenshot: invalid node_workdir '%s'" % working_dir
            logging.error(err)
            raise Exception(err)

        call_args = ['node', script_file]
        (returncode, stdout, stderr) = u.run_command(
            call_args, working_dir)
        # logging.debug("returncode: %s\nstdout: %s\nstderr: %s" % (returncode, stdout, stderr))
        if returncode == 0:
            # we know the file that was created, so make its metadata now
            newfi = {}
            newfi['parent_full'] = finfo['full'] # provenance
            newfi['name'] = dest_file
            newfi['path'] = dest_dir
            newfi['full'] = dest
            newfi['rules_run'] = False
            newfi.pop('groups', None)
            return {'new_finfo': newfi}
        else:
            Config.log("rc %i, stderr = '%s'" % (returncode, stderr),
                tag='GET_SCREENSHOT_ERROR')
    except Exception as exc:
        Config.log(str(exc), tag='GET_SCREENSHOT_ERROR')

