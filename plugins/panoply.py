import logging
import os
import copy

import util as u
from config import Config

def register():
    return {'panoply': panoply}

# panoply uses magic numbers for output image size. given a width in pixels, return magic
# number that will return largest image <= that width.

def _panoply_size_to_size_factor(size):
    if size:
        if size <= 592:
            return '80' # extra small
        if size <= 736:
            return '100' # small
        if size <= 880:
            return '120' # standard
        if size <= 1024:
            return '140' # large
        if size <= 1168:
            return '160' # extra large
        if size <= 1328:
            return '180' # jumbo
        if size <= 1472:
            return '200' # super jumbo
        if size <= 1760:
            return '250' # king
        if size <= 2192:
            return '300' # maximum
        Config.log(str(size), tag='PANOPLY_size_too_big')
    return '140'

def panoply(interp, finfo, argstr):
    Config.log("file '%s' argstr '%s'" % (finfo['full'], argstr), tag='PANOPLY')
    try:
        interpret = u.interpret_method(interp)
        argstr = u.debracket(argstr, interpret, finfo=finfo)
        args = u.parse_arg_string(argstr)
        if not 'action' in args:
            raise Exception("panoply: 'action arg is required")
        if not 'dest' in args:
            raise Exception("panoply: dest arg is required")
        action = args['action']
        dest = u.debracket(args['dest'], interpret, finfo=finfo)
        panoply_templates = Config.main.template_root + '/panoply'
        src = finfo['full']
        if not src.endswith('.nc'):
            logging.error("panoply command: '%s' is not a dataset file" % src)
            return

        ### TODO more copied stuff from copy_with_metadata!
        (dest_dir, dest_file) = os.path.split(dest)
        u.ensure_path(dest_dir)
        jar = 'PanoplyCL.jar'

        size = None
        if 'size' in args:
            size = int(args['size'])
        size_factor = _panoply_size_to_size_factor(size)

        template_file = panoply_templates + '/' + action + '.pclt'
        try:
            template = open(template_file).read()
        except Exception as exc:
            logging.error("panoply command: error '%s' opening template file '%s" %
                          (str(exc), template_file))
            raise
        symbols = {
            'dataset': src,
            'output_file': dest,
            'size_factor': size_factor
        }
        script = u.debracket(template, interpret, symbols=symbols)
        script_path = Config.main.output + '/tmp'
        u.ensure_path(script_path)
        script_file = script_path + '/' + action + '.pcl'
        open(script_file, 'w').write(script)

        working_dir = Config.main.get('tools', 'panoply_workdir')
        if not os.path.isdir(working_dir):
            err = "panoply: invalid panoply_workdir '%s'" % working_dir
            logging.error(err)
            raise Exception(err)

        call_args = ['java', '-jar', jar, script_file]
        (returncode, stdout, stderr) = u.run_command(call_args, working_dir)
        logging.debug("returncode: %s\nstdout: %s\nstderr: %s" %
                      (returncode, stdout, stderr))
        if returncode == 0:
            # we know the file that was created, so make its metadata now
            newfi = {}
            newfi['name'] = dest_file
            newfi['path'] = dest_dir
            newfi['full'] = dest
            newfi['rules_run'] = False
            tmp = u.local_metadata(newfi['path'], newfi['name'])
            newfi['size'] = tmp['size']
            newfi['modified'] = tmp['modified']
            return {'new_finfo': newfi}
        else:
            logging.error(
                "panoply failed with rc '%i', stderr = '%s'" % (returncode, stderr))
    except Exception as exc:
        # py2 logging.error("panoply exception '%s'" % exc.message)
        logging.error("panoply exception '%s'" % str(exc))

