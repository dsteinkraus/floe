import logging
import os
#import faulthandler
import netCDF4 as nc

import util as u
from config import Config

logger = logging.getLogger(__name__)

def register():
    return {
        'smoke_test': smoke_test
    }

def smoke_test(interp, finfo, argstr):
    try:
        logger.info('I do not think this will work')
        # REVIEW: faulthandler not safe in apache, assumes stdout available
        #faulthandler.enable()
        interpret = u.interpret_method(interp)
        argstr = u.debracket(argstr, interpret, finfo=finfo)
        args = u.parse_arg_string(argstr)
        #Config.log(argstr, tag='NETCDF_SMOKE')
        #if not u.have_required(args, 'dest'):
        #    raise Exception("get_screenshot incomplete args '%s'" % argstr)
        #dest = u.debracket(args['dest'], interpret, finfo=finfo)
        #(dest_dir, dest_file) = os.path.split(dest)
        #u.ensure_path(dest_dir)
        full = finfo['full']
        if not os.path.exists(full):
            # TODO config logging not working here?
            Config.log(full + " not found", tag='NETCDF_BAD_ARG')
            logger.info(full + ' not found')
            return 'todo file not found ' + full
        Config.log("here goes nc.Dataset with " + full)
        rootgrp = nc.Dataset(finfo['full'], 'r', format='NETCDF4')
        ret = 'variables:\n' + str(rootgrp.variables)
        rootgrp.close()
        return ret
    except Exception as exc:
        Config.log(str(exc), tag='NETCDF_SMOKE_ERROR')
        return str(exc)

