import logging
import os
import copy

import util as u
from config import Config

def register():
    return {
        'get_frame_count': get_frame_count,
        'extract_frames': extract_frames,
        'combine_frames': combine_frames,
        'scale_and_copy': scale_and_copy,
        'make_thumb': make_thumb,
        'captionize': captionize
    }

# helper to get frame count for a possibly animated file (e.g. .gif).
# this assumes that ImageMagick's 'identify' is on the path
# TODO - apparently IM's 'identify' examines every frame, gets slow on large animations.
# look for a faster method if necessary.
def get_frame_count(full):
    # identify -format %n  posible_animation.gif
    Config.log(full, tag='IM_GET_FRAME_COUNT')
    try:
        runargs = ['identify', '-format', '%n', full]
        (returncode, stdout, stderr) = u.run_command(runargs)
        # logging.debug("identify returncode: %s\nstdout: %s\nstderr: %s" % (returncode, stdout, stderr))
        if returncode == 0:
            tmp = stdout.decode('utf-8').strip()
            return int(tmp)
        else:
            logging.error("get_frame_count failed with rc %i, stderr = '%s'" % (returncode, stderr))
    except Exception as exc:
        logging.error("get_frame_count exception '%s'" % str(exc))
        raise

# use non-verbose identify to grab width, height (maybe more later)
def get_image_size(full):
    Config.log(full, tag='IM_GET_IMAGE_SIZE')
    try:
        # [0] specifies first frame, no harm if not multiframe
        runargs = ['identify', '-format', '%w %h', full + '[0]']
        (returncode, stdout, stderr) = u.run_command(runargs)
        # logging.debug("identify returncode: %s\nstdout: %s\nstderr: %s" % (returncode, stdout, stderr))
        if returncode == 0:
            return stdout.decode('utf-8').strip().split(' ')
        else:
            msg = "rc = %i, stderr = '%s'" % (returncode, stderr)
            raise Exception(msg)
    except Exception as exc:
        Config.log(str(exc), tag='IM_GET_IMAGE_SIZE_ERROR')
        raise

# internal helper. return True on success
def _extract_frames(src, dest_dir):
    try:
        u.ensure_path(dest_dir)
        (src_path, src_name) = os.path.split(src)
        destspec = dest_dir + '/' + src_name + '_frame_%05d.gif'
        runargs = ['convert', '-coalesce', src, destspec]
        (returncode, stdout, stderr) = u.run_command(runargs)
        logging.debug("convert returncode: %s\nstdout: %s\nstderr: %s" % (returncode, stdout, stderr))
        if returncode == 0:
            return True
        else:
            logging.error("get_frame_count failed with rc %i, stderr = '%s'" % (returncode, stderr))
    except Exception as exc:
        logging.error("_extract_frames src '%s' exception '%s'" % (src, str(exc)))
        raise

# combine frames into new animated gif
def combine_frames(frame_list, dest_dir, dest_name, delay=None):
    try:
        u.ensure_path(dest_dir)
        runargs = ['convert']
        if delay is not None:
            runargs = runargs + ['-delay', delay]
        runargs = runargs + ['@' + frame_list, '-loop', '0', dest_dir + '/' + dest_name]
        (returncode, stdout, stderr) = u.run_command(runargs)
        logging.debug("convert returncode: %s\nstdout: %s\nstderr: %s" % (returncode, stdout, stderr))
        if returncode == 0:
            return True
        else:
            logging.error("combine_frames failed with rc %i, stderr = '%s'" % (returncode, stderr))
    except Exception as exc:
        logging.error("combine_frames frame_list '%s' exception '%s'" % (frame_list, str(exc)))
        raise

def scale_and_copy(interp, finfo, argstr):
    try:
        interpret = u.interpret_method(interp)
        argstr = u.debracket(argstr, interpret, finfo=finfo)
        args = u.parse_arg_string(argstr)
        src = finfo['full']
        Config.log("src '%s', argstr '%s'" % (src, argstr), tag='SCALE_AND_COPY')
        # image magick infers format from extension
        if not u.have_required(args, 'dest', 'size'):
            raise Exception("scale_and_copy incomplete args '%s'" % argstr)
        if 'larger_dim' in args:
            # TODO: this alternative to 'size' requires getting dims of original
            raise Exception("scale_and_copy: 'larger_dim' not yet supported")
        dest = args['dest']
        size = int(args['size'])
        size_str = "%ix%i" % (size, size)
        # to convert only first frame of animated gif, specify 'file[0]'
        if 'single_frame' in args and args['single_frame']:
            src += '[0]'
        ### TODO more copied stuff from copy_with_metadata!
        (dest_dir, dest_file) = os.path.split(dest)
        u.ensure_path(dest_dir)
        call_args = ['convert', src, '-resize', size_str, dest]
        (returncode, stdout, stderr) = u.run_command(call_args)
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
            # add thumb dimensions to metadata
            newfi['width'], newfi['height'] = get_image_size(dest)
            return {'new_finfo': newfi}
        else:
            logging.error("scale_and_copy failed with rc %i, stderr = '%s'" % (returncode, stderr))
            return {}
    except Exception as exc:
        logging.error("scale_and_copy exception '%s'" % str(exc))
        return {}

# make a thumbnail of an image. For now, the main point of this func is to prevent
# infinite loop of thumbnails (which can also be adressed by rule tweaks, but
# this is handy).
# TODO! if animated, offer option only include frame N (default 0)
def make_thumb(interp, finfo, argstr):
    msg = None
    if 'parent_full' in finfo:
        msg = "file '%s' has parent_full, won't make thumb of thumb." % finfo['full']
    if msg:
        Config.log(msg, tag='MAKE_THUMB_NORECURSE')
        return
    ret = scale_and_copy(interp, finfo, argstr)
    if 'new_finfo' not in ret:
        return None
    # note that if you make multiple thumbs from one image (why?) this will
    # only track the latest one.
    finfo['thumb_full'] = ret['new_finfo']['full']
    # we have changed the finfo of the parent image, so we need to
    # make sure that's tracked
    ret['source_changed'] = True
    Config.log(ret['new_finfo']['full'], tag='MAKE_THUMB_OK')
    return ret

# given a multi-frame animation (e.g. gif), extract the frames to individual
# image files in /tmp. Attach metadata for these images to finfo.
# CAUTION: this does not make finfos for the new frame files.
def extract_frames(interp, finfo, argstr):
    try:
        interpret = u.interpret_method(interp)
        argstr = u.debracket(argstr, interpret, finfo=finfo)
        args = u.parse_arg_string(argstr)
        src = finfo['full']
        Config.log(src, tag='EXTRACT_FRAMES_CALLED')
        src_name = finfo['name']
        frame_ct = get_frame_count(src)
        if frame_ct < 2:
            msg = "extract_frames: '%s' not multi-frame, taking no action" % src
            logging.warning(msg)
            return frame_ct
        if not 'out_dir' in args:
            raise Exception("out_dir not specified in args '%s'" % argstr)
        dest = args['out_dir']
        logging.debug("extract_frames dest is %s" % dest)
        if _extract_frames(src, dest):
            raw_files = u.plain_files(dest, src_name + '_frame_(\d+)\.gif')
            if 'frames_wanted' in args:
                wanted = args['frames_wanted']
                # TODO support more syntax, 1..max, 1..4, 0, max-10..max, etc.
                # for now only support '0'
                if wanted != '0':
                    raise Exception("unsupported 'frames_wanted' spec '%s'" % wanted)
                # todo - crude version for frame 0 support
                tag = '00000'
                the_files = []
                for file in raw_files:
                    if tag in file:
                        the_files.append(file)
                    else:
                        os.remove(dest + '/' + file)
            else:
                the_files = raw_files

            # save some metadata
            finfo['frames_path'] = dest
            finfo['frames_files'] = the_files
            finfo['frames_count'] = len(the_files)
            msg = "src '%s', dest '%s', count '%s'" % (src, dest, finfo['frames_count'])
            Config.log(msg, tag='EXTRACT_FRAMES_OK')
            return finfo['frames_count']
    except Exception as exc:
        logging.error("extract_frames exception '%s'" % str(exc))

# todo - look for ways to reduce dupe code
def captionize(interp, finfo, argstr):
    try:
        interpret = u.interpret_method(interp)
        argstr = u.debracket(argstr, interpret, finfo=finfo)
        args = u.parse_arg_string(argstr)
        src = finfo['full']
        Config.log("src '%s', argstr '%s'" % (src, argstr), tag='CAPTIONIZE')
        if not u.have_required(args, 'dest', 'where', 'font_size', 'bar_size', 'pad_x', 'pad_y', 'text'):
            raise Exception("captionize incomplete args '%s'" % argstr)
        dest = args['dest']
        (dest_dir, dest_file) = os.path.split(dest)
        u.ensure_path(dest_dir)
        params = []
        if args['where'] == 'top':
            params.extend(['-gravity', 'northwest'])
        elif args['where'] == 'bottom':
            params.extend(['-gravity', 'southwest'])
        else:
            raise Exception("captionize invalid 'where' arg in " + argstr)
        # TODO: validate colors. See https://www.imagemagick.org/script/color.php
        if 'background_color' in args:
            params.extend(['-background', args['background_color']])
        else:
            params.extend(['-background', 'white'])
        if 'text_color' in args:
            params.extend(['-fill', args['text_color']])
        else:
            params.extend(['-fill', 'black'])
        # TODO: validate font name. "convert -list font" will list them. system dependent.
        if 'font' in args:
            params.extend(['-font', args['font']])
        else:
            params.extend(['-font', 'Helvetica'])
        params.extend(['-pointsize', args['font_size']])
        params.extend(['-splice', '0x' + args['bar_size']])
        x = u.force_sign(args['pad_x'])
        y = u.force_sign(args['pad_y'])
        params.extend(['-annotate', x + y])
        fixed_text = args['text'].replace("'", "\\'")
        params.append('"' + fixed_text + '"')

        call_args = ['convert', src] + params + [ dest]
        (returncode, stdout, stderr) = u.run_command(call_args)
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
            logging.error("captionize failed with rc %i, stderr = '%s'" % (returncode, stderr))
    except Exception as exc:
        logging.error("captionize exception '%s'" % str(exc))
