import logging
import os

import util as u
from config import Config

def register():
    return {
        'render_mode_hybrid_forecasts': make_hybrid_forecast
    }


def make_hybrid_forecast(webmaker, dest_key, work_item, list_args):
    try:
        Config.log("dest_key = '%s'" % dest_key, tag='HYBRID_FORECAST_START_' + dest_key)
        (dest_path, dest_name) = os.path.split(dest_key)
        finfos = work_item['roles']['default']
        if not len(finfos):
            msg = "logic error, no finfos on default list for dest_key '%s'" % dest_key
            raise Exception(msg)
        archive_root = webmaker.config.get('local', 'archive', return_none=True)
        if not archive_root:
            logging.error("make_hybrid_forecast: fail, no archive_root configured")
            return 'todo error1'

        local_store_start = u.datestr_to_daynum(webmaker.config.get('local', 'local_store_start'))
        history_days = webmaker.config.get_int('tools', 'hybrid_forecast_history_days', default=20)
        history_frame0s = []

        extract_frames = webmaker.config.plugin.get('extract_frames')
        combine_frames = webmaker.config.plugin.get('combine_frames')
        get_frame_count = webmaker.config.plugin.get('get_frame_count')

        # we need the current image for forecast frames, should be finfo
        current_image = finfos[0]['full']
        if not current_image or not os.path.isfile(current_image):
            msg = "make_hybrid_forecast can't find expected current_image '%s'" % current_image
            logging.error(msg)
            return 'todo error2'
        frame_ct = get_frame_count(current_image)
        if frame_ct < 2:
            msg = "make_hybrid_forecast: current_image '%s' not multi-frame, taking no action" % current_image
            logging.warning(msg)
            return ''
        logging.info("got current_image file '%s'" % current_image)
        (current_path, current_name) = os.path.split(current_image)

        # look for frame0 files first in output, then archive
        roots = [webmaker.config.output + '/static', archive_root + '/output/static']

        # find history files going back from date of current file
        if 'item_args' in work_item and 'date_string' in work_item['item_args']:
            date_str = work_item['item_args']['date_string']
        else:
            msg = "make_hybrid_forecast can't find date_string"
            logging.error(msg)
            return 'todo error3'

        testdate = u.datestr_to_daynum(date_str) - 1
        while True:
            date_str = u.daynum_to_datestr(testdate)
            # TODO what if it isn't a gif? also hard-coded convention
            fname = u.find_in_paths(
                current_name + '_frame_00000.gif',
                'esrl-daily-forecasts/' + date_str + '/frame0',
                roots,
                options={'full': True}
            )
            if fname:
                # logging.info("got frame0 file '%s'" % fname)
                Config.log(fname, tag='HYBRID_FORECAST_FRAME_GOT0')
                history_frame0s.insert(0, fname)
            else:
                Config.log("frames not contiguous by date, missing '%s'" % date_str, 'HYBRID_FORECAST_FRAME_ERR')
            if len(history_frame0s) >= history_days:
                break
            testdate -= 1
            if testdate < local_store_start:
                msg = "make_hybrid_forecast only found %i history frames" % len(history_frame0s)
                break
        hist_frame_ct = len(history_frame0s)
        if not hist_frame_ct:
            msg = "key '%s': no history frames, can't proceed" % dest_key
            Config.log(msg, tag='HYBRID_FORECAST_ERROR')
            webmaker.config.add_to_final_summary(msg)
            return 'todo error4'
        # extract frames from current image
        work_path = webmaker.config.output + '/tmp/' + dest_name
        u.ensure_path(work_path)
        extract_frames(webmaker.interpret, finfos[0], 'out_dir:' + work_path)

        # make list of frame files for new image (TODO review path of tmp file)
        flist = work_path + '/' + dest_name + '.frames'
        content = "\n".join(history_frame0s)
        # add forecast frames
        for frame_num in range(1, frame_ct - 1):
            fname = work_path + '/' + current_name + '_frame_' + '{:05d}'.format(frame_num) + '.gif'
            content += '\n' + fname
        open(flist, 'w').write(content)
        # make output animation
        if 'delay' in list_args:
            delay = list_args['delay']
        else:
            delay = webmaker.config.get('tools', 'default_frame_delay', return_none=True)
        combine_frames(flist, webmaker.output_root + '/' + dest_path, dest_name, delay=delay)
        # TODO use utility method??
        display_name = dest_name
        if 'display_name' in finfos[0]:
            display_name = finfos[0]['display_name']
        item_args = work_item['item_args']
        if 'display_name' in item_args:
            display_name = u.parse_subargs(
                item_args['display_name'],
                webmaker.interpret,
                webmaker.config,
                finfos[0]
            )
        # TODO REVIEW: we do this for panoply, should we here also?
        info = {
            'name': dest_name,
            'path': webmaker.output_root + '/' + dest_path,
            'rel_path': dest_path,
            'full': webmaker.output_root + '/' + dest_key,
            'key': dest_key,
            'display_name': display_name
        }
        tmp = u.local_metadata(info['path'], info['name'])
        info['size'] = tmp['size']
        info['modified'] = tmp['modified']
        webmaker.track_file(info)
        work_item['roles']['output'] = info
        os.remove(flist)
        Config.log("key '%s'" % dest_key, tag='HYBRID_FORECAST_SUCCESS')
        # TODO review - returning empty string because invoking from html
        return ""
    except Exception as exc:
        Config.log(str(exc), tag='HYBRID_FORECAST_EXCEPTION')
        raise
