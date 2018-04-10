__author__ = 'dsteinkraus'

import sys
import logging
from config import Config
from ftp_mgr import FtpMgr
from url_mgr import UrlMgr
from tree_processor import TreeProcessor
from webmaker import WebMaker
from file_dest import FileDest
from plugin import Plugin

#---------------- the program ----------------------------

if len(sys.argv) > 1:
    config_file = sys.argv[1]
else:
    print("no config file specified, quitting.")
    sys.exit(1)
config = Config(config_file)
if len(sys.argv) > 2:
    config.set_special_mode(sys.argv[2:])

try:
    config.plugin = Plugin()
    plugin_root = config.get('local', 'plugins', return_none=True)
    if plugin_root:
        logging.info("loading plugins from '%s'" % plugin_root)
        config.plugin.import_folder(plugin_root)
        logging.info("plugins loaded:\n%s" % config.plugin.list(pretty=True))
    else:
        raise Exception('plugins in [local] section not configured')

    # create and configure all objects.
    # build has markup so needs to be separate from both staging and prod.
    build_dest_mgr = None
    if config.get('build', 'bucket_name', return_none=True):
        S3 = config.plugin.get('S3')
        if not S3:
            raise Exception("S3 plugin needed, but not available")
        build_dest_mgr = S3(config, 'build')
    elif config.get('build', 'file_dest_root', return_none=True):
        build_dest_mgr = FileDest(config, 'build')

    staging_dest_mgr = None
    if config.get('staging', 'bucket_name', return_none=True):
        S3 = config.plugin.get('S3')
        if not S3:
            raise Exception("S3 plugin needed, but not available")
        staging_dest_mgr = S3(config, 'staging')
    elif config.get('staging', 'file_dest_root', return_none=True):
        staging_dest_mgr = FileDest(config, 'staging')

    production_dest_mgr = None
    if config.get('production', 'bucket_name', return_none=True):
        S3 = config.plugin.get('S3')
        if not S3:
            raise Exception("S3 plugin needed, but not available")
        production_dest_mgr = S3(config, 'production')
    elif config.get('production', 'file_dest_root', return_none=True):
        production_dest_mgr = FileDest(config, 'production')

    tp = TreeProcessor(config, dest_mgr=build_dest_mgr)
    wm = WebMaker(config, build_dest_mgr, track_file_callback=tp.track_file)
    input_mgrs = []
    if config.do('ftp'):
        ftp_sites = config.get_multi('input', 'servers', return_none=True, remove_quotes=True, split_commas=True)
        if ftp_sites:
            # for now only support one site. In future, use site label to find the site info matching each folder.
            ftp_info = ftp_sites[0]

            ftp_folders = config.get_multi('input', 'folders', remove_quotes=True, split_commas=True)
            for ftp_folder in ftp_folders:
                ftp = FtpMgr(
                    config,
                    label=ftp_folder[0],
                    site=ftp_info[1],
                    login=ftp_info[2],
                    password=ftp_info[3],
                    folder=ftp_folder[2],
                    dest_root=config.input + '/' + ftp_folder[0])  # use label as dest folder
                # note that keys here are full paths, so should be no collisions
                # between multiple ftp downloads (unless deliberate)
                input_mgrs.append(ftp)
        url_mgr = None
        url_patterns = config.get_multi('input', 'urls', return_none=True)
        if url_patterns:
            url_mgr = UrlMgr(config, url_patterns)
            input_mgrs.append(url_mgr)
    tp.input_mgrs = input_mgrs
    tp.web_maker = wm

    # handle special modes
    if config.in_special_mode():
        if config.do('ftp_catchup') or config.do('ftp_remove') or config.do('im_rerun') \
                or config.do('ftp_clear') or config.do('im_meta_from_local'):
            for im in input_mgrs:
                im.update_products()
            sys.exit(1)
        if config.do('restore_from_archive'):
            tp.restore_from_archive(config.special_mode_args[1], options={'verbose': True})
            sys.exit(1)
        if config.do('test_deploy_test'):
            deploy_cycle = config.plugin.get('deploy_cycle')
            if not staging_dest_mgr or not production_dest_mgr:
                raise Exception("can't run test_deploy_test without both staging and production dest_mgrs")
            # run test/deployment cycle
            deploy_cycle(config, build_dest_mgr, staging_dest_mgr, production_dest_mgr, args=config.special_mode_args)
            sys.exit(2)

    # normal mode
    work_done = True
    config.iteration = 1
    max_iter = config.get_int('actions', 'max_iterations', default=10)
    tp_do_clear_info = True
    # get initial deployed state. We will keep track locally for the rest of the run.
    build_dest_mgr.sync_tree_info()

    while work_done and config.iteration <= max_iter:
        work_done = False
        if config.do('ftp'):
            for im in input_mgrs:
                work_done = im.update_products() > 0 or work_done
        Config.log("work_done %s after im update (iter %i)" % (work_done, config.iteration), tag='WORK_DONE')

        if config.do('process'):
            work_done = tp.process(do_clear_info=tp_do_clear_info) or work_done
            tp_do_clear_info = False
            # if there is a nonempty run_post_tree file, render it, then
            # re-do tree processing to handle any new/changed files
            if config.do('web'):
                # apply dest rules to the dest tree
                wm.render_special_file('run_post_tree')
                tp.dest_process()
                tp.clear_first = False
                work_done = tp.process(do_clear_info=False) or work_done
        Config.log("work_done %s after tp (iter %i)" % (work_done, config.iteration), tag='WORK_DONE')

        if config.do('upload'):
            local_tree_meta = tp.file_info
            work_done = build_dest_mgr.upload_tree(config.output, local_tree_meta=local_tree_meta) > 0 or work_done
        Config.log("work_done %s after upload_tree (iter %i)" % (work_done, config.iteration), tag='WORK_DONE')

        if config.do('web'):
            # this is a no-op if we did upload
            build_dest_mgr.sync_tree_info()
            # build web assets and upload them
            wm.process()

        if config.do('archive'):
            tp.archive()

        build_dest_mgr.write_tree_info()
        tp.remove_unpacked_files()
        logging.info("completed iteration %i of %i" % (config.iteration, max_iter))
        config.iteration += 1

    if config.final_summary:
        logging.info("Run summary:\n%s" % config.final_summary)
    logging.info("%s run complete." % config.display_times())
except Exception as exc:
    Config.log(str(exc), tag='FLOE_OUTER_EXCEPTION')
    logging.exception(str(exc))
