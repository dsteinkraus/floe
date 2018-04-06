import logging
import re
import json

import util as u
from config import Config

def register():
    return {
        'deploy_cycle': deploy_cycle,
        'mini_tests': mini_tests
    }

# deployment cycle. First, copy (with fixups) the output tree (source_root) to staging
# server and run acceptance tests. If passed, copy (with fixups) the output
# tree to production server and run same tests there.

def deploy_cycle(
        config,
        build_dest_mgr,
        staging_dest_mgr,
        production_dest_mgr,
        args=None):
    test_suite = config.get('test', 'test_suite')
    tester = config.plugin.get(test_suite)
    testargs = {}

    no_deploy = args and len(args) >= 2 and args[1] == 'no_deploy'
    force_deploy = args and len(args) >= 2 and args[1] == 'force_deploy'

    def file_filter(file_name):
        # todo review - multi uses for this fn
        return config.is_template_type(file_name)

    try:
        Config.log('', tag='TDT_STAGING_TEST_START')

        # deploy to staging and test.

        def fixer_staging(full):
            # TODO should put fixups in the config instead of hardcoding
            find_expr = '_STATIC_SERVER_'
            repl_expr = config.get('staging', 'static_server')
            u.file_search_replace(full, re.compile(find_expr), repl_expr)

        staging_dest_mgr.sync_to_upstream_dest_mgr(
            build_dest_mgr,
            refresh_me='smart',
            refresh_upstream='smart',
            tmp_folder=config.get('staging', 'temp_file_root'),
            fixer=fixer_staging)

        testargs = {}
        testargs['static_server'] = config.get('staging', 'static_server')
        testargs['dynamic_server'] = config.get('staging', 'dynamic_server')
        ok, results = tester(testargs)
        if ok:
            Config.log(str(results), tag='TDT_STAGING_TEST_OK')
        else:
            msg = "staging tests failed, won't deploy. %s" % str(results)
            Config.log(msg, tag='TDT_STAGING_TEST_FAIL')
            if not force_deploy:
                return

        if no_deploy:
            Config.log('', tag='TDT_NO_DEPLOY')
            return

        # tests passed on staging. move on to production.

        def fixer_prod(full):
            # TODO should put fixups in the config instead of hardcoding
            find_expr = '_STATIC_SERVER_'
            repl_expr = config.get('production', 'static_server')
            u.file_search_replace(full, re.compile(find_expr), repl_expr)

        production_dest_mgr.sync_to_upstream_dest_mgr(
            build_dest_mgr,
            refresh_me='smart',
            refresh_upstream='smart',
            tmp_folder=config.get('production', 'temp_file_root'),
            fixer=fixer_prod)

        testargs = {}
        testargs['static_server'] = config.get('production', 'static_server')
        testargs['dynamic_server'] = config.get('production', 'dynamic_server')
        ok, results = tester(testargs)
        if not ok:
            msg = "PRODUCTION TESTS FAILED, DEPLOYMENT IS BAD! %s" % str(results)
            Config.log(msg, tag='TDT_PRODUCTION_TEST_FAIL')
            return
        Config.log(str(results), tag='TDT_PRODUCTION_TEST_OK')
        Config.log('', tag='TDT_COMPLETE')
    except Exception as exc:
        err = "test_deploy_test: exception '%s'" % str(exc)
        logging.error(err)
        raise

# initial, bare-bones test suite. requests a few urls and verifies basic success
def mini_tests(args):
    if not u.have_required(args, 'static_server', 'dynamic_server'):
        raise Exception("mini_tests incomplete args '%s'" % str(args))
    url_file = Config.main.get('test', 'test_urls')
    with open(url_file) as fh:
        urls = json.loads(fh.read())
    results = []
    success = True
    for url, test_info in urls.items():
        full = args[test_info['host']] + '/' + url
        # todo rework
        status, length, message, content, content_type, meta = u.fetch_page(full)
        results.append(
            {
                'url': full,
                'status': status,
                'message': message
            }
        )
        if status != 200:
            success = False
    Config.log(success, tag='MINI_TESTS_DONE')
    return success, results
