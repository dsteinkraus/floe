import logging
import os
import re

import util as u
from config import Config

def register():
    return {
        'render_mode_images': render_mode_images,
        'render_mode_pages': render_mode_pages,
        'render_mode_thumbs': render_mode_thumbs
    }

re_static_key = re.compile(r'^(/?)static/(.*)')

# if a key starts with 'static/', use the static_server value to
# build corresponding url. If not, just return key. (this is a
# particular design choice, which is why it's in a plugin.)
def key_to_static_url(key):
    match = re.search(re_static_key, key)
    if not match:
        return key
    return '_STATIC_SERVER_' + '/' + match.group(2)

def render_gather_facts(webmaker, work_item):
    if 'output' in work_item['roles']:
        finfo = work_item['roles']['output']
    else:
        finfo = work_item['roles']['default'][0]
    if 'dest_key' in finfo:
        rel_path = '/' + finfo['dest_key']
    elif 'key' in finfo:
        rel_path = '/' + finfo['key']
    elif 'full' in finfo:
        rel_path = '/' + u.make_rel_path(webmaker.output_root, finfo['full'], no_leading_slash=True)
    else:
        raise Exception('logic error, no way to determine rel_path')
    (path, file) = os.path.split(rel_path)
    return finfo, rel_path, path, file

def render_mode_images(webmaker, dest_key, work_item, list_args):
    finfo, rel_path, path, file = render_gather_facts(webmaker, work_item)
    display_name, got_display_name = u.get_display_name(finfo, work_item)
    ret = "<p><img src='%s'></img></p>\n" % (rel_path)
    ret += "<p>%s</p>\n" % display_name
    return ret

def render_mode_pages(webmaker, dest_key, work_item, list_args):
    try:
        logging.debug("render_mode_pages, dest_key = '%s'" % dest_key)
        if dest_key == 'if_empty':
            dest_key = work_item['item_args']['static_page_url']

        if 'item_args' in work_item and 'page_context' in work_item['item_args']:
            webmaker.page_context = work_item['item_args']['page_context']
        else:
            webmaker.page_context = dest_key
        (dest_path, dest_name) = os.path.split(dest_key)
        # if page has a context-specific worklist, use it for symbols
        # TODO hardcoded name
        # TODO design is clunky and prohibits nesting of default worklists
        page_worklist_key = webmaker.page_context + '.' + 'page_wl'
        if page_worklist_key in webmaker._worklists:
            webmaker._default_worklist = webmaker._worklists[page_worklist_key]

        # get page template - look first in work item args, then list args
        item_args = work_item['item_args']
        if 'page_template' in item_args:
            page_template = item_args['page_template']
        elif 'page_template' in list_args:
            page_template = list_args['page_template']
        else:
            raise Exception("render_mode_pages: no page_template")
        # see if output file name is in list_args
        page_name = page_template + '.html'
        if 'page_name' in list_args:
            # TODO should support debracketing
            page_name = list_args['page_name']

            webmaker.default_template_file_action(
                webmaker.generate_root,
                page_template + '.html',  # template file
                dest_rel_path=dest_path,
                dest_name=page_name
            )
            webmaker._default_worklist = None
            webmaker.page_context = None
        return ''
    except Exception as exc:
        Config.log(str(exc), tag='PAGES')
        raise

def render_grid_cell(webmaker, args):
    if 'name' not in args:
        args['name'] = 'thumb_grid_cell'
    if not u.have_required(args, 'width', 'height', 'thumb_url', 'title', 'full_img_url',
                           'description', 'caption'):
        raise Exception('render_grid_cell missing args ' + str(args))
    return webmaker.render_part_args(args)

def render_mode_thumbs(webmaker, dest_key, work_item, list_args):
    try:
        if dest_key == 'if_empty':
            return list_args[dest_key]
        ret = ''
        finfo, href, path, file = render_gather_facts(webmaker, work_item)
        display_name, got_display_name = u.get_display_name(finfo, work_item)
        render = {}
        grid_columns = None
        if 'grid_columns' in list_args:
            grid_columns = list_args['grid_columns']
            # default size 200x200 but should get from thumb metadata below
            render['width'] = '200'
            render['height'] = '200'
        if 'modal' in list_args and list_args['modal']:
            modal = True
        # we (that is, the finfo) might be the thumb, or we
        # might be the image that has a thumb.
        # first see if we're the thumb:
        parent_finfo = None
        parent_key = None

        if 'thumb_full' in finfo or 'thumb_key' in finfo:
            if 'thumb_full' in finfo:
                thumb_path, thumb_file = os.path.split(finfo['thumb_full'])
                # only valid if relative to output_root
                thumb_key = u.make_rel_path(
                    webmaker.output_root,
                    thumb_path,
                    strict=False,
                    no_leading_slash=True
                )
                thumb_key = thumb_key + '/' + thumb_file
            else:
                thumb_key = finfo['thumb_key']
            if thumb_key and thumb_key in webmaker.dest_mgr.tree_info:
                thumb_finfo = webmaker.dest_mgr.tree_info[thumb_key]
                if 'width' in thumb_finfo and 'height' in thumb_finfo:
                    render['width'] = thumb_finfo['width']
                    render['height'] = thumb_finfo['height']
                if not got_display_name and 'display_name' in thumb_finfo:
                    display_name = thumb_finfo['display_name']

                href = key_to_static_url(href)
                thumb_href = key_to_static_url(thumb_key)

                if grid_columns:
                    render['title'] = display_name
                    render['full_img_url'] = href
                    render['thumb_url'] = thumb_href
                    render['description'] = ' ' # todo
                    render['caption'] = display_name
                    ret = render_grid_cell(webmaker, render)
                else:
                    ret = "<p><a href='%s'><img src='%s'></img></a></p>\n" % (href, thumb_href)
                    ret += "<p><a href='%s'>%s</a></p>\n" % (href, display_name)
            else:
                msg = "%s" % thumb_key
                Config.log(msg, tag='RENDER_THUMBS_thumb_not_in_dest')

        elif 'parent_full' in finfo or 'parent_key' in finfo:
            if 'parent_full' in finfo:
                # only valid if relative to output_root
                parent_key = u.make_rel_path(
                    webmaker.output_root,
                    finfo['parent_full'],
                    strict=False,
                    no_leading_slash=True
                )
            else:
                parent_key = finfo['parent_key']
            if parent_key and parent_key in webmaker.dest_mgr.tree_info:
                if 'width' in finfo and 'height' in finfo:
                    render['width'] = finfo['width']
                    render['height'] = finfo['height']
                parent_finfo = webmaker.dest_mgr.tree_info[parent_key]
                if not got_display_name and 'display_name' in parent_finfo:
                    display_name = parent_finfo['display_name']

                parent_href = '/' + parent_key
                if grid_columns:
                    render['title'] = display_name
                    render['full_img_url'] = parent_href
                    render['thumb_url'] = href
                    render['description'] = ' ' # todo
                    render['caption'] = display_name
                    ret = render_grid_cell(webmaker, render)
                else:
                    ret = "<p><a href='%s'><img src='%s'></img></a></p>\n" % (parent_href, href)
                    ret += "<p><a href='%s'>%s</a></p>\n" % (parent_href, display_name)
            else:
                msg = "%s" % parent_key
                Config.log(msg, tag='RENDER_THUMBS_parent_not_in_dest')

        else:
            msg = "unable to resolve '%s' as thumb or parent of thumb" % href
            Config.log(msg, tag='RENDER_THUMBS_not_resolved')

            if grid_columns:
                render['title'] = 'not available'
                render['full_img_url'] = '#'
                render['thumb_url'] = '#'
                render['description'] = 'description not available'
                render['caption'] = dest_key + ' not available'
                ret = render_grid_cell(webmaker, render)
            else:
                ret = "<p><a href='%s'>not available</a></p>\n" % (dest_key)
                ret += "<p><a href='%s'>%s</a></p>\n" % (dest_key, dest_key + ' not available')

        return ret

        # NOTE: used to be code here to support fallback to naming convention (prepending 'thumb_'
        # to the name of the parent file to make thumbnail name). No longer supported, but could
        # be turned back on if found useful.
    except Exception as exc:
        Config.log(str(exc), tag='GRID_THUMBS')
        raise
