import logging
import os
import copy

import util as u
from config import Config

def register():
    return {
        #'render_mode_forecast_nav_by_date': render_mode_forecast_nav_by_date,
        #'render_mode_recent_nav_by_date': render_mode_recent_nav_by_date,
        'render_mode_generic_nav_by_date': render_mode_generic_nav_by_date,
        'render_mode_nav_links': render_mode_nav_links
    }

def render_mode_generic_nav_by_date(
        webmaker,
        dest_key,
        work_item,
        list_args):
    try:
        if dest_key == 'if_empty':
            # no items to render. make 1 page with emptiness message, and
            # a link to it.
            # passing the URL of the apology page this way
            page_url = list_args['if_empty']
            links_wl = webmaker.ensure_worklist(list_args['links_wl_name'])
            links_wl[dest_key] = {
                'index': 0,
                'roles': {'default': []},
                'page_url': page_url,
                'item_args': {}
            }

            # single-item wl for the page
            per_page_worklist_name = page_url + Config.main.default_page_wl_name()
            per_page_wl = webmaker.ensure_worklist(per_page_worklist_name)
            per_page_wl[dest_key] = {
                'index': 0,
                'roles': {'default': []},
                'item_args':{'list_header': 'No items are available.', 'sort_key': ''}
            }

            # wl to render page:
            gen_pages_wl = webmaker.ensure_worklist(list_args['gen_pages'])
            # TODO REVIEW think this is right...NOTE not valid if !django!!! TODO FIX
            if 'short' in list_args:
                static_page_url = page_url + '/' + list_args['url_suffix']
            else:
                static_page_url = page_url
            gen_pages_wl['if_empty'] = {
                'index': 0,
                'item_args': {'page_context': page_url, 'static_page_url': static_page_url},
                'list_args': list_args
            }

            # single-entry wl to point to latest and only page:
            entry_wl = webmaker.ensure_worklist(list_args['entry_worklist_name'])
            django_page_url = '/' + list_args['url_prefix'] + '/' + '0000-00-00'
            entry_wl['if_empty'] = {
                'date_string': '0000-00-00',
                'value': page_url,
                'django_value': django_page_url,
                'index': 0,
                'item_args': {}
            }

            return ''

        finfo = work_item['roles']['default'][0]
        # ensure it's on the worklist of dates to link from left nav
        ### TODO - generated file name should not be created ad-hoc in two places,
        ### it should be stored as an arg where both the link and the saving of the
        ### page can use it

        if not u.have_required(list_args, 'url_prefix', 'url_suffix', 'links_wl_name',
                               'display_name_spec', 'list_header_spec', 'gen_pages',
                               'entry_worklist_name'):
            raise Exception('required arguments are missing from list_args')

        # NOTE! the link dest is the page containing this image, not the image itself
        # TODO do this further upstream, this needlessly overwrites the worklist item N times
        page_url = '/' + list_args['url_prefix'] + '/' + work_item['item_args']['date_string']
        if 'short' not in list_args:
            page_url = page_url + '/' + list_args['url_suffix']
        links_wl = webmaker.ensure_worklist(list_args['links_wl_name'])
        links_wl[page_url] = {
            'index': len(links_wl),
            'roles': {'default': [finfo]},
            'page_url': page_url,
            'item_args': copy.deepcopy(work_item['item_args'])
        }
        # add to custom worklist for its page
        # TODO fix hardcoded default name
        per_page_worklist_name = page_url + '.page_wl'
        per_page_wl = webmaker.ensure_worklist(per_page_worklist_name)
        per_page_wl[dest_key] = {
            'index': len(per_page_wl),
            'roles': {'default': [finfo]},
            'item_args': copy.deepcopy(work_item['item_args'])
        }
        # TODO find a less wonky solution to bracket in bracket (and colon!) issue
        display_name_spec = u.fix_alt_arg(list_args['display_name_spec'])
        # todo can we combine this with u.get_display_name somehow??
        display_name = u.parse_subargs(
            display_name_spec,
            webmaker.interpret,
            webmaker.config,
            finfo)
        per_page_wl[dest_key]['item_args']['display_name'] = display_name

        # TODO see above. Also, no good reason this is different from display name!!!
        list_header_spec = u.fix_alt_arg(list_args['list_header_spec'])
        list_header = u.debracket(
            list_header_spec,
            webmaker.interpret,
            finfo=finfo,
            symbols=work_item['item_args'])
        # todo: this is ad-hoc and inscrutable. need page context for debracketing.
        per_page_wl[dest_key]['item_args']['list_header'] = list_header
        per_page_wl[dest_key]['item_args']['image_url'] = dest_key
        per_page_wl[dest_key]['item_args']['symbol_key'] = finfo['name']

        # ensure it's on the worklist of pages to generate
        gen_pages_wl = webmaker.ensure_worklist(list_args['gen_pages'])
        # REVIEW why isn't the key page_url here?
        gen_pages_wl[dest_key] = {
            'index': len(gen_pages_wl),
            'item_args': copy.deepcopy(work_item['item_args']),
            'list_args': list_args
        }
        gen_pages_wl[dest_key]['item_args']['page_context'] = page_url

        # ensure single-item worklist that links to most recent (used to select entry page)
        entry_wl = webmaker.ensure_worklist(list_args['entry_worklist_name'])
        setit = True
        cur_date = work_item['item_args']['date_string']
        if 'only' in entry_wl:
            latest_date = entry_wl['only']['date_string']
            # format YYYY-MM-DD so we can use string compare
            setit = cur_date > latest_date
        if setit:
            django_page_url = '/' + list_args['url_prefix'] + '/' + work_item['item_args']['date_string']
            entry_wl['only'] = {
                'date_string': cur_date,
                'value': page_url,
                'django_value': django_page_url,
                'index': 0,
                'item_args': copy.deepcopy(work_item['item_args'])
            }
        return ''
    except Exception as exc:
        Config.log(str(exc), tag='NAV_BY_DATE')
        raise

# render a list item with a link to the finfo
def render_mode_nav_links(webmaker, dest_key, work_item, list_args):
    try:
        if dest_key == 'if_empty':
            return "<li>No items</li>"
        page_url = work_item['page_url']
        display_name = work_item['item_args']['date_string']
        selected = display_name in webmaker.page_context

        link = "<a href='%s'>%s</a>" % (page_url, display_name)
        if selected:
            link = "<b>" + link + "</b>"
        link = "<li>" + link + "</li>"
        return link
    except Exception as exc:
        Config.log(str(exc), tag='NAV_LINKS')
        raise
