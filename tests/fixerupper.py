import sys
import re
import os

if __name__ == '__main__' and __package__ is None:
    from os import path
    sys.path.append(path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import util as u

# a quick utility to do global regex search/replace on all files in a
# folder (or a subset that matches file_filter_expr)

def usage():
    print('usage: python fixerupper.py root, search_expr, replacement, file_filter_expr')
    print('  root = root folder in which to do replacements')
    print('  search_expr = a regular expression to be searched for in files')
    print('  replacement = string to be inserted in place of search_expr')
    print('  file_filter_expr = a regular expression, only filenames matching this will be processed')
    print('  (optional) encoding = file encoding, e.g. utf-8-sig')
    sys.exit(1)

if len(sys.argv) < 5:
    usage()
(root, search_expr, replacement, file_filter_expr) = sys.argv[1:5]

options = None
if len(sys.argv) >= 6:
    options = {'encoding': sys.argv[5]}

if not os.path.isdir(root):
    print("error: '%s' is not a directory." % root)
    usage()

re_file_filter = re.compile(file_filter_expr)
def file_filter(s):
    return re_file_filter.search(s)

n_seen, n_skipped, n_unchanged, n_changed = \
    u.folder_search_replace(root, search_expr, replacement, file_filter=file_filter, options=options)
print("seen: %i, skipped: %i, unchanged: %i, changed: %i" % (n_seen,
        n_skipped, n_unchanged, n_changed))
sys.exit(0)