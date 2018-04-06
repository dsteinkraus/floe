import sys
import os
import shutil
import errno

# a one-off script to move some files around

def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    raise Exception(text + " does not start with " + prefix)

def ensure_path(dir_):
    try:
        os.makedirs(dir_)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

src_root = sys.argv[1]
dest_root = sys.argv[2]
dry_run = len(sys.argv) == 4 and sys.argv[3] == 'True'

if not os.path.isdir(src_root):
    print(src_root + " is not a directory")
    sys.exit(2)
if not os.path.isdir(dest_root):
    print(dest_root + " is not a directory")
    sys.exit(2)

for dir_name, subdirs, files in os.walk(src_root):
    for file_name in files:
        if 'frame_00000.gif' in file_name:
            subpath = remove_prefix(dir_name, src_root)
            src = dir_name + '/' + file_name
            ensure_path(dest_root + subpath)
            dest = dest_root + subpath + '/' + file_name
            if dry_run:
                print("would copy '%s' to '%s'" % (src, dest))
            else:
                print("copying '%s' to '%s'" % (src, dest))
                shutil.copyfile(src, dest)
