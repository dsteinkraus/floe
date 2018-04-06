# floe
A general-purpose, rules-driven engine for manipulating files and building web pages

### Summary

Floe is a Python program to acquire, process, and distribute files. It was created in order to build
https://floe.keytwist.net, and then to rebuild it nightly, incorporating the latest information from
various public sources. I looked for an existing framework, but wasn't able to find one that met my
needs, so my prototype code evolved into floe. The steps in a floe run are:

* Acquire new or changed files (via FTP, HTTP, file system, etc.)

* Apply rules to the files, and to any new files created by those rules. Repeat until there's no more
  work to be done.

* Upload the files we want to a destination manager (such as a web server, or an S3 bucket).

* Use templates to render a set of supporting files (such as HTML files comprising a website).

* Upload the supporting files.

The floe code itself is extremely generic, and easy to extend with plugins, so it can be used for
nearly any task that requires keeping a set of output files up-to-date with a set of input files, with
arbitrary processing in between. All the functionality is in Python modules that can be imported by other programs.

IMPORTANT: this project is under active development, and changes should be expected.

### Prerequisites

Required:

* Python 3.4 or greater

* Linux (Ubuntu has been used the most, porting to other Unix-based platforms shouldn't be difficult)

Optional, depending on scenario:

* Boto3 (if using the S3 destination manager)

* netCDF4 (if processing netCDF files)

* ImageMagick (if needed to perform operations on image files)

* PanoplyCL (if needed to build image files from netCDF data)

These are only required by corresponding plugin files in the plugins/ directory, and by removing unused
plugins, the corresponding dependencies will also go away.

Currently, floe keeps track of file metadata in flat files, so no database software is needed. An optional
PostgreSQL module for metadata is in the works, and this of course would require a PostgreSQL server.

### Installing

```
git clone https://github.com/dsteinkraus/floe.git
```

### Configuring

Floe most have a configuration file, and a rule file. Let's set up an example scenario to fetch some graphics files
from keytwist.net (my site, itself built with floe), and "post" them to a local folder.
Here is a very simple configuration file, save it as simple.config:

```
[actions]

[input]

urls =
    DEMO, https://s.keytwist.net/esrl-daily-forecasts/[YYYY]-[MM]-[DD]/thumb_Arctic10.gif,
	    esrl-daily/[YYYY]-[MM]-[DD]/Arctic10.[MM][DD][YYYY].gif,
	    2018-03-28, 0, fetch

[local]
input = /home/floeuser/floe_data/configs/simple/input
output = /home/floeuser/floe_data/configs/simple/output
admin = /home/floeuser/floe/configs/simple/admin
archive = /home/floeuser/floe_data/configs/simple/archive
plugins = /home/floeuser/floe/plugins

debug_tags = ALL
log_config = True

logfile = simple.log
log_to_console = true

[process]
rule_file = rules.config

[symbols]

[tools]

[build]
file_dest_root = /home/floeuser/floe_data/build
ignore_missing_persist_file = true
refresh_dest_meta = true
```

In the above file, replace "floeuser" with a suitable username. Aside from that, floe generally tries to silently create
any directories it needs, so you won't have to build them all out.

To get started, configure the 'plugins' entry to point to an empty folder (this simple example doesn't need any plugins).

Then create this very simple rule file, and name it rules.config (the rule_file entry in the config file must point
to this file). Rules consist of a condition (boolean test applied to each file) and one or more actions
to take if the condition is satisfied:

```
[label: sample-copy]
if [full] like [input_root]/.*/(\d{4})-(\d{2})-(\d{2})/.*:
    copy to [output_root]/DEMO/[$1][$2][$3]_arctic10.gif
```

### Running

Just invoke floe.py, specifying the configuration file:

```
python floe.py simple.config
```

Watch the log file (/home/floeuser/floe/configs/simple/admin/logs/simple.log) to see progress. Thumbnail images
from 3/28/18 up to the present, should be downloaded to the input folder, copied
to the output folder, and finally, uploaded to the destination folder ("file_dest_root" entry).

If you have an AWS account and want to use an S3 bucket as your destination, copy the plugin file S3.py to your
configured plugins folder. Make your AWS credentials available, e.g. by setting AWS_SHARED_CREDENTIALS_FILE in your
environment (see AWS documentation for details). Then change the [build] section of the config file to point to
your bucket (changing bucket_name and region as needed):

```
[build]
bucket_name = floe_sample_bucket
region = us-west-1
ignore_missing_persist_file = true
refresh_dest_meta = true
```

The images will now be copied (if new or changed) to the file_dest_root directory. (If a web server is configured
to serve content from that location, the files will be visible there.)

## Documentation

A very preliminary floe manual is [here](documentation/Floe_manual.pdf).

The code for https://floe.keytwist.net, which integrates Django with floe, will be added to github soon. This will
illustrate floe usage in more detail.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

This project is licensed under the MIT License - see [LICENSE.md](LICENSE.md)

## Acknowledgments

This effort was inspired by the [Arctic Sea Ice Forums](https://forum.arctic-sea-ice.net), and especially
by user "A-Team" on that site.