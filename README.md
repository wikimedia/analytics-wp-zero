


SINCE 2015-03-20, THIS REPOSITORY IS DEPRECATED AND NO LONGER IN USE.

Wikipedia Zero graphs are shown on wiki, and the data for the graphs is
computed through Hive. Yuri would know further details.







## Installation

```bash
$ git clone git@github.com:embr/wp-zero.git
$ cd wp-zero
$ pip install --user -e .
```

## Usage

to create dashboards using only carrier counts:

```bash
$ ./make_dashabords.py --carier_counts=carrier.tsv
```

to create dashboards that incorporate country level counts:

```bash
$ ./make_dashabords.py --carier_counts=carrier.tsv --country_counts=country.tsv
```

to make dashboards which include graphs at a daily granularity, in addtion to monthly rollups, include the `--daily` flag

## Options

to configure things to suit your data, check out the options:

```bash
usage: make_dashabords.py [-h] [-l {DEBUG,INFO,WARNING,CRITICAL,ERROR}]
                          [--carrier_counts CARRIER_COUNTS]
                          [--country_counts COUNTRY_COUNTS] [--prefix PREFIX]
                          [--carrier_sep CARRIER_SEP]
                          [--country_sep COUNTRY_SEP]
                          [--carrier_date_fmt CARRIER_DATE_FMT]
                          [--country_date_fmt COUNTRY_DATE_FMT]
                          [--carriers CARRIERS [CARRIERS ...]]
                          [--limn_basedir LIMN_BASEDIR] [--daily]

Process a collection of squid logs and write certain extracted metrics to file

optional arguments:
  -h, --help            show this help message and exit
  -l {DEBUG,INFO,WARNING,CRITICAL,ERROR}, --loglevel {DEBUG,INFO,WARNING,CRITICAL,ERROR}
                        log level
  --carrier_counts CARRIER_COUNTS
                        file in which to find counts of zero filtered request.
                        each run just appends to this file.
  --country_counts COUNTRY_COUNTS
                        directory containing hadoop output files which contain
                        counts for each country, language, project and version
  --prefix PREFIX       string to prepend to all graph and dashboard names
  --carrier_sep CARRIER_SEP
  --country_sep COUNTRY_SEP
  --carrier_date_fmt CARRIER_DATE_FMT
  --country_date_fmt COUNTRY_DATE_FMT
  --carriers CARRIERS [CARRIERS ...]
                        list of carriers for which to create dashboards
  --limn_basedir LIMN_BASEDIR
                        basedir in which to place limn
                        datasource/datafile/graphs/dashboards directories
  --daily               whether to include daily graphs on dashboards
```
