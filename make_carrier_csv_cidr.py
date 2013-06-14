#!/usr/bin/env python

import argparse
import csv
import logging
import pprint
from operator import itemgetter

from squid import SquidArgumentParser, count_files, get_files

logger = logging.getLogger(__name__)

DEFAULT_PROVIDERS = ['digi-malaysia',
                     'dtac-thailand',
                     'grameenphone-bangladesh',
                     'orange-kenya',
                     'orange-niger',
                     'orange-tunesia',
                     'orange-uganda',
                     'orange-cameroon',
                     'orange-ivory-coast',
                     'telenor-montenegro',
                     'tata-india',
                     'saudi-telecom',
                     'dtac-thailand'
                     'orange-congo',
                     'smart-philippines',
                     'tim-brasil']


def parse_args():

    parser = SquidArgumentParser(description='Process a collection of squid logs and write certain extracted metrics to file',
           formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('providers', 
                        metavar='PROVIDER_IDENTIFIER',
                        nargs='*',
                        default=DEFAULT_PROVIDERS,
                        help='list of provider identifiers used in squid log file names')
    parser.add_argument('--name_format',
                        dest='name_format',
                        type=str,
                        default='%s.tab.log-%.gz',
                        help='a printf style format string which is formatted with the tuple: (provider_name, date_representation')
    parser.set_defaults(datadir='/a/squid/archive/zero')


    args = parser.parse_args()
    # custom logic for which files to grab
    prov_files = {}
    for prov in args.providers:
        basename = 'zero-%s' % prov
        logger.debug('basename: %s', basename)
        prov_files[prov] = get_files(args.start, args.end, args.datadir, basename)
    setattr(args, 'squid_files', prov_files)

    
    logger.info(pprint.pformat(args.__dict__))
    return args

def main():
    args = parse_args()
    keepers = ['date', 'lang', 'project', 'site', 'na']

    criteria = [
            lambda r : r.datetime() > args.start,
            lambda r : r.datetime() < args.end,
            lambda r : r.old_init_request()
    ]

    for prov in args.providers:
        if not args.squid_files[prov]:
            logger.info('skipping provider: %s because no files were found', prov)
            continue
        counts = count_files(args.squid_files[prov], keepers, criteria, count_event=10)
        rows = [fields + (prov, count) for fields,count in counts.items()]
        rows = [map(str,row) for row in rows]
        rows.sort(key=itemgetter(*range(len(keepers))))
        with open('%s.counts.csv' % prov, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(keepers + ['provider', 'count'])
            writer.writerows(rows)

if __name__ == '__main__':
    main()
