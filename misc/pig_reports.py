import argparse
import pandas as pd
import logging
import pprint
from operator import itemgetter

from squid.mapreduce import count_files
from squid import SquidArgumentParser

# this change made from local machine

DEFAULT_PROVIDERS = ['zero-digi-malaysia',
                     'zero-grameenphone-bangladesh',
                     'zero-orange-kenya',
                     'zero-orange-niger',
                     'zero-orange-tunesia',
                     'zero-orange-uganda',
                     'zero-orange-cameroon',
                     'zero-orange-ivory-coast',
                     'zero-telenor-montenegro',
                     'zero-tata-india',
                     'zero-saudi-telecom',
                     'zero-dtac-thailand']


def parse_args():

    parser = SquidArgumentParser(description='Process a collection of squid logs and write certain extracted metrics to file')
    parser.add_argument('providers', 
                        metavar='PROVIDER_IDENTIFIER',
                        nargs='*',
                        default=DEFAULT_PROVIDERS,
                        help='list of provider identifiers used in squid log file names')
    parser.add_argument('--name_format',
                        dest='name_format',
                        type=str,
                        default='%s.log-%.gz.counts',
                        help='a printf style format string which is formatted with the tuple: (provider_name, date_representation')
    parser.set_defaults(datadir='/home/erosen/src/dashboard/mobile/zero_counts')


    args = parser.parse_args()
    # custom logic for which files to grab
    prov_files = {}
    for prov in args.providers:
        args.basename = prov
        logging.info('args prior to ge_files: %s', pprint.pformat(args.__dict__))
        prov_files[prov] = SquidArgumentParser.get_files(args)
    setattr(args, 'squid_files', prov_files)

    
    logging.info(pprint.pformat(args.__dict__))
    return args


def main():
    args = parse_args()

    for prov in args.providers:
        # 3,2013-01-30,bs,wikipedia,M,ME,telenor-montenegro
        header = ['count', 'date', 'language', 'project','site', 'country', 'provider'] 
        counts = pd.concat([pd.read_csv(f, names=header, parse_dates=['date']) for f in args.squid_files[prov]])
        counts = counts[(counts['date'] < args.end) & (counts['date'] >= args.start)]
        counts['date'] = counts['date'].apply(lambda d : d.date())
        final = ['date', 'project', 'language', 'site', 'count']
        counts = counts[final]
        print counts
        counts = counts.fillna(None).groupby(['date', 'project', 'language', 'site']).sum().reset_index()
        counts['count'] = counts['count'] * 10
        counts = counts.sort(final)
        counts.to_csv(open('%s.counts.csv' % prov, 'w'), index=False)

if __name__ == '__main__':
    main()
