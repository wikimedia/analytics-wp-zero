#!/usr/bin/python
from squid import count_files, write_counts
from squid.util import SquidArgumentParser
import logging
import pprint

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    parser = SquidArgumentParser()
    parser.set_defaults(basename='zero-.*', datadir='/a/squid/archive/zero')
    args = parser.parse_args()
    logger.info(pprint.pformat(vars(args)))


    criteria = [
            lambda r : r.site() in ['M', 'Z'],
            lambda r : r.old_init_request(),
            lambda r : r.project == 'wikipedia',
            ]

    fields = ['date', 'language', 'project', 'site', 'na', 'provider_from_file']
    
    counts = count_files(args.squid_files,
            fields,
            criteria,
            nproc=10,
            limit=args.max_lines,
            fname='carrier_counts_cidr_all.incremental.csv')

    write_counts(counts, 'carrier_counts_cidr_all.counts.csv')
