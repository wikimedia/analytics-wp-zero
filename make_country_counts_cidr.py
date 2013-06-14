#!/usr/bin/env python
import sys
import csv
import logging
import pprint
from operator import itemgetter

from squid.mapreduce import count_files, write_counts
from squid import SquidArgumentParser

logger = logging.getLogger(__name__)

def main():
    parser = SquidArgumentParser()
    parser.add_argument('--nprocs', default=10)
    args = parser.parse_args()
    logger.info(pprint.pformat(args.__dict__))

    keepers = ['date', 'language', 'project', 'site', 'country', 'na']

    criteria = [
            lambda r : r.old_init_request(),
            lambda r : r.site() == 'M',
            lambda r : r.datetime() > args.start,
            lambda r : r.datetime() < args.end,
    ]

    counts = count_files(args.squid_files, 
            keepers, 
            criteria,
            count_event=1000,
            limit=args.max_lines,
            nproc=15,
            fname='country_counts_incremental.csv')

    write_counts(counts, 'country_counts.csv')

if __name__ == '__main__':
    main()
