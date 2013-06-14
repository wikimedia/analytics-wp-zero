#!/usr/bin/python
import pprint
import squid
import logging

logger = logging.getLogger(__name__)

def main():
    parser = squid.SquidArgumentParser('filters squid logs by provider ranges')
    squid.squidrow.load_cidr_ranges()
    parser.add_argument('provider', 
            choices=squid.squidrow.cidr_ranges.keys(),
            help='name of a provider to filter by')
    parser.add_argument('-o', '--outdir', default='.', help='directory in which to place the filtered files')
    args = parser.parse_args()
    logger.info(pprint.pformat(args))
   
    keepers = ['date', 'language', 'project', 'site', 'country', 'na']

    criteria = [
            lambda r : r.site() in ['M', 'Z'],
            lambda r : r.old_init_request(),
            lambda r : r.provider() == args.provider,
            lambda r : r.datetime() > args.start,
            lambda r : r.datetime() < args.end,
    ]

    counts = squid.count_files(args.squid_files, 
            keepers, 
            criteria,
            count_event=1000,
            limit=args.max_lines,
            nproc=15,
            fname='%s_counts_incremental.csv' % args.provider)

    squid.write_counts(counts, '%s_counts.csv' % args.provider)

if __name__ == '__main__':
    main()
