#!/usr/bin/python
import pprint
import squid
import logging

logger = logging.getLogger(__name__)

def main():
    parser = squid.SquidArgumentParser('filters squid logs by provider ranges')
    squid.squidrow.load_cidr_ranges()
    parser.add_argument('country_code')
    parser.add_argument('-o', '--outdir', default='.', help='directory in which to place the filtered files')
    args = parser.parse_args()
    logger.info(pprint.pformat(args))

    keepers = ['language', 'project', 'site', 'title']

    criteria = [
            lambda r : r.country_code2() == args.country_code,
            lambda r : r.old_init_request(),
            lambda r : r.datetime() > args.start,
            lambda r : r.datetime() < args.end,
    ]

    counts = squid.count_files(args.squid_files, 
            keepers, 
            criteria,
            count_event=1000,
            limit=args.max_lines,
            nproc=15,
            fname='%s_top_k_titles_incremental.csv' % args.country_code)

    squid.write_counts(counts, '%s_top_k_articles.csv' % args.country_code)

if __name__ == '__main__':
    main()
