import argparse
import os
import multiprocessing
from Queue import Queue

from processing import job, calculate_grid_score, api
from region import Region


def main():

    parser = argparse.ArgumentParser(description='Download glad, find last 30 days, tabulate score by PWT grid')
    parser.add_argument('--region', '-r', required=True, choices=['south_america', 'africa', 'se_asia', 'all'],
                        help='the region to process')
    parser.add_argument('--threads', '-n', help='the number of threads', default=multiprocessing.cpu_count(), type=int)
    parser.add_argument('--debug', dest='debug', action='store_true')
    parser.add_argument('--test', dest='test', action='store_true')
    args = parser.parse_args()

    root_dir = os.path.dirname(__file__)

    if args.region == 'all':
        region_list = ['south_america', 'se_asia', 'africa']
    else:
        region_list = [args.region]

    q = Queue()

    for r in region_list:
        region = Region(r, args.test, q)

        region.download_source()

        region.prep_raster_data()

        region.emissions_to_point()

    job.process_queue(args.threads, q, args.debug)

    top_break_rows = calculate_grid_score.summarize(root_dir, region_list, args.threads)

    api.push_to_carto(top_break_rows, root_dir, args.test)


if __name__ == '__main__':
    main()
