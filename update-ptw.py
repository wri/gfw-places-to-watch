import argparse
import multiprocessing
from Queue import Queue

from region import Region
from processing.job import process_queue


def main():

    parser = argparse.ArgumentParser(description='Download glad, find last 30 days, tabulate score by PWT grid')
    parser.add_argument('--region', '-r', required=True, choices=['south_america', 'africa', 'asia', 'all'],
                        help='the region to process')
    parser.add_argument('--threads', '-n', help='the number of threads', default=multiprocessing.cpu_count(), type=int)
    parser.add_argument('--debug', dest='debug', action='store_true')
    parser.add_argument('--test', dest='test', action='store_true')
    args = parser.parse_args()

    if args.test:
        args.threads = 1

    if args.region == 'all':
        region_list = ['south_america', 'asia', 'africa']
    else:
        region_list = [args.region]

    q = Queue()

    for r in region_list:
        region = Region(r, args.test, q)

        region.download_source()

        region.prep_raster_data()

        # region.emissions_to_point()
        #
        # region.classify_point_data_by_grid_cell()
        #
        # region.tabulate_results()

    process_queue(args.threads, q, args.debug)

    # TODO add something to push results to API


if __name__ == '__main__':
    main()
