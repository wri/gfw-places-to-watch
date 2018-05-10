import argparse
import os
import sys

from processing import calculate_grid_score, api, validate as val

external_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(external_dir)

# The hadoop_pip package must be in the external dir referenced above, i.e.:
# If you don't have it, it's available here: https://github.com/wri/hadoop_pip
# - Desktop
#    - gfw-places-to-watch
#    - hadoop-pip

from hadoop_pip import run_pip


def main():

    parser = argparse.ArgumentParser(description='Run hadoop process to count GLAD by PTW grid cell')
    parser.add_argument('--min-date', required=True, type=val.valid_date,
                        help='the minimum alert date (YYYY-mm-dd) to consider for this run (inclusive)')
    parser.add_argument('--max-date', required=True, type=val.valid_date,
                        help='the maximum alert date (YYYY-mm-dd) to consider for this run (inclusive)')
    parser.add_argument('--staging', dest='staging', action='store_true', help='pushes results to staging table')
    args = parser.parse_args()

    # start hadoop cluster, run analysis with hadoop_ptw.ini as input
    ptw_config = os.path.join('processing', 'hadoop_ptw.ini')
    s3_result_output = run_pip.run([ptw_config])
   
    # grab results from s3, tabulate glad count * importance, pick top 10 for each region
    top_10_results = calculate_grid_score.tabulate_results(s3_result_output, args.min_date, args.max_date)

    # push to carto PTW table
    api.push_to_carto(top_10_results, args.staging)


if __name__ == '__main__':
    main()
