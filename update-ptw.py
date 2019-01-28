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
    parser.add_argument('--analysis-type', required=True,  choices=('flagship', 'soy', 'palm'),
                        help='the specific PTW analysis we want to run')
    parser.add_argument('--staging', dest='staging', action='store_true', help='pushes results to staging table')
    args = parser.parse_args()

    # start hadoop cluster, run analysis with the correct config file as input
    ptw_config = os.path.join('config', args.analysis_type + '.ini')
    s3_result_output = run_pip.run([ptw_config])  ##IC CHANGE BACK
    #s3_result_output  = r's3://gfw2-data/alerts-tsv/hadoop-jobs/de7fc4be-2e2b-47bb-87ba-bed520be066c/output1.csv' #hadoop
    # grab results from s3, tabulate glad count * importance, pick top 10 for each region
    print s3_result_output
    top_10_results = calculate_grid_score.tabulate_results(s3_result_output, args.min_date, args.max_date, args.analysis_type)

    # push to carto PTW table
    api.push_to_carto(top_10_results, args.analysis_type, args.staging)


if __name__ == '__main__':
    main()
