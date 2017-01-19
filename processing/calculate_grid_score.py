import os
import fiona
import json
import jenks

from utilities import snap_results_to_grid


def summarize(root_dir, region_list, threads):

    snap_dict = snap_results_to_grid.count_alerts_by_grid(root_dir, region_list, threads)

    ptw_grid_dict = get_ptw_grid_attributes(root_dir)

    results_dict = calculate_score_by_cell(snap_dict, ptw_grid_dict)

    top_break_json = calc_natural_breaks(results_dict)

    return top_break_json


def get_ptw_grid_attributes(root_dir):

    print 'Reading attributes from grid.shp into a dictionary'

    grid_dict = {}

    grid_fc = os.path.join(root_dir, 'data', 'grid', 'ptw_grid.shp')

    with fiona.open(grid_fc, 'r') as the_grid:
        for feature in the_grid:
            lower_left_corner = (feature['properties']['lly_val'], feature['properties']['llx_val'])

            grid_dict[lower_left_corner] = feature['properties']

    return grid_dict


def calculate_score_by_cell(results_dict, grid_dict):

    ptw_dict = {}

    for lower_left_corner, result_dict in results_dict.iteritems():

        # Example result dict:
        # {(2312312.2354, 234900.12300): {'glad_count': 234, 'emissions_sum': 0.00023}}

        ptw_score = grid_dict[lower_left_corner]['importance']
        grid_id = grid_dict[lower_left_corner]['grid_id']
        iso = grid_dict[lower_left_corner]['ISO']

        result_dict['ISO'] = iso
        result_dict['score'] = ptw_score * result_dict['glad_count']
        result_dict['grid_id'] = grid_id

        # Example PTW dict
        # From:
        #{"emissions_sum": 0.0008351699999999998, "score": 0.0, "glad_count": 21, "ISO": "COG", "grid_id": "COG_107229"}
        # create key of the iso, and add each dictionary as item in list.
        # To:
        # {"COG":[{"emissions_sum": 0.00083, "score": 0.0, "glad_count": 21, "ISO": "COG", "grid_id": "COG_107229"},
        #{"emissions_sum": 0.000835, "score": 0.0, "glad_count": 21, "ISO": "COG", "grid_id": "COG_10742"}],"IDN":[...]}
        try:
            ptw_dict[iso].append(result_dict)

        except:
            ptw_dict[iso] = [result_dict]

    process_results(ptw_dict, "all_data")

    return ptw_dict


def calc_natural_breaks(ptw_dict):

    print "calculating natural breaks"
    top_break_ptw_dict = {}

    for country, country_dict_list in ptw_dict.iteritems():

        ptw_score_list = []

        # from ptw dict, write just the score into the score list
        for country_dict in country_dict_list:
            ptw_score_list.append(country_dict['score'])

        # run jenks on the score list, returning the base break
        breaks = jenks.jenks(ptw_score_list, 5)
        base_break = breaks[len(breaks) - 2]
        print "{} top break value is: {}".format(country, base_break)

        # retreive those ptw cells with score >= the base break
        for country_dict in country_dict_list:

            if country_dict['score'] >= base_break:
                try:
                    top_break_ptw_dict[country].append(country_dict)
                except:
                    top_break_ptw_dict[country] = [country_dict]

    top_break_rows = process_results(top_break_ptw_dict, "top_break")

    return top_break_rows


def process_results(input_dict, list_name):

    all_row_list = []

    for country, rows_list in input_dict.iteritems():

        all_row_list.extend(rows_list)

        outfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), '{}_{}.json'.format(country, list_name))

        with open(outfile, mode='w') as outfile_obj:
            json.dump(rows_list, outfile_obj)

    return all_row_list
