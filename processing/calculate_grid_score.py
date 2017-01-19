import os
import fiona
import json
import jenks

from utilities import snap_results_to_grid


def summarize(root_dir, region_list, threads):

    snap_dict = snap_results_to_grid.count_alerts_by_grid(root_dir, region_list, threads)

    ptw_grid_dict = get_ptw_grid_attributes(root_dir)

    results_dict = calculate_score_by_cell(snap_dict, ptw_grid_dict)

    top_break_json = filter_top_10(results_dict)

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

        has_match = False

        try:
            matching_feat = grid_dict[lower_left_corner]
            has_match = True

        except KeyError:
            has_match = False

        if has_match:
            ptw_score = grid_dict[lower_left_corner]['importance']
            grid_id = grid_dict[lower_left_corner]['grid_id']
            region = grid_dict[lower_left_corner]['region']

            result_dict['region'] = region
            result_dict['score'] = ptw_score * result_dict['glad_count']
            result_dict['grid_id'] = grid_id

            # Example PTW dict
            # From:
            #{"emissions_sum": 0.0008351699999999998, "score": 0.0, "glad_count": 21, "region": "africa", "grid_id": "africa_107229"}
            # create key of the iso, and add each dictionary as item in list.
            # To:
            # {"africa":[{"emissions_sum": 0.00083, "score": 0.0, "glad_count": 21, "region": "africa", "grid_id": "africa_107229"},
            #{"emissions_sum": 0.000835, "score": 0.0, "glad_count": 21, "region": "africa", "grid_id": "africa_10742"}],"IDN":[...]}
            try:
                ptw_dict[region].append(result_dict)

            except:
                ptw_dict[region] = [result_dict]

        else:
            root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            error_log = os.path.join(root_dir, "error.log")
            with open(error_log, 'w') as errortext:
                errortext.write(str(lower_left_corner) + '\n')

    return ptw_dict


def filter_top_10(ptw_dict):

    all_region_top_10 = []

    for region, region_dict_list in ptw_dict.iteritems():

        sorted_regiondictlist = sorted(region_dict_list, key=lambda k: k['score'], reverse=True)

        top_10_unfiltered = sorted_regiondictlist[0:10]

        top_10 = [d for d in top_10_unfiltered if d['score'] > 0.75]

        export_results(top_10, 'top_10', region)
        export_results(sorted_regiondictlist, 'all', region)

        all_region_top_10.extend(top_10)

    return all_region_top_10


def export_results(export_list, list_name, region):

    outfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), '{}_{}.json'.format(region, list_name))

    with open(outfile, mode='w') as outfile_obj:
        json.dump(export_list, outfile_obj)
