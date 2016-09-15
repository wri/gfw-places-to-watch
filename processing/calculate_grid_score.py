import os
import fiona
import json

from utilities import snap_results_to_grid, jenks2


def summarize(root_dir, region_list, threads):

    snap_dict = snap_results_to_grid.count_alerts_by_grid(root_dir, region_list, threads)

    ptw_grid_dict = get_ptw_grid_attributes(root_dir)

    results_dict = calculate_score_by_cell(snap_dict, ptw_grid_dict)

    top_break_json = calc_natural_breaks(results_dict)

    print top_break_json

    return top_break_json
    # Return a dict of [grid_id: {alerts_count: x, att1: y, att2: z}


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

        ptw_score = grid_dict[lower_left_corner]['wt_IFL2']
        result_dict['score'] = ptw_score * result_dict['glad_count']

        iso = grid_dict[lower_left_corner]['ISO']
        result_dict['ISO'] = iso

        grid_id = grid_dict[lower_left_corner]['grid_id']

        # Example PTW dict
        # {'bra_19021': {'glad_count': 234, 'emissions_sum': 0.00023, 'score': 10.124, 'iso': 'BRA'}
        ptw_dict[grid_id] = result_dict

    return ptw_dict


def calc_natural_breaks(ptw_dict):
    print "calculating natural breaks"
    top_break_ptw_dict = {}
    ptw_score_list = []

    for key in ptw_dict:
        ptw_score_list.append(ptw_dict[key]['score'])

    breaks = jenks2.jenks(ptw_score_list, 5)
    base_break = breaks[len(breaks) - 2]
    print "top break value is: {}".format(base_break)
    for grid_id, results in ptw_dict.iteritems():

        if ptw_dict[grid_id]['score'] >= base_break:
            top_break_ptw_dict[grid_id] = results

    outfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'outfile.json')
    with open(outfile, mode='w') as outfile_obj:
        json.dump(top_break_ptw_dict, outfile_obj)
    return top_break_ptw_dict



