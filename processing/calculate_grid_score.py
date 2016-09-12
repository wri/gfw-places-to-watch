import os
import fiona
import random

from utilities import snap_results_to_grid, jenks2


def summarize(root_dir, region_list, threads):

    results_dict = snap_results_to_grid.count_alerts_by_grid(root_dir, region_list, threads)

    ptw_grid_dict = get_ptw_grid_attributes(root_dir)

    results_dict = calculate_score_by_cell(results_dict, ptw_grid_dict)

    return results_dict
    # Return a dict of [grid_id: {alerts_count: x, att1: y, att2: z}


def get_ptw_grid_attributes(root_dir):

    print 'Reading attributes from grid.shp into a dictionary'

    grid_dict = {}

    grid_fc = os.path.join(root_dir, 'data', 'grid', 'grid.shp')

    with fiona.open(grid_fc, 'r') as the_grid:
        for feature in the_grid:
            lower_left_corner = (feature['properties']['lly_val'], feature['properties']['llx_val'])

            grid_dict[lower_left_corner] = feature['properties']

    return grid_dict


def calculate_score_by_cell(results_dict, grid_dict):

    for lower_left_corner, output in results_dict.iteritems():
        glad_count = output['glad_count']
        emissions_sum = output['emissions_sum']

        grid_iso_id = grid_dict[lower_left_corner]['grid_id']

        print 'Found {0} alerts and {1} total emissions for grid {2}'.format(glad_count, emissions_sum, grid_iso_id)

        # Dummy output
        output['score'] = random.randint(0, 10)

    return results_dict


def calc_natural_breaks():

    pass