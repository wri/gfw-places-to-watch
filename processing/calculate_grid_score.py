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

        ptw_score = grid_dict[lower_left_corner]['wt_IFL2']
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

    write_results_to_json(ptw_dict, "all_data")

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

    write_results_to_json(top_break_ptw_dict, "top_break")


def write_results_to_json(input_dict, list_name):

    for country, rows_list in input_dict.iteritems():

        outfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), '{}_{}.json'.format(country, list_name))

        with open(outfile, mode='w') as outfile_obj:
            json.dump(rows_list, outfile_obj)

# dict_list = [{"BRA_67354": {"emissions_sum": 4.329e-05, "score": 0.775195882, "glad_count": 1, "ISO": "BRA"}, "BRA_57969": {"emissions_sum": 0.00048661999999999995, "score": 6.9550631759999995, "glad_count": 12, "ISO": "BRA"}, "BRA_60267": {"emissions_sum": 0.00017411, "score": 3.0971168579999997, "glad_count": 6, "ISO": "BRA"}, "BRA_57967": {"emissions_sum": 0.004268999999999999, "score": 45.320944759999996, "glad_count": 124, "ISO": "BRA"}, "BRA_57964": {"emissions_sum": 3.837e-05, "score": 0.417911567, "glad_count": 1, "ISO": "BRA"}, "BRA_60264": {"emissions_sum": 0.0005383, "score": 2.671036992, "glad_count": 18, "ISO": "BRA"}, "BRA_57962": {"emissions_sum": 2.871e-05, "score": 0.079145917, "glad_count": 1, "ISO": "BRA"}, "BRA_60262": {"emissions_sum": 0.00040554, "score": 6.89472402, "glad_count": 10, "ISO": "BRA"}, "BRA_60261": {"emissions_sum": 0.00012293999999999999, "score": 1.02897393, "glad_count": 3, "ISO": "BRA"}, "BRA_57961": {"emissions_sum": 0.00155708, "score": 5.2032356040000005, "glad_count": 42, "ISO": "BRA"}},{"COG_107229": {"emissions_sum": 0.0008351699999999998, "score": 0.0, "glad_count": 21, "ISO": "COG"}, "COG_107855": {"emissions_sum": 5.187e-05, "score": 0.0, "glad_count": 4, "ISO": "COG"}, "COG_107993": {"emissions_sum": 0.0005666, "score": 0.0, "glad_count": 33, "ISO": "COG"}, "COG_108020": {"emissions_sum": 0.0031209500000000012, "score": 0.0, "glad_count": 162, "ISO": "COG"}, "COG_107501": {"emissions_sum": 0.00016106, "score": 0.0, "glad_count": 9, "ISO": "COG"}, "COG_107994": {"emissions_sum": 0.00114903, "score": 0.0, "glad_count": 56, "ISO": "COG"}, "COG_107995": {"emissions_sum": 0.00023807000000000003, "score": 0.0, "glad_count": 18, "ISO": "COG"}, "COG_107185": {"emissions_sum": 0.00010401, "score": 1.296747790221, "glad_count": 3, "ISO": "COG"}, "COG_107184": {"emissions_sum": 0.00084161, "score": 10.256250980661, "glad_count": 21, "ISO": "COG"}, "COG_107187": {"emissions_sum": 0.0018963500000000004, "score": 0.059442048, "glad_count": 48, "ISO": "COG"}, "COG_107186": {"emissions_sum": 0.0017167999999999997, "score": 1.7145105280000001, "glad_count": 46, "ISO": "COG"}, "COG_108100": {"emissions_sum": 1.215e-05, "score": 0.0, "glad_count": 1, "ISO": "COG"}, "COG_107188": {"emissions_sum": 0.00110375, "score": 0.0, "glad_count": 28, "ISO": "COG"}, "COG_108102": {"emissions_sum": 0.00010791, "score": 0.0, "glad_count": 6, "ISO": "COG"}, "COG_108103": {"emissions_sum": 0.00030411, "score": 0.0, "glad_count": 13, "ISO": "COG"}, "COG_108104": {"emissions_sum": 0.00017241000000000004, "score": 0.0, "glad_count": 12, "ISO": "COG"}, "COG_107727": {"emissions_sum": 5.044e-05, "score": 0.0, "glad_count": 2, "ISO": "COG"}, "COG_107726": {"emissions_sum": 4.425e-05, "score": 0.0, "glad_count": 2, "ISO": "COG"}, "COG_108164": {"emissions_sum": 0.00020541, "score": 0.0, "glad_count": 5, "ISO": "COG"}, "COG_107901": {"emissions_sum": 0.0005165399999999999, "score": 0.0, "glad_count": 27, "ISO": "COG"}, "COG_107900": {"emissions_sum": 0.0005812699999999999, "score": 0.0, "glad_count": 36, "ISO": "COG"}, "COG_108197": {"emissions_sum": 0.003578080000000001, "score": 0.0, "glad_count": 125, "ISO": "COG"}, "COG_108196": {"emissions_sum": 0.00118736, "score": 0.0, "glad_count": 32, "ISO": "COG"}, "COG_108198": {"emissions_sum": 0.00279652, "score": 0.0, "glad_count": 69, "ISO": "COG"}, "COG_108299": {"emissions_sum": 0.0017579699999999993, "score": 4.3608367626912, "glad_count": 62, "ISO": "COG"}, "COG_107967": {"emissions_sum": 1.321e-05, "score": 0.0, "glad_count": 1, "ISO": "COG"}, "COG_108264": {"emissions_sum": 0.005009280000000001, "score": 0.0, "glad_count": 166, "ISO": "COG"}, "COG_107831": {"emissions_sum": 0.00019601000000000002, "score": 0.0, "glad_count": 10, "ISO": "COG"}, "COG_107143": {"emissions_sum": 0.0030243700000000006, "score": 8.662598011185299, "glad_count": 91, "ISO": "COG"}, "COG_107832": {"emissions_sum": 1.278e-05, "score": 0.0, "glad_count": 1, "ISO": "COG"}, "COG_108045": {"emissions_sum": 0.0008604400000000001, "score": 0.0, "glad_count": 58, "ISO": "COG"}, "COG_108044": {"emissions_sum": 0.0002810399999999999, "score": 0.0, "glad_count": 22, "ISO": "COG"}, "COG_108046": {"emissions_sum": 0.00073045, "score": 0.0, "glad_count": 56, "ISO": "COG"}}]
# calc_natural_breaks(dict_list)
