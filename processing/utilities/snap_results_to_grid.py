import os
import multiprocessing
import csv
import json
import operator
import glob
from osgeo import osr, ogr

from processing.utilities import multiprocessing_mapreduce


def snap_alerts_to_grid(input_csv):
    """Read a CSV and return a list snapped grid values
    """

    print multiprocessing.current_process().name, 'reading', os.path.basename(input_csv)
    output = []

    with open(input_csv, 'r') as f:
        csv_reader = csv.reader(f)

        # Skip header
        csv_reader.next()

        for row in csv_reader:
            lat_val = float(row[1])
            lon_val = float(row[0])
            emiss_val = float(row[3])

            eck_y, eck_x = wgs84_to_eckert_vi(lat_val, lon_val)

            snap_y = round((eck_y - 5000 + 144.58445) / 10000) * 10000 - 144.58445
            snap_x = round((eck_x - 5000 + 3593.06028) / 10000) * 10000 - 3593.06028

            # output.append(((snap_y, snap_x), (1, emiss_val)))
            output.append(((snap_y, snap_x), (1, emiss_val)))

    return output


def wgs84_to_eckert_vi(lat_val, lon_val):

    source = osr.SpatialReference()
    source.ImportFromEPSG(4326)

    target = osr.SpatialReference()
    target.ImportFromEPSG(54010)

    transform = osr.CoordinateTransformation(source, target)

    wgs84_dict = {'type': 'Point', 'coordinates': [lon_val, lat_val]}
    wgs84_json = json.dumps(wgs84_dict)

    point = ogr.CreateGeometryFromJson(wgs84_json)
    point.Transform(transform)

    eck_json = point.ExportToJson()
    eck_dict = json.loads(eck_json)

    eck_x, eck_y = eck_dict['coordinates']

    return eck_y, eck_x


def sum_alerts(item):
    """Convert the partitioned data for a word to a
    tuple containing the word and the number of occurrences.
    """
    grid_id, result_tuple = item

    return grid_id, map(sum, zip(*result_tuple))


def count_alerts_by_grid(root_dir, region_list, threads):

    input_files = []

    for r in region_list:
        region_dir = os.path.join(root_dir, 'data', r)
        csv_files = glob.glob(region_dir + '/*.csv')

        input_files.extend(csv_files)

    mapper = multiprocessing_mapreduce.SimpleMapReduce(snap_alerts_to_grid, sum_alerts, threads)
    grid_counts = mapper(input_files)

    print 'Map Reduce job complete.'

    output_dict = {}

    for lower_left_corner, results_tuple in grid_counts:
        output_dict[lower_left_corner] = {'glad_count': results_tuple[0], 'emissions_sum': results_tuple[1]}

    return output_dict

