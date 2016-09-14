import os

from processing.job import Job


def convert(tile_dict, data_dir, script_dir, q):

    for tile_id in tile_dict.keys():
        j = Job('python')

        emissions_raster = os.path.join(data_dir, 'emissions_30days', '{0}.tif'.format(tile_id))

        script_path = os.path.join(script_dir, 'raster_to_point.py')
        output_csv = os.path.join(data_dir, 'emissions_30days_point', '{0}.csv'.format(tile_id))

        j.input = emissions_raster
        j.output = output_csv

        j.arguments = [script_path, '-i', emissions_raster, '-o', j.output]
        q.put(j)
