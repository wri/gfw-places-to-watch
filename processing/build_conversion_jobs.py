import os

from processing.job import Job


def convert(data_dir, script_dir, q):

    j = Job('python')

    emissions_raster = os.path.join(data_dir, 'emissions_30days.tif')

    script_path = os.path.join(script_dir, 'raster_to_point.py')
    output_csv = os.path.splitext(emissions_raster)[0] + '.csv'

    j.input = emissions_raster
    j.output = output_csv

    j.arguments = [script_path, '-i', emissions_raster, '-o', j.output]
    q.put(j)
