import os
import datetime
import time

from processing.job import Job
from utilities import raster_utilities, file_utilities


def create_30_days_mask(tile_dict, data_dir, q):

    glad_tile_dir = os.path.join(data_dir, 'glad_tiles')
    glad30_days_dir = os.path.join(data_dir, 'glad_30days')
    emissions_clip_dir = os.path.join(data_dir, 'emissions_clip')
    emissions30_days_dir = os.path.join(data_dir, 'emissions_30days')

    for tile_id, coord_list in tile_dict.iteritems():

        filter_glad_30days(tile_id, glad_tile_dir, glad30_days_dir)

        clip_emissions(tile_id, coord_list, data_dir, emissions_clip_dir)

        extract_emissions_30days(tile_id, glad30days_dir, emissions_clip_dir, emissions30_days_dir)


def filter_glad_30days(tile_id, input_dir, output_dir):

    input_raster = os.path.join(input_dir, '{0}.tif'.format(tile_id))
    output_raster = os.path.join(output_dir, '{0}.tif'.format(tile_id))

    # Windows can't find gdal_calc.py, apparently
    if os.name == 'nt':
        gdal_calc_path = r'C:\Program Files\GDAL\gdal_calc.py'
        j = Job('python', [gdal_calc_path])

    else:
        j = Job('gdal_calc.py', [])

    j.input = input_raster
    j.output = output_raster

    frm = '%Y.%m.%d'
    todays_date = time.strftime(frm)
    dt = datetime.datetime.strptime(todays_date, frm)
    tt = dt.timetuple()
    day_of_year = tt.tm_yday
    days_30_ago = day_of_year - 30

    j.arguments += ['-A', input_raster, "--co=COMPRESS=LZW", '--outfile={0}'.format(output_raster)]
    j.arguments += ['--calc=0*(A<{0})'.format(days_30_ago), '--calc=1*(A>={0})'.format(days_30_ago)]
    j.arguments += ['--type=Float32', '--NoDataValue=0']

    q.put(j)

def clip_emissions(tile_id, bbox_coords, data_dir, output_dir):

    emissions_dir = os.path.join(os.path.dirname(data_dir), 'emissions')
    emissions_ras = os.path.join(emissions_dir, '{0}.tif'.format(region_name))
    emissions_clip = os.path.join(output_dir, '{0}.tif'.format(tile_id))

    j = Job('gdal_translate', ['-co', 'COMPRESS=LZW', '--config', 'GDAL_CACHEMAX', file_utilities.get_mem_pct()])

    j.arguments += ['-projwin'] + bbox_coords
    j.arguments += [emissions_ras, emissions_clip]

    j.input = emissions_ras
    j.output = emissions_clip

    q.put(j)

def extract_emissions_30days(tile_id, glad30days_dir, emissions_clip_dir, emissions_30days_dir):

    if os.name == 'nt':
        gdal_calc_path = r'C:\Program Files\GDAL\gdal_calc.py'
        j = Job('python', [gdal_calc_path])

    else:
        j = Job('gdal_calc.py', [])

    tif_filename = '{0}.tif'.format(tile_id)

    glad_30days = os.path.join(glad30days_dir, tif_filename)
    emissions_clip = os.path.join(glad30days_dir, tif_filename)
    emissions_30days = os.path.join(emissions_30days_dir, tif_filename)

    j.arguments += ['-A', glad30days, '-B', emissions_clip, '--outfile={0}'.format(emissions_30days)]
    j.arguments += ['--calc=A*B', "--co=COMPRESS=LZW"]

    j.input = [glad30days, emissions_clip]
    j.output = emissions_30days

    q.put(j)

    return emissions_30days
