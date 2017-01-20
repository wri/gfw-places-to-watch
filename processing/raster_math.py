import os
import datetime
import time

from processing.job import Job
from utilities import file_utilities


def create_30_days_mask(tile_dict, data_dir, q):

    glad_tile_dir = os.path.join(data_dir, 'glad_source')
    glad30days_dir = os.path.join(data_dir, 'glad_30days')
    emissions_clip_dir = os.path.join(data_dir, 'emissions_clip')
    emissions30_days_dir = os.path.join(data_dir, 'emissions_30days')

    for tile_id, coord_list in tile_dict.iteritems():

        filter_glad_30days(tile_id, glad_tile_dir, glad30days_dir, q)

        clip_emissions(tile_id, coord_list, emissions_clip_dir, q)

        extract_emissions_30days(tile_id, glad30days_dir, emissions_clip_dir, emissions30_days_dir, q)


def filter_glad_30days(tile_id, input_dir, output_dir, q):

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
    j.to_delete = input_raster

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


def clip_emissions(tile_id, bbox_coords, output_dir, q):

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    local_data_dir = os.path.join(os.path.dirname(root_dir), 'data')
    emissions_ras = os.path.join(local_data_dir, 'emissions', 'emissions.vrt')
    emissions_clip = os.path.join(output_dir, '{0}.tif'.format(tile_id))

    j = Job('gdal_translate', ['-co', 'COMPRESS=LZW', '--config', 'GDAL_CACHEMAX', file_utilities.get_mem_pct()])

    j.arguments += ['-projwin'] + bbox_coords
    j.arguments += [emissions_ras, emissions_clip]

    j.input = emissions_ras
    j.output = emissions_clip

    q.put(j)


def extract_emissions_30days(tile_id, glad30days_dir, emissions_clip_dir, emissions_30days_dir, q):

    if os.name == 'nt':
        gdal_calc_path = r'C:\Program Files\GDAL\gdal_calc.py'
        j = Job('python', [gdal_calc_path])

    else:
        j = Job('gdal_calc.py', [])

    tif_filename = '{0}.tif'.format(tile_id)

    glad30days = os.path.join(glad30days_dir, tif_filename)
    emissions_clip = os.path.join(emissions_clip_dir, tif_filename)
    emissions_30days = os.path.join(emissions_30days_dir, tif_filename)

    j.arguments += ['-A', glad30days, '-B', emissions_clip, '--outfile={0}'.format(emissions_30days)]
    j.arguments += ['--calc=A*B', "--co=COMPRESS=LZW"]

    j.input = [glad30days, emissions_clip]
    j.output = emissions_30days
    j.to_delete = [glad30days, emissions_clip]

    q.put(j)
