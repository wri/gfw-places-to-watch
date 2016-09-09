import os
import datetime
import time

from processing.job import Job
from utilities import raster_utilities, file_utilities


def create_30_days_mask(region_name, data_dir, q):
    
    print 'Filtering source raster'

    input_raster = os.path.join(data_dir, 'source.tif')

    # Windows can't find gdal_calc.py, apparently
    if os.name == 'nt':
        gdal_calc_path = r'C:\Program Files\GDAL\gdal_calc.py'
        j = Job('python', [gdal_calc_path])

    else:
        j = Job('gdal_calc.py', [])

    j.input = input_raster
    output_30days_ras = os.path.join(data_dir, 'last30days.tif')
    j.output = output_30days_ras

    frm = '%Y.%m.%d'
    todays_date = time.strftime(frm)
    dt = datetime.datetime.strptime(todays_date, frm)
    tt = dt.timetuple()
    day_of_year = tt.tm_yday
    days_30_ago = day_of_year - 30

    j.arguments += ['-A', input_raster, "--co=COMPRESS=LZW", '--outfile={0}'.format(output_30days_ras)]
    j.arguments += ['--calc=0*(A<{0})'.format(days_30_ago), '--calc=1*(A>={0})'.format(days_30_ago)]
    j.arguments += ['--type=Float32']

    q.put(j)

    ### NEW JOB ####

    emissions_dir = os.path.join(os.path.dirname(data_dir), 'emissions')
    emissions_ras = os.path.join(emissions_dir, '{0}.tif'.format(region_name))
    emissions_clip = os.path.join(data_dir, 'emissions.tif')

    j = Job('gdal_translate', ['-co', 'COMPRESS=LZW', '--config', 'GDAL_CACHEMAX', file_utilities.get_mem_pct()])
    # make them the same size
    coords = raster_utilities.GetExtent(input_raster)

    j.arguments += ['-projwin'] + coords
    j.arguments += [emissions_ras, emissions_clip]

    j.input = emissions_ras
    j.output = emissions_clip

    q.put(j)

    #### NEW JOB #####

    if os.name == 'nt':
        gdal_calc_path = r'C:\Program Files\GDAL\gdal_calc.py'
        j = Job('python', [gdal_calc_path])

    else:
        j = Job('gdal_calc.py', [])

    emissions_30days_ras = os.path.join(data_dir, 'emissions_30days.tif')

    j.arguments += ['-A', output_30days_ras, '-B', emissions_clip, '--outfile={0}'.format(emissions_30days_ras)]
    j.arguments += ['--calc=A*B', "--co=COMPRESS=LZW"]

    j.input = [output_30days_ras, emissions_clip]
    j.output = emissions_30days_ras

    q.put(j)

    return emissions_30days_ras
