import os

from processing.job import Job


def create_30_days_mask(region_name, data_dir, q):
    
    print 'Filtering source raster'

    input_raster = os.path.join(data_dir, 'source.tif')

    j = Job('gdal_calc.py', ["--co=COMPRESS=LZW"])

    j.input = input_raster
    output_30days_ras = os.path.join(data_dir, 'last30days.tif')
    j.output = output_30days_ras

    j.arguments += ['-A', input_raster, '--outfile={0}'.format(output_30days_ras)]
    j.arguments += ['--calc=A+65535*(A==0)', '--NoDataValue=65535']

    q.put(j)

    # Create another job
    j = Job('gdal_calc.py', ["--co=COMPRESS=LZW"])

    emissions_dir = os.path.join(os.path.basename(data_dir), 'emissions')
    emissions_ras = os.path.join(emissions_dir, '{0}.tif'.format(region_name))
    emissions_30days_ras = os.path.join(data_dir, 'emissions_30days.tif')

    j.arguments += ['-A', output_30days_ras, '-B', emissions_ras, '--outfile={0}'.format(emissions_30days_ras)]
    j.arguments += ['--calc=A+B']

    j.input = [output_30days_ras, emissions_ras]
    j.output = emissions_30days_ras

    q.put(j)

    return emissions_30days_ras
