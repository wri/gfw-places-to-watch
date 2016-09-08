import os

from processing.job import Job


def create_30_days_mask(region_name, source_dir, data_dir, q):
    
    print 'Filtering source raster'

    input_raster = os.path.join(source_dir, '{0}_day2016.tif'.format(region_name))

    j = Job('gdal_calc.py', ["--co=COMPRESS=LZW"])

    j.input = input_raster
    output_30days_ras = os.path.join(data_dir, 'last30days.tif')
    j.output = output_30days_ras

    j.arguments += ['-A', input_raster, '--outfile={0}'.format(output_30days_ras)]
    j.arguments += ['--calc=A+65535*(A==0)', '--NoDataValue=65535']

    q.put(j)

    # Create another job
    j = Job('gdal_calc.py', ["--co=COMPRESS=LZW"])

    emissions_ras = os.path.join(source_dir, '{0}_emissions.tif'.format(region_name))
    emissions_30days_ras = os.path.join(data_dir, 'emissions_30days.tif')

    j.arguments += ['-A', output_30days_ras, '-B', emissions_ras, '--outfile={0}'.format(emissions_30days_ras)]
    j.arguments += ['--calc=A+B']

    j.input = [output_30days_ras, emissions_ras]
    j.output = emissions_30days_ras

    q.put(j)

    return emissions_30days_ras
