import os
import argparse
import csv
import subprocess
from osgeo import gdal


def main():

    parser = argparse.ArgumentParser(description='Download GLAD, process rasters, and encode in RGB tiles')
    parser.add_argument('--input_ras', '-i', help='the raster to process', required=True)
    parser.add_argument('--output_csv', '-o', help='the raster to process', required=True)
    args = parser.parse_args()

    # Source: http://gis.stackexchange.com/questions/90726
    # open raster and choose band to find min, max
    gtif = gdal.Open(args.input_ras)
    srcband = gtif.GetRasterBand(1)

    # Get raster statistics
    stats = srcband.GetStatistics(True, True)

    if len(set(stats)) == 1 and stats[0] == 0.0:
        print 'Raster {0} has no valid pixels'.format(os.path.basename(args.input_ras))

        # Create csv placeholder but don't write anything to it
        open(args.output_csv, 'w+')
        
    else:

        # Windows
        if os.name == 'nt':
            import arcpy

            print 'Starting arcpy raster to point'
            temp_shp = os.path.splitext(args.output_csv)[0] + '.shp'
            arcpy.RasterToPoint_conversion(args.input_ras, temp_shp, "VALUE")

            print 'Writing point shapefile to CSV'
            cmd = ['ogr2ogr', '-f', 'CSV', args.output_csv, temp_shp, '-lco', 'GEOMETRY=AS_XY']
            subprocess.check_call(cmd)

        else:
            # -s flag filters input to remove NaN values, greatly increasing speed
            subprocess.check_call(['gmt', 'grd2xyz', args.input_ras, '>', args.output_csv, '-s'])

if __name__ == '__main__':
    main()
