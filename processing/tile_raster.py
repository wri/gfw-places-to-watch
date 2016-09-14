import os
import subprocess
import fiona
from shapely.geometry import shape

import processing.utilities.file_utilities as file_util
from processing.job import Job


def build_projwin_jobs(source_raster, data_dir, grid_shp, is_test, q):

    tile_dict = build_tile_dict(data_dir, grid_shp, source_raster)

    if is_test:
        first_key = tile_dict.keys()[0]
        first_value = tile_dict[first_key]
        tile_dict = {first_key: first_value}

    create_jobs_from_dict(tile_dict, source_raster, data_dir, q)

    return tile_dict


def build_tile_dict(data_dir, grid_shp, source_raster):

    index_filename = 'data_index.shp'
    source_filename = os.path.basename(source_raster)

    subprocess.check_call(['gdaltindex', index_filename, source_filename], cwd=data_dir)

    index_shp = os.path.join(data_dir, index_filename)

    tile_dict = {}

    with fiona.open(grid_shp, 'r') as grid:
        with fiona.open(index_shp, 'r') as index:
            for g in grid:
                for i in index:
                    if shape(g['geometry']).intersects(shape(i['geometry'])):
                        coords = g['geometry']['coordinates'][0]
    
                        upper_left = coords[1]
                        lower_right = coords[3]

                        tile_id = g['properties']['uniqueid']
                        coord_list_int = [upper_left[0], upper_left[1], lower_right[0], lower_right[1]]
                        coord_list = [str(round(x)) for x in coord_list_int]

                        tile_dict[tile_id] = coord_list

    return tile_dict


def create_jobs_from_dict(tile_dict, source_raster, data_dir, q):

    mem_pct = file_util.get_mem_pct()
    tif_dir = os.path.join(data_dir, 'glad_source')

    for tile_id, bbox in tile_dict.iteritems():
        j = Job('gdal_translate')

        output_ras = os.path.join(tif_dir, '{0}.tif'.format(tile_id))

        j.arguments = [source_raster, '--config', 'GDAL_CACHEMAX', mem_pct, '-co', 'COMPRESS=LZW', output_ras]
        j.arguments += ['-projwin'] + bbox

        j.input = source_raster
        j.output = output_ras

        q.put(j)
