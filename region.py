import os
import logging

from processing import raster_math, tile_raster, build_conversion_jobs, download_glad_source
from processing.utilities import file_utilities as file_util


class Region(object):

    def __init__(self, region_name, is_test, q):

        self.name = region_name
        self.is_test = is_test
        self.q = q

        logging.debug('Starting region {0}'.format(self.name))

        self.root_dir = os.path.dirname(os.path.abspath(__file__))
        self.script_dir = os.path.join(self.root_dir, 'processing', 'utilities')
        self.data_dir = os.path.join(self.root_dir, 'data', self.name)

        self.tile_dict = None

        # remove all files from data_dir before we start this process
        file_util.remove_all_files(self.data_dir)

    def download_source(self):

        print 'Downloading glad from source'
        download_glad_source.download_glad(self.name, self.data_dir, self.is_test)

    def prep_raster_data(self):
        print 'Starting to prep glad data'

        tile_grid_shp = os.path.join(self.root_dir, 'data', 'footprint_5degrees.shp')
        tile_dict = tile_raster.build_projwin_jobs(self.data_dir, tile_grid_shp, self.q)

        raster_math.create_30_days_mask(self.name, self.data_dir, self.q)

    def emissions_to_point(self):
        print 'Converting output raster to points'
        build_conversion_jobs.convert(self.data_dir, self.script_dir, self.q)