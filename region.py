import os
import logging

from processing import raster_math, build_conversion_jobs


class Region(object):

    def __init__(self, region_name, source_dir, q):

        self.name = region_name
        self.source_dir = source_dir
        self.q = q

        logging.debug('Starting region {0}'.format(self.name))

        self.root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.script_dir = os.path.join(self.root_dir, 'processing', 'utilities')
        self.data_dir = os.path.join(self.root_dir, 'data', self.name)

    def prep_raster_data(self):
        print 'Starting to prep glad data'
        raster_math.create_30_days_mask(self.name, self.source_dir, self.data_dir, self.q)

    def emissions_to_point(self):
        print 'Converting output raster to points'

        build_conversion_jobs.convert(self.data_dir, self.script_dir, self.q)

    def classify_point_data_by_grid_cell(self):
        print 'Updating sqlite data'

    def tabulate_results(self):
        print 'tabulating results'

