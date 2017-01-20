import os
import shutil
import requests
import subprocess
import logging

import utilities.file_utilities as file_util

download_dict = {
                    'south_america': ['brazil_day2017', 'peru_day2017'],
                    'africa': ['Africa_day_2017n'],
                    'se_asia': ['SEA_day_2017n']
                }


def download_glad(region, data_dir):

    pull_from_s3(region, data_dir)

    multiple_countries = len(download_dict[region]) > 1

    if multiple_countries:
        src_file = build_vrt(download_dict[region], data_dir)

    else:
        src_name = download_dict[region][0]
        src_file = os.path.join(data_dir, src_name + '.tif')

    final_source = os.path.join(data_dir, 'source.tif')

    if multiple_countries:
        final_source = os.path.join(data_dir, 'output.vrt')

    else:
        shutil.move(src_file, final_source)

    file_util.write_marker_txt_file(final_source)

    return final_source


def build_vrt(ras_name_list, data_dir):

    output_vrt = os.path.join(data_dir, 'output.vrt')
    vrt_cmd = ['gdalbuildvrt', output_vrt]

    for ras_name in ras_name_list:
        src_path = os.path.join(data_dir, ras_name + '.tif')

        vrt_cmd.append(src_path)

    subprocess.check_call(vrt_cmd)

    return output_vrt


def vrt_to_tif(vrt_file, output_tif):

    logging.debug('Writing vrt to tif')

    cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '--config', 'GDAL_CACHEMAX', file_util.get_mem_pct()]
    cmd += [vrt_file, output_tif]

    subprocess.check_call(cmd)

    return output_tif


def pull_from_s3(region, data_dir):

    glad_url = r'http://umd-landsat-alerts.s3.amazonaws.com/{0}.tif'

    ras_name_list = download_dict[region]

    for ras_name in ras_name_list:

        url = glad_url.format(ras_name)

        file_name = ras_name + '.tif'
        output_path = os.path.join(data_dir, file_name)

        download_file(url, output_path)


def download_file(url, output_path):
    # Source: http://stackoverflow.com/questions/16694907

    r = requests.get(url)

    with open(output_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
