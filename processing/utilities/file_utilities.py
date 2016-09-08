import os
import multiprocessing


def get_mem_pct():

    num_cores = multiprocessing.cpu_count()

    memory_parts = num_cores + 1

    mem_pct = int((1. / float(memory_parts)) * 100)

    mem_pct_text = '{0}%'.format(mem_pct)

    return mem_pct_text


def remove_all_files(input_dir):

    for root, dirs, files in os.walk(input_dir):
        for f in files:
            os.remove(os.path.join(root, f))


def remove_all_dirs(input_dir):

    for root, dirs, files in os.walk(input_dir):
        for d in dirs:
            os.rmdir(os.path.join(root, d))


def write_marker_txt_file(raster_path):
    output_file = os.path.splitext(raster_path)[0] + '.txt'

    with open(output_file, 'wb') as theFile:
        theFile.write('Complete')
