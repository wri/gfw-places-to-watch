

def download_glad(region_list, data_dir):

    # TODO download files from s3 to the appropriate directories
    # use this opportunity to change filenames (brazil_date2015 vs day2015) and change bit types etc
    # also can use it if we're looking at a --test example-- clip to the test dict

    for region in region_list:
        print 'Downloading recent GLAD for {0} to {1}'.format(region, data_dir)