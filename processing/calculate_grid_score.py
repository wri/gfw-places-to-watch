import datetime
from urlparse import urlparse
from StringIO import StringIO

import pandas as pd
import boto3

import flagship, soy, palm


def tabulate_results(s3_path_list, min_date, max_date, analysis_type):

    df = read_result_from_s3(s3_path_list)

    df = filter_by_date_range(df, min_date, max_date)

    if analysis_type == 'flagship':
        top_10_list = flagship.tabulate_and_pick_top10(df)

    elif analysis_type == 'soy':
        top_10_list = soy.tabulate_and_pick_top10(df)

    else:
        top_10_list = palm.tabulate_and_pick_top10(df)

    return top_10_list


def read_result_from_s3(s3_path_list):

    # comes back as a list of lists, apparently
    # possible to do multiple EMR steps (first level of list)
    # and then within that, have multiple query outputs per step

    print "in read result from s3"
    print s3_path_list
    print "\n"
    s3_path = s3_path_list[0][0] #IC***## CHANGE BACK
    #s3_path = s3_path_list   #IC hadoop
    print 'reading results from s3 path: \n{}'.format(s3_path)

    # source: https://stackoverflow.com/a/35376156/4355916
    s3 = boto3.resource('s3')

    # hadoop pip returns a list of outputs; we're only using one
    parsed = urlparse(s3_path)

    # use urlparse to separate the bucket from the path, required for boto3
    obj = s3.Object(parsed.netloc, parsed.path.lstrip('/'))
    csv_string = obj.get()['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_string), header=None)
    '''
    df.columns = ['year', 'julian_day', 'area_ha', 'emissions_sum',
                  'ptw_importance', 'grid_id', 'glad_count']
    '''
    #df.columns = ['year', 'julian_day', 'area_ha', 'grid_id', 'glad_count', 'iso']  #IC added iso
    df.columns = ['year', 'julian_day', 'area_ha', 'grid_id', 'glad_count']  #IC added iso

    return df


def filter_by_date_range(df, min_date, max_date):

    print 'filtering df to remove unnecessary years'

    #max_date = int(max_date)
    #min_date = int(min_date)
    #print min_date
    #print max_date

    #print "\n"

    # remove old years, then do costly jd --> date calculation
    # explicitly making a copy of the df because of settingwithcopy warning
    filtered = df.loc[(df.year >= min_date.year) & (df.year <= max_date.year)].copy()
    filtered['alert_date'] = filtered.apply(lambda row: jd_to_date(row.year, row.julian_day), axis=1)

    # now that the df has an alert_date column, filter using dates
    filtered = filtered.loc[(filtered.alert_date >= min_date) & (filtered.alert_date <= max_date)]

    return filtered


def jd_to_date(year, julian_day):
    # source: https://stackoverflow.com/a/17216581/4355916
    #print "year"
    year = int(year)
    #print year
    #print type(year)
    #print "julian day"
    julian_day = int(julian_day)
    #print julian_day
    #print type(julian_day)
    #print "\n"
    return datetime.date(year, 1, 1) + datetime.timedelta(julian_day - 1)
