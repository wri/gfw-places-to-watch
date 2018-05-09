import datetime
from urlparse import urlparse
from StringIO import StringIO

import pandas as pd
import boto3


def tabulate_results(s3_path_list, min_date, max_date, staging):

    df = read_result_from_s3(s3_path_list)

    df = filter_by_date_range(df, min_date, max_date)

    top_10_list = tabulate_and_pick_top10(df)

    return top_10_list


def read_result_from_s3(s3_path_list):

    s3_path = s3_path_list[0]
    print 'reading results from s3 path: \n{}'.format(s3_path)

    # source: https://stackoverflow.com/a/35376156/4355916
    s3 = boto3.resource('s3')

    # hadoop pip returns a list of outputs; we're only using one
    parsed = urlparse(s3_path)

    # use urlparse to separate the bucket from the path, required for boto3
    obj = s3.Object(parsed.netloc, parsed.path.lstrip('/'))
    csv_string = obj.get()['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_string), header=None)
    df.columns = ['year', 'julian_day', 'area_ha', 'emissions_sum',
                  'ptw_importance', 'grid_id', 'glad_count']

    return df


def filter_by_date_range(df, min_date, max_date):

    print 'filtering df to remove unnecessary years'

    # remove old years, then do costly jd --> date calculation
    # explicitly making a copy of the df because of settingwithcopy warning
    filtered = df.loc[(df.year >= df.min_date.year) & (df.year <= df.max_date.year)].copy()
    filtered['alert_date'] = filtered.apply(lambda row: jd_to_date(row.year, row.julian_day), axis=1)

    # now that the df has an alert_date column, filter using dates
    filtered = filtered.loc[(filtered.alert_date >= min_date) & (filtered.alert_date <= max_date)]

    return filtered


def tabulate_and_pick_top10(df):

    print 'tabulating score per grid cell and selecting top 10 by region'

    # group alerts by grid_id/importance, summing, count, area_ha and emissions
    sum_list = ['glad_count', 'area_ha', 'emissions_sum']
    grouped = df.groupby(['grid_id', 'ptw_importance'])[sum_list].sum().reset_index()

    # calculate PTW score based on GLAD count * importance
    grouped['score'] = grouped.ptw_importance * grouped.glad_count

    # can drop ptw_importance now- not required for output
    del grouped['ptw_importance']

    # any score we care about must be > 0.75
    grouped = grouped.loc[grouped.score > 0.75]

    # add region based on grid_id column
    grouped['region'] = grouped.apply(lambda row: grid_id_to_region(row['grid_id']), axis=1)

    # group by region, taking top 10 score for each
    top_10 = grouped.groupby('region')['score'].nlargest(10).reset_index()

    # remove all columns from this df except for level_1, which corresponds to
    # the index of our rows of interest in the grouped df
    top_10 = top_10['level_1'].to_frame()

    # join back to grouped to get auxiliary grid_id, score, region data etc
    final_df = pd.merge(grouped, top_10, left_index=True, right_on='level_1')

    return final_df.to_dict(orient='records')


def jd_to_date(year, julian_day):
    # source: https://stackoverflow.com/a/17216581/4355916
    return datetime.date(year, 1, 1) + datetime.timedelta(julian_day - 1)


def grid_id_to_region(grid_id):
    return '_'.join(grid_id.split('_')[:-1])

