import pandas as pd

def tabulate_and_pick_top10(df):

    print 'doing some palm process!'
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
    # must return our output in this 'records' format
    # so that we can upload it to CARTO properly


def grid_id_to_region(grid_id):
    grid_id = str(grid_id)
    return '_'.join(grid_id.split('_')[:-1])
