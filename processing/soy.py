import pandas as pd


def tabulate_and_pick_top10(df):
    print "soy table"
    print 'tabulating score per soy grid cell and selecting top 10'

    # group alerts by grid_id/importance, summing, count, area_ha and emissions
    sum_list = ['glad_count', 'area_ha']
    grouped = df.groupby(['grid_id'])[sum_list].sum().reset_index()

    # calculate PTW score based on GLAD count * importance
    grouped['score'] =  grouped.glad_count

    # can drop ptw_importance now- not required for output
    #del grouped['ptw_importance']

    # any score we care about must be > 0.75
    #grouped = grouped.loc[grouped.score > 0.75]

    # add region based on grid_id column
    grouped['region'] = grouped.apply(lambda row: grid_id_to_region(row['grid_id']), axis=1)

    # add iso based on grid_id column
    grouped['iso'] = grouped.apply(lambda row: grid_id_to_iso(row['grid_id']), axis=1)

    # group by grid_id, taking top 10 score for each
    top_10 = grouped.groupby('region')['score'].nlargest(10).reset_index()  #should I delete this after regions are corrected to SA countries - IC? ansewr is yes!
    #top_10 = grouped.groupby('grid_id')['score'].nlargest(10).reset_index()

    # remove all columns from this df except for level_1, which corresponds to
    # the index of our rows of interest in the grouped df
    top_10 = top_10['level_1'].to_frame()

    # join back to grouped to get auxiliary grid_id, score, region data etc
    final_df = pd.merge(grouped, top_10, left_index=True, right_on='level_1')

    return final_df.to_dict(orient='records')


def grid_id_to_region(grid_id):
    grid_id = str(grid_id) #IC
    #region = grid_id.split('_')[0:1]
    #return '_'.join(grid_id.split('_')[:-1])
    return '_'.join(grid_id.split('_')[0:1])

def grid_id_to_iso(grid_id): #ic added function for iso field to show state
    grid_id = str(grid_id) #IC
    #iso = grid_id.split('_')[2:3]
    #return '_'.join(grid_id.split('_')[:-1])
    return '_'.join(grid_id.split('_')[2:3])
