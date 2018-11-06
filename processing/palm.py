

def tabulate_and_pick_top10(df):

    print 'doing some palm process!'

    # must return our output in this 'records' format
    # so that we can upload it to CARTO properly
    return df.to_dict(orient='records')
