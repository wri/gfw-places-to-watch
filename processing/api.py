import os
import requests


def push_to_carto(result_rows, root_dir):

    username = 'wri-01'
    token = get_token(username, root_dir)
    table_name = 'ptw_top_breaks'

    # Note the {{ }} around sql-- this allows us to format this string in two places
    # First we add username/token here, then we can add sql later
    url = r'https://{username}.carto.com/api/v2/sql?q={{sql}}&api_key={token}'.format(username=username, token=token)

    truncate_table(url, table_name)

    push_results(url, table_name, result_rows)


def data_to_string(data):

    str_values = []

    for val in data:

        # Carto requires strings to have '' around them
        if isinstance(val, basestring):
            str_values.append("'{0}'".format(val))
        else:
            str_values.append(str(val))

    return ', '.join(str_values)


def get_token(account_id, root_dir):

    account_name = '{0}@cartodb'.format(account_id)
    local_token = os.path.join(root_dir, 'tokens', account_name)

    token = None

    with open(local_token, "r") as f:
        for row in f:
            token = row
            break

    return token


def truncate_table(url, table_name):

    sql = 'DELETE FROM {0}'.format(table_name)
    r = requests.get(url.format(sql=sql))

    validate_response(r)


def push_results(url, table_name, result_rows):

    col_names = ['emissions_sum', 'glad_count', 'grid_id', 'iso', 'score']
    template_sql = 'INSERT INTO {table_name} ( {cols} ) VALUES ( {values} )'

    for row in result_rows:
        data = [row[col] for col in col_names]

        col_str = ', '.join(col_names)
        data_str = data_to_string(data)

        sql = template_sql.format(table_name=table_name, cols=col_str, values=data_str)

        r = requests.get(url.format(sql=sql))
        validate_response(r)


def validate_response(r):

    if r.status_code == 200 and 'error' not in r.json().keys():
        print 'Request succeeded'
    else:
        raise ValueError('Request failed. Response: ', r.json())