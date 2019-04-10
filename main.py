#!/usr/bin/env python

import os
import pandas as pd
from sodapy import Socrata
import requests

'''
1.1 Download Data
'''

def get_data_chunk(client, code, offset, limit, whereclause=None):
    if whereclause is not None:
        return client.get(code,
                          where=whereclause,
                          limit=limit,
                          offset=offset)
    else:
        return client.get(code,
                          limit=limit,
                          offset=offset)

def get_df_from_socrata(client, code, whereclause=None, max_size=None):
# Max size must be less than 50k

    offset = 0
    combined_data = []

    if max_size is None:
        limit = 50000
    else:
        limit = max_size
    
    chunk = get_data_chunk(client, code, offset, limit, whereclause)
    while len(chunk) > 0:
        print('Processing chunk....')
        combined_data.extend(chunk)

        if max_size is None:
            offset += limit + 1
            chunk = get_data_chunk(client, code, offset, limit, whereclause)
        
        elif len(combined_data) < max_size:
            offset += limit + 1
            limit = max_size - limit
            chunk = get_data_chunk(client, code, offset, limit, whereclause)
        else:
            break
    return pd.DataFrame.from_records(combined_data)

def get_census_tract(lat, lon):
    url = 'https://geocoding.geo.census.gov/geocoder/geographies/coordinates?benchmark=Public_AR_Current&vintage=ACS2018_Current&'
    url += 'y=' + str(lat) + '&x=' + str(lon)+ '&format=json'

    d = requests.get(url).json()
    try:
        res = d['result']['geographies']['2010 Census Blocks'][0]['TRACT']
        return res
    except:
        print('geocode error')
        return None

'''
Combine Chicago data with ACS data
'''

def get_small_chicago_df():
    client = Socrata("data.cityofchicago.org", None)
    code = "6zsd-86xi"
    whereclause = \
        "date between '2017-01-01T00:00:00' and '2019-01-01T00:00:00'"

    res = get_df_from_socrata(client, code, whereclause=whereclause, max_size=1000)
    print('shape', res.shape)
    return res

def get_big_chicago_df():
    client = Socrata("data.cityofchicago.org", None)
    code = "6zsd-86xi"
    whereclause = \
        "date between '2017-01-01T00:00:00' and '2019-01-01T00:00:00'"

    res = get_df_from_socrata(client, code, whereclause=whereclause)
    print('shape', res.shape)
    return res

def test_geo_get():
    url = 'https://geocoding.geo.census.gov/geocoder/geographies/coordinates?benchmark=Public_AR_Current&vintage=ACS2018_Current&y=41.843778126&x=-87.694637678&format=json'

    d = requests.get(url).json()
    return d
