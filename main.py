#!/usr/bin/env python

import os
import pandas as pd
from sodapy import Socrata
import requests
import numpy as np


ACS_COLS = {
    'S1901_C01_001E': '12m_income',
    'S1902_C01_001E': '12m_mean_income',
    'S2201_C01_001E': 'snap_benefits',
    'S0101_C01_001E': 'population',
    'S2301_C01_003E': 'empl_status_20-24',
    'S2301_C01_004E': 'empl_status_25-29',
    'S1501_C01_001E': 'ed_attain_18-24',
    'S1501_C01_009E': 'hs_grads',
    'S0102_C01_006E': 'race_white',
    'S0102_C01_007E': 'race_black',
    'S0102_C01_005E': 'race_hisp',
    'S0102_C01_006E': 'race_asian',
    'S0102_C01_011E': 'race_other',
    'S0102_C01_021E': 'hh_total',
    'S0102_C01_022E': 'hh_families_total',
    'S0102_C01_023E': 'hh_families_married',
    'S0102_C01_024E': 'hh_families_only_female',
}

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

def get_tract(lat, lon):
    print(lat, lon)
    url = 'https://geocoding.geo.census.gov/geocoder/geographies/coordinates?benchmark=Public_AR_Current&vintage=ACS2018_Current&y=' \
        + str(lat) + '&x=' + str(lon)+ '&format=json'

    try:
        d = requests.get(url).json()
        parent = d['result']['geographies']['2010 Census Blocks'][0]

        return (parent['TRACT'], parent['COUNTY'], parent['STATE'])
    except:
        print('geocode error')
        return None

def get_small_chicago_df(n):
    client = Socrata("data.cityofchicago.org", None)
    code = "6zsd-86xi"
    whereclause = \
        "date between '2017-01-01T00:00:00' and '2019-01-01T00:00:00'"

    res = get_df_from_socrata(client, code, 
                              whereclause=whereclause, max_size=n)
    print('df shape', res.shape)
    return res

def get_big_chicago_df():
    client = Socrata("data.cityofchicago.org", None)
    code = "6zsd-86xi"
    whereclause = \
        "date between '2017-01-01T00:00:00' and '2019-01-01T00:00:00'"

    res = get_df_from_socrata(client, code, whereclause=whereclause)
    print('df shape', res.shape)
    return res

def get_acs_data(acs_name, tract):
    try:
        t_name, county, state = tract
        url = 'https://api.census.gov/data/2017/acs/acs5/subject?get=NAME,' + \
              acs_name +'&for=tract:' + t_name + '&in=state:' + state + \
              '%20county:' + county
        res = requests.get(url).json()

        return res[1][1]
    except:
        print('acs request error')
        return None


def add_acs_data(df): 
    df = df[df['latitude'].notnull()]
    df = df[df['longitude'].notnull()]

    df['tract'] = df.apply(lambda row: get_tract(row['latitude'],
                                                      row['longitude']),
                                axis=1)

    for k, v in ACS_COLS.items():
        df[v] = df.apply(lambda row: get_acs_data(k, row['tract']), axis=1)
    
    df.reset_index()
    return df

def demo(n):
    df = get_small_chicago_df(n)
    augmented = add_acs_data(df)

    print('complete')
    return augmented

