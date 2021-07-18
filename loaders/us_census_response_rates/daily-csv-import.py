#!/usr/local/bin/python3

import csv
import datetime
import urllib.request
import io

from doltpy.core import Dolt
from doltpy.core.write import bulk_import

geo_column_map = {
    'GEO_ID': 'geo_id',
    'Geo_Type': 'geo_type',
    'State': 'state',
    'Geo_Name': 'name',
}

data_column_map = {
    'GEO_ID': 'geo_id',
    'RESP_DATE': 'response_date',
    'DRRINT': 'daily_response_rate_internet',
    'DRRALL': 'daily_reponse_rate_all',
    'CRRINT': 'cumulative_response_rate_internet',
    'CRRALL': 'cumulative_reponse_rate_all',
    'DINTMIN': 'min_daily_repsonse_rate_internet',
    'DMIN': 'min_daily_repsonse_rate',
    'CINTMIN': 'min_cumulative_response_rate_internet',
    'CMIN': 'min_cumulative_response_rate',
    'DINTMAX': 'max_daily_repsonse_rate_internet',
    'DMAX': 'max_daily_repsonse_rate',
    'CINTMAX': 'max_cumulative_response_rate_internet',
    'CMAX': 'max_cumulative_response_rate',
    'DINTAVG': 'avg_daily_repsonse_rate_internet',
    'DAVG': 'avg_daily_repsonse_rate',
    'CINTAVG': 'avg_cumulative_response_rate_internet',
    'CAVG': 'avg_cumulative_response_rate',
    'DINTMED': 'med_daily_repsonse_rate_internet',
    'DMED': 'med_daily_repsonse_rate',
    'CINTMED': 'med_cumulative_response_rate_internet',
    'CMED': 'med_cumulative_response_rate',
}

pks = ['geo_id']

data_url = 'https://www2.census.gov/programs-surveys/decennial/2020/data/2020map/2020/decennialrr2020.csv'
geo_url = 'https://www2.census.gov/programs-surveys/decennial/2020/data/2020map/2020/decennialrr2020_crosswalkfile.csv'

org = 'Liquidata'
repo_name = 'us-census-response-rates'

target = f'{org}/{repo_name}'

print(f'Cloning {target}')
repo = Dolt.clone(target, '.')

# Import GEO mapping table
outcsvfile = 'geo.csv'
print(f'Reading {geo_url}')
with urllib.request.urlopen(geo_url) as response, open(outcsvfile, "w") as outcsvhandle:
    csvreader = csv.reader(io.StringIO(response.read().decode('latin1')))
    csvwriter = csv.writer(outcsvhandle)

    header = next(csvreader)

    header_out = []
    for col in header:
        if geo_column_map.get(col):
            header_out.append(geo_column_map.get(col))
        else:
            raise Exception(f'{col} not found in column map')

    csvwriter.writerow(header_out)
    
    for row in csvreader:
        csvwriter.writerow(row)

    print('Importing to Dolt')
    bulk_import(repo, 'geos', open(outcsvfile), ['geo_id'], 'replace')
    
outcsvfile = 'data.csv'
print(f'Reading {data_url}')
with urllib.request.urlopen(data_url) as response, open(outcsvfile, "w") as outcsvhandle:
    csvreader = csv.reader(io.StringIO(response.read().decode('latin1')))
    csvwriter = csv.writer(outcsvhandle)

    print('Converting to Dolt format')
    header = next(csvreader)

    header_out = []
    for col in header:
        if data_column_map.get(col):
            header_out.append(data_column_map.get(col))
        else:
            raise Exception(f'{col} not found in column map')
        
    csvwriter.writerow(header_out)

    for row in csvreader:
        # Convert dates
        response_date = datetime.datetime.strptime(row[1], '%Y-%m-%d')
        row[1] = response_date.date()

        csvwriter.writerow(row)


    bulk_import(repo, 'responses', open(outcsvfile), pks, 'replace')

if repo.status().is_clean:
    print('No changes to repo. Exiting')
else:
    print('Commiting and pushing to DoltHub')
    repo.add('.')

    now = datetime.datetime.now()
    print(f'Latest data downloaded from {data_url} at {now}')
    repo.commit(f'Latest data downloaded from {data_url} at {now}')
    repo.push('origin', 'master')
