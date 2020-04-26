#!/usr/bin/python3

import datetime
import json
import pandas

from doltpy.core import Dolt
from pprint import pprint

column_map = {
    'name': 'name',
    'justice_link': 'oyez_link',
    'wikipedia_link': 'wikipedia_link',
    'start_date': 'start_date',
    'end_date': 'end_date',
    'person' : {
        'gender' : 'gender',
        'date_of_birth': 'date_of_birth',
        'place_of_birth': 'place_of_birth',
        'date_of_death': 'date_of_death',
        'place_of_death': 'place_of_death',
        'interred': 'interred',
        'origin': 'origin',
        'ethnic': 'ethnic', 
        'race': 'race',
        'religion' : 'religion',
        'family_status': 'family_status',
        'mother': 'mother',
        'father': 'father',
        'mothers_occupation': 'mothers_occupation',
        'fathers_occupation': 'fathers_occupation',
    },
    'role': {
        'appointing_president': 'appointing_president',
        'appointing_party': 'appointing_party',
        'start_affiliation': 'start_affiliation',
        'end_affiliation': 'start_affiliation',
        'party_switch_year': 'party_switch_year',
        'date_appointed': 'date_appointed',
        'date_commissioned': 'date_commissioned',
        'date_sworn_in': 'date_sworn_in',
        'seat': 'seat',
        'reason_for_leaving': 'reason_for_leaving',
        'preceded_by': 'preceded_by',
        'succeeded_by': 'succeeded_by'
        }
    }

date_cols = [
    'date_of_birth',
    'date_of_death',
    'date_appointed',
    'date_commissioned',
    'date_sworn_in',
]

date_3cols = [
    'start_date',
    'end_date'
]

def normalize(data, mapper, output):
    for key in dict.keys(mapper):
        if isinstance(mapper[key], dict):
            normalize(data[key], mapper[key], output)
        else:
            output[mapper[key]] = data[key]

def convert_dates(data, cols):
    for col in cols:
        if data[col] == '':
            continue
        data[col] = datetime.datetime.strptime(data[col], '%b %d, %Y')

def convert_dates_3col(data, cols):
    for col in cols:
        year  = data[col]['year']
        month = data[col]['month']
        day   = data[col]['day']

        if (year == 'None' or year == '' or
            month == 'None' or month == '' or
            day == 'None' or day == ''):
            data[col] = ''
        else:
            data[col] = datetime.datetime.strptime(f'{month} {day}, {year}',
                                                   '%b %d, %Y')

#
# Coerce the JSON file into a flat dictionary
#
with open("supreme-court-cases/justices.js") as file:
    justice_dict = json.load(file)

justices = []
for justice_name in dict.keys(justice_dict):
    justice = justice_dict[justice_name]
    output = {}
    normalize(justice, column_map, output)
    convert_dates(output, date_cols)
    convert_dates_3col(output, date_3cols)
    justices.append(output)

#
# Import into Dolt
#
justices_df = pandas.DataFrame(justices)

# Convert boolean columns using pandas
justices_df[["ethnic"]] *= 1

repo = Dolt('./')
repo.import_df('justices', justices_df, ['name'], import_mode='replace')

