
## Background
[Open Elections](http://openelections.net/) is a project to make high quality election data freely available. The main challenge in doing so is that the various state and county bodies that administer elections do not have uniform reporting methods. At a high level the process involves:
- identifying elections of interest, i.e. defining the scope of the data to be collected
- using some kind of task management tool to break it up into tasks that can be taken on by a volunteer
- checking the submissions and then publishing data

The project is managed via [GitHub](https://github.com/openelections/), where each state has a repository for sources, and a repository containing the CSV files that those sources are transformed into.

## Goal
Our goal is to transform the per state data repositories into a single Dolt repository containing every election result available. This is a database design problem as not all states have the same schema, but they do share some common properties.

## Details
The process works through a script called `load_by_state.py`. It requires:
- Doltpy to be installed
- clone the Open Elections repository you want to load
- clone the Dolt repository containing the data

Then for the state you want to load you need to create a file named `airflow_dags.open_elections.<state>`. For California we have `airflow_dags.open_elections.ca`. That file contains schema information for California:
```python 
from airflow_dags.open_elections.load_by_state import StateMetadata, load_to_dolt
import argparse

PRECINCT_VOTES_CREATE = '''
    CREATE TABLE ca_precinct_votes (
        `election_id` VARCHAR(256),
        `precinct` VARCHAR(256),
        `party` VARCHAR(256),
        `candidate` VARCHAR(256),
        `votes` INT,
        `mail` INT,
        `election_day` INT,
        `absentee` INT,
        `reg_voters` INT,
        `total_ballots` INT,
        `early_votes` INT,
        `provisional` INT,
        `polling` FLOAT,
        PRIMARY KEY (`election_id`, `precinct`, `party`, `candidate`)
    )
'''

PRECINCT_VOTE_PKS = ['election_id', 'precinct', 'party', 'candidate']

PRECINCT_VOTES_COLS = [
    'election_id',
    'precinct',
    'party',
    'candidate',
    'votes',
    'mail',
    'election_day',
    'absentee',
    'reg_voters',
    'total_ballots',
    'early_votes',
    'provisional',
    'polling',
]

COUNTY_VOTES_CREATE = '''
    CREATE TABLE ca_county_votes (
        `election_id` VARCHAR(256),
        `county` VARCHAR(256),
        `party` VARCHAR(256),
        `candidate` VARCHAR(256),
        `votes` INT,
        PRIMARY KEY (`election_id`, `county`, `party`, `candidate`)
    )
'''

COUNTY_VOTE_PKS = ['election_id', 'county', 'party', 'candidate']

VOTE_COUNT_COLS = ['votes',
                   'mail',
                   'election_day',
                   'absentee',
                   'reg_voters',
                   'total_ballots',
                   'early_votes',
                   'provisional']
```

We can then pass this module to the `load_by_state.py` script as follows:
```
$ cd liquidata-etl-jobs && export PYTHONPATH=.
$ python airflow_dags/open_elections/tools.py --base-dir path/to/openelections-data-ca --dolt-dir path/to/open-elections --state-module airflow_dags.open_elections.ca --load-precinct-votes --load-elections --load-county-votes
```

This will grab all the CSVs in the repository, extract metadata from the file paths, and then create Python data structures representing the tables. It may be necessary to tweak the script to account for oddities in a given state's data. For example the following code is used to fix types:
```python
if key in ('election_id', 'state', 'year', 'date', 'election', 'special', 'election'):
    # We injected these columns so we don't need to worry about the type
    pass
elif key in ('candidate', 'precinct', 'county', 'party'):
    if value is None or pd.isna(value):
        dic[key] = DEFAULT_PK_VALUE
elif key == 'election_id':
    pass
elif key == 'polling':
    if pd.isna(value):
        dic[key] = None
elif key in vote_count_cols:
    if type(value) == str:
        if value in ('X', '-', '', 'S'):
            dic[key] = None
        else:
            dic[key] = int(value.replace(',', ''))
    elif pd.isna(value):
        dic[key] = None
    elif type(value) == int:
        pass
    elif type(value) == float:
        dic[key] = int(value)
    else:
        raise ValueError('Value {} is not valid or vote ("{}") column'.format(value, key))
elif key == 'polling':
    pass
else:
    raise ValueError('Encountered unknown column {}'.format(key))
```

You should add special cases as they arise to get the loader working. 