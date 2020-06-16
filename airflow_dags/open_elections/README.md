
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

We now step through how to create this file, and then running it through the loader.

### Get State Data
Go to Open Elections [GitHub](https://github.com/openelections) page and select a whose data you would like to extract, the example above is from [California](https://github.com/openelections/openelections-data-ca). Clone it.

Go to DoltHub and clone Open Elections [repository](https://www.dolthub.com/repositories/open-elections/elections-poc).

### Extract State Schema
The first thing to do is extract a schema by running the `load_by_state.py` script as follows:
```
$  python airflow_dags/open_elections/load_by_state.py --state ca --base-dir path/to/open-elections/openelections-data-ca --show-columns
```

`path/to/open-elections/openelections-data-ca` is the directory we just cloned. This script will traverse the repository grabbing CSVs, turn them into DataFrames and state specific column names. Be aware of misnamed columns, for example New York has `vote` and `votes` columns, but we want to `votes`. `ny.py` contains a transformer that fixes this.

### Implement Rules
Implement a list of functions that map a `pd.DataFrame` to a `pd.DataFrame` that encode data clean up rules and will be run on the data before it is loaded.

### Create Tables
Write a state specific SQL file, see `schema_definitions/ca.sql` for example, containing a create statement for the extracted data. Run it against the Dolt repo.

### Execute the Load
You now have created a schema for the votes for your state in your Dolt repo, a module with the required information from the schema in Python, and some rules for cleaning the data. Time to load it:
```
$  python airflow_dags/open_elections/load_by_state.py --state ca --base-dir path/to/open-elections/openelections-data-ca --dolt-dir path/to/open-elections --state-module airflow_dags.open_elections.ca --load-elections --load-precinct-votes --load-county-votes
```
