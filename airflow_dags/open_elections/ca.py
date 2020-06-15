from airflow_dags.open_elections.tools import StateMetadata, load_to_dolt
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


