from airflow_dags.open_elections.load_by_state import StateMetadata

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

COUNTY_VOTE_PKS = ['election_id', 'county', 'party', 'candidate']

VOTE_COUNT_COLS = ['votes',
                   'mail',
                   'election_day',
                   'absentee',
                   'reg_voters',
                   'total_ballots',
                   'early_votes',
                   'provisional']

metadata = StateMetadata(None, 'ca', VOTE_COUNT_COLS, COUNTY_VOTE_PKS, PRECINCT_VOTE_PKS, None, None)
