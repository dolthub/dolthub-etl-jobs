from airflow_dags.open_elections.load_by_state import StateMetadata

VOTE_COUNT_COLS = ['votes',
                   'mail',
                   'election_day',
                   'absentee',
                   'reg_voters',
                   'total_ballots',
                   'early_votes',
                   'provisional']

metadata = StateMetadata(None, 'ca', VOTE_COUNT_COLS, None, None)
