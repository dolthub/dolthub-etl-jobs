from airflow_dags.open_elections.load_by_state import StateMetadata

VOTE_COUNT_COLS = ['votes',
                   'mail',
                   'election_day',
                   'absentee',
                   'reg_voters',
                   'total_ballots',
                   'early_votes',
                   'provisional']

transformers = [lambda df: df.drop(columns=['early_voting']) if 'early_voting' in df.columns else df]

metadata = StateMetadata(None, 'ca', VOTE_COUNT_COLS, transformers, None)
