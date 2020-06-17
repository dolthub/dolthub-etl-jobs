from airflow_dags.open_elections.load_by_state import StateMetadata

PRECINCT_VOTE_PKS = ['election_id', 'precinct', 'party', 'candidate']

PRECINCT_VOTES_COLS = ['election_id',
                       'precinct',
                       'party',
                       'candidate',
                       'absentee',
                       'election_day',
                       'votes',
                       'assembly district',
                       'election district',
                       'machine',
                       'affidavit',
                       'federal',
                       'absentee_affidavit',
                       'absentee_hc',
                       'military',
                       'uocava',
                       'assembly_district',
                       'public_counter_votes',
                       'emergency_votes',
                       'absentee_military_votes',
                       'federal_votes',
                       'affidavit_votes',
                       'machine_votes']

COUNTY_VOTE_PKS = ['election_id', 'county', 'party', 'candidate']

COUNTY_VOTE_COLS = ['election_id', 'county',  'district', 'party', 'candidate', 'votes']

VOTE_COUNT_COLS = ['absentee',
                   'election_day',
                   'votes',
                   'assembly district',
                   'election district',
                   'machine',
                   'affidavit',
                   'federal',
                   'absentee_affidavit',
                   'absentee_hc',
                   'military',
                   'uocava',
                   'assembly_district',
                   'public_counter_votes',
                   'emergency_votes',
                   'absentee_military_votes',
                   'federal_votes',
                   'affidavit_votes',
                   'machine_votes']

df_transformers = [lambda df: df.drop(columns=['winner'], errors='ignore'),
                   lambda df: df.rename(columns={'vote': 'votes'}, errors='ignore')]

metadata = StateMetadata(None,
                         'ny',
                         VOTE_COUNT_COLS,
                         COUNTY_VOTE_PKS,
                         PRECINCT_VOTE_PKS,
                         df_transformers=df_transformers)
