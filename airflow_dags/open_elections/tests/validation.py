from airflow_dags.open_elections.ca import metadata
from airflow_dags.open_elections.load_by_state import extract_precinct_data, build_state_dataframes

source_dir = '/Users/oscarbatori/Documents/open-elections/openelections-data-ca'

def check_precinct():
    metadata.set_source_dir(source_dir)
    _, precinct_vote_df = build_state_dataframes(metadata)

    return extract_precinct_data(precinct_vote_df, metadata)
