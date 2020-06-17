from airflow_dags.open_elections.ca import metadata
from airflow_dags.open_elections.load_by_state import parse_for_state

data_dir = '/Users/oscarbatori/Documents/open-elections/openelections-data-ca'


def test_reproduction_of_duplicate_pk():
    # ((12797,"743ae437cd843e17273cc2881d4135ec",13575,"101",7803,"REP",12436,"David King"))
    metadata.set_source_dir(data_dir)
    _, all_precinct_votes, _ = parse_for_state(metadata)

    for dic in all_precinct_votes:
        if (dic['election_id'] == '743ae437cd843e17273cc2881d4135ec' and
                dic['precinct'] == '101' and
                dic['party'] == 'REP' and
                dic['candidate'] == 'David King'):
            print(dic)

    print('Done dumping out possible duplicate keys')
