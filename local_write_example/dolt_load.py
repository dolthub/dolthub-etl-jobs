import pandas as pd
from doltpy_etl import Dataset
from typing import List


def _test_data_generator(name: str, all_time_greatness_rank: 1):
    return {'name': name, 'all_time_greatness': all_time_greatness_rank}


def get_data_builder():
    def inner() -> pd.DataFrame:
        return pd.DataFrame([
            _test_data_generator('Marat Saffin', 1),
            _test_data_generator('Roger Federer', 2),
            _test_data_generator('Novak Djokovic', 3),
            _test_data_generator('Rafael Nadal', 4),
            _test_data_generator('Pete Sampras', 5),
        ])

    return inner


def get_dolt_datasets() -> List[Dataset]:
    return [Dataset('great_players', get_data_builder(), pk_cols=['name'], import_mode='create')]

