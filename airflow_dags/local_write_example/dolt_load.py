import pandas as pd
from doltpy_etl import get_df_table_loader
import logging

logger = logging.getLogger(__name__)


def _test_data_generator(name: str, all_time_greatness_rank: 1):
    return {'name': name, 'all_time_greatness': all_time_greatness_rank}


def get_data_builder():
    def inner() -> pd.DataFrame:
        logger.info('Generating example data for some great players')
        return pd.DataFrame([
            _test_data_generator('Marat Saffin', 1),
            _test_data_generator('Roger Federer', 2),
            _test_data_generator('Novak Djokovic', 3),
            _test_data_generator('Rafael Nadal', 4),
            _test_data_generator('Pete Sampras', 5),
        ])

    return inner


loaders = [get_df_table_loader('great_players', get_data_builder(), pk_cols=['name'], import_mode='create')]

