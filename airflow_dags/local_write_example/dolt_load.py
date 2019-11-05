import pandas as pd
from doltpy.etl import get_df_table_writer, get_dolt_loader
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


def get_loaders():
    writer = get_df_table_writer('great_players', get_data_builder(), pk_cols=['name'], import_mode='create')
    return [get_dolt_loader([writer], True, 'Added some great players!')]
