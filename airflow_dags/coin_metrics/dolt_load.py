import requests
from coin_metrics.coinlist_config import COINLIST, get_data_url
import pandas as pd
from io import BytesIO
import logging
from doltpy.etl import get_df_table_writer, get_dolt_loader
from datetime import datetime
logger = logging.getLogger(__name__)


def get_data():
    return pd.concat([_get_data_helper(ticker) for ticker in COINLIST])


def _get_data_helper(ticker: str):
    # logger.info('Fetching data for ticker {}'.format(ticker))
    print('Fetching data for ticker {}'.format(ticker))
    url = get_data_url(ticker)
    data = pd.read_csv(BytesIO(requests.get(url).content)).assign(ticker=ticker)
    return data


def get_loaders():
    table_writers = [get_df_table_writer('eod_data', get_data, ['date', 'ticker'], 'update')]
    return [get_dolt_loader(table_writers, True, 'Updated at {}'.format(datetime.now()))]


