
from doltpy.etl import get_df_table_writer, get_dolt_loader
import requests
from airflow_dags.coin_metrics.coinlist_config import COINLIST, get_data_url
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger(__name__)


def get_data():
    return pd.concat([_get_data_helper(ticker) for ticker in COINLIST])


def _get_data_helper(ticker: str):
    # logger.info('Fetching data for ticker {}'.format(ticker))
    print('Fetching data for ticker {}'.format(ticker))
    url = get_data_url(ticker)
    data = pd.read_csv(BytesIO(requests.get(url).content)).assign(ticker=ticker)
    return data


def get_dolt_datasets():
    table_writers = []