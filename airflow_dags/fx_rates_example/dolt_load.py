#!/usr/local/bin/python3

import pandas as pd
import requests
from doltpy.etl import get_df_table_writer, get_table_transformer, get_dolt_loader, load_to_dolthub
from doltpy.core import Dolt, read
from datetime import datetime
import logging
import argparse

logger = logging.getLogger(__name__)
API_KEY = 'f5fd4ab6514fa48753cb1083af2b1e33'
URL = 'http://data.fixer.io/api/latest?access_key={}'.format(API_KEY)


FX_RATES_REPO = 'oscarbatori/fx-test-data'


def get_raw_data() -> pd.DataFrame:
    r = requests.get(URL)
    logger.info('Fetching rates data from URL {}'.format(URL))
    if r.status_code == 200:
        data = r.json()
        exchange_rates = data['rates']
        return pd.DataFrame([{'currency': currency, 'rate': rate, 'timestamp': data['timestamp']}
                             for currency, rate in exchange_rates.items()])
    else:
        raise ValueError('Bad request, return code {}'.format(r.status_code))


def get_raw_fx_rates(repo: Dolt):
    return read.read_table(repo, 'eur_fx_rates')


def get_average_rates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info('Computing moving averages of currency rates')
    return df.groupby('currency').mean().reset_index()[['currency', 'rate']].rename(columns={'rate': 'average_rate'})


def load_raw_fx_rates():
    table_writer = get_df_table_writer('eur_fx_rates', get_raw_data, ['currency', 'timestamp'])
    message = 'Updated raw FX rates for date {}'.format(datetime.now())
    loader = get_dolt_loader(table_writer, commit=True, message=message)
    load_to_dolthub(loader, clone=True, push=True, remote_url=FX_RATES_REPO)


def load_fx_rates_running_averages():
    table_writer = get_table_transformer(get_raw_fx_rates, 'eur_fx_rate_averages', ['currency'], get_average_rates)
    loader = get_dolt_loader(table_writer, True, 'Updated averages for date {}'.format(datetime.now()))
    load_to_dolthub(loader, clone=True, push=True, remote_url=FX_RATES_REPO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--raw', action='store_true')
    parser.add_argument('--averages', action='store_true')
    args = parser.parse_args()

    if args.raw:
        load_raw_fx_rates()
    if args.averages:
        load_fx_rates_running_averages()
    else:
        raise argparse.ArgumentError('One of --raw or --averages must be passed')

