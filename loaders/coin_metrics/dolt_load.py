import requests
from coin_metrics.coinlist_config import COINLIST, get_data_url
import pandas as pd
from io import BytesIO
import logging
from doltpy.etl import get_df_table_writer, get_dolt_loader, load_to_dolthub
import argparse
from helpers.github_actions import get_commit_message

logger = logging.getLogger(__name__)

REMOTE_DB = "dolthub/coin-metrics-daily-data"

def get_data():
    return pd.concat([_get_data_helper(ticker) for ticker in COINLIST])


def _get_data_helper(ticker: str):
    # logger.info('Fetching data for ticker {}'.format(ticker))
    print('Fetching data for ticker {}'.format(ticker))
    url = get_data_url(ticker)
    data = pd.read_csv(BytesIO(requests.get(url).content)).assign(ticker=ticker)
    return data


def load(git_hash: str, github_actions_run_url: str):
    table_writers = [get_df_table_writer('eod_data', get_data, ['date', 'ticker'], 'update')]
    loaders = [get_dolt_loader(table_writers, True, get_commit_message(git_hash, github_actions_run_url))]
    load_to_dolthub(loaders, clone=True, push=True, remote_name='origin', remote_url=REMOTE_DB)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--git-hash', type=str)
    parser.add_argument('--github-actions-run-url', type=str)
    args = parser.parse_args()
    load(args.git_hash, args.github_actions_run_url)
