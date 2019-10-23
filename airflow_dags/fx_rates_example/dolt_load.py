import pandas as pd
import requests
from doltpy.etl import get_df_table_loader, get_table_transfomer
from doltpy.core import Dolt
import logging

logger = logging.getLogger(__name__)
API_KEY = 'f5fd4ab6514fa48753cb1083af2b1e33'
URL = 'http://data.fixer.io/api/latest?access_key={}'.format(API_KEY)


def get_data() -> pd.DataFrame:
    r = requests.get(URL)
    logger.info('Fetching rates data from URL {}'.format(URL))
    if r.status_code == 200:
        data = r.json()
        exchange_rates = data['rates']
        return pd.DataFrame([{'currency': currency, 'rate': rate, 'timestamp': data['timestamp']}
                             for currency, rate in exchange_rates.items()])
    else:
        raise ValueError('Bad request, return code {}'.format(r.status_code))


def get_average(repo: Dolt):
    return repo.read_table('eur_fx_rates').to_pandas()


def get_average_rates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info('Computing moving averages of currency rates')
    return df.groupby('currency').mean().reset_index()[['currency', 'rate']].rename(columns={'rate': 'average_rate'})


raw_table_loaders = [get_df_table_loader('eur_fx_rates', get_data, ['currency', 'timestamp'])]
transformed_table_loaders = [get_table_transfomer(get_average, 'eur_fx_rate_averages', ['currency'], get_average_rates)]