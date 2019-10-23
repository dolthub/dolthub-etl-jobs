import requests as req
import pandas as pd
from typing import Callable, Mapping
from doltpy.etl import get_df_table_loader
import logging

logger = logging.getLogger(__name__)

BASE_URL = 'https://date.nager.at/Api/v2'
AVAILABLE_COUNTRIES = 'AvailableCountries'
PK_COLS = ['date', 'year', 'localName', 'country_code']


def _get_country_url(country: str, year: int) -> str:
    return '{}/PublicHolidays/{}/{}'.format(BASE_URL, year, country)


def _get_holidays_for_year(year: int, code_name_lookup: Mapping[str, str]) -> Callable[[], pd.DataFrame]:
    def inner() -> pd.DataFrame:
        return pd.concat([_get_holidays_country_year(code, name, year) for code, name in code_name_lookup.items()])

    return inner


def _get_holidays_country_year(code: str, name: str, year: int) -> pd.DataFrame:
    try:
        url = _get_country_url(code, year)
        logger.info('Getting data for {} from {}'.format(name, url))
        json = req.get(url).json()
    except:
        raise

    return pd.DataFrame(json).assign(country_code=code, country_name=name, year=year)


def _get_codename_lookup() -> Mapping[str, str]:
    codename_lookup_url = '{}/{}'.format(BASE_URL, AVAILABLE_COUNTRIES)
    logger.info('Fetching codenames from {}'.format(codename_lookup_url))
    countries = req.get(codename_lookup_url).json()
    return {country['key']: country['value'] for country in countries}


def get_loaders(start_year: int, end_year: int):
    [get_df_table_loader('public_holidays',
                         _get_holidays_for_year(year, _get_codename_lookup()),
                         PK_COLS) for year in range(start_year, end_year)]

