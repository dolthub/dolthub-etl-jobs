from doltpy.dolt import Dolt
import requests as req
import pandas as pd
from typing import Callable, Mapping
from doltpy_etl import Dataset, ETLWorkload

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
        print('Getting data for {} from {}'.format(name, url))
        json = req.get(url).json()
    except:
        raise

    return pd.DataFrame(json).assign(country_code=code, country_name=name, year=year)


def _get_codename_lookup() -> Mapping[str, str]:
    countries = req.get('{}/{}'.format(BASE_URL, AVAILABLE_COUNTRIES)).json()
    return {country['key']: country['value'] for country in countries}


def load_to_dolt(dolt_dir: str, start_year: int, end_year: int, commit: bool = False):
    repo = Dolt(dolt_dir)
    assert repo.repo_is_clean()

    dolt_datasets = [Dataset('public_holidays', _get_holidays_for_year(year, _get_codename_lookup()), PK_COLS)
                     for year in range(start_year, end_year)]
    workload = ETLWorkload(repo, dolt_datasets)
    workload.load_to_dolt(commit)


