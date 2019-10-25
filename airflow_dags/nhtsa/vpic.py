"""Scrape NHTSA's Product Information Catalog and Vehicle Listing (vPIC) API, store results in Dolt"""

import requests
import pandas as pd
import logging
from doltpy.etl import get_df_table_loader
import uuid


logger = logging.getLogger(__name__)
ALL_MAKES_URL = 'https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json'
MODELS_FOR_MAKE_URL = 'https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMake/{make_name}?format=json'


def _get_models_for_make_url(make: str):
    return MODELS_FOR_MAKE_URL.format(make_name=make)


def _get_make_index():
    logger.info('Getting index of all makes from {}'.format(ALL_MAKES_URL))
    try:
        json = requests.get(ALL_MAKES_URL).json()
        return list(pd.DataFrame(json['Results'])['Make_Name'])
    except Exception as e:
        logger.critical('Failed to get brand index exiting, exception was {}'.format(e))
        raise e


def _get_data_for_make(make_name: str):
    url = _get_models_for_make_url(make_name)
    logger.info('Fetching data for make {} from URL {}'.format(make_name, url))
    try:
        json = requests.get(url).json()
        data = pd.DataFrame(json['Results'])
        logger.info('Found {} models for make {}'.format(len(data), make_name))
        return data
    except Exception as e:
        logger.error('Failed to get data for make {} with exception {}'.format(make_name, e))
        return pd.DataFrame()


def get_raw_data():
    make_index = _get_make_index()
    all_makes_data = pd.concat([_get_data_for_make(make_name) for make_name in make_index])
    return all_makes_data.rename(columns={col: col.lower() for col in all_makes_data.columns})


def generate_branch_name():
    return str(uuid.uuid1())


loaders = [get_df_table_loader('makes', get_raw_data, ['model_id'])]
