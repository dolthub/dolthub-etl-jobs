import requests as req
import pandas as pd
from typing import List
from doltpy import Dolt
from doltpy_etl import Dataset, ETLWorkload, insert_unique_key, Transformer

OPEN_DATA_NYC_BASE_URL = 'https://data.ny.gov/resource'
MAXRECS = 10000000


class MTADataSet:

    def __init__(self, table_name: str, dataset_id: str, pk_cols: List[str]=None):
        self.table_name = table_name
        self.dataset_id = dataset_id
        self.pk_cols = pk_cols


DATASETS = [
    MTADataSet('wifi_locations', 'pwa9-tmie', ['station_name', 'lines']),
    MTADataSet('contract_solicitations', 'e3e7-qwer', ['reference_number']),
    MTADataSet('customer_feedback', 'tppa-s6t6'),
    MTADataSet('mta_agency_kpis', 'cy9b-i9w9', ['indicator_sequence', 'period']),
    MTADataSet('hourly_traffic_bridges', 'qzve-kjga', ['plaza_id', 'date', 'hour', 'direction']),
    MTADataSet('capital_dashboard_agencies', 'kizb-nxtu', ['project_number', 'capital_plan', 'plan_revision']),
    MTADataSet('fare_card_history', 'v7qc-gwpn', ['from_date', 'to_date', 'station', 'remote_station_id'])
]


def get_mta_data_as_df(url: str) -> Transformer:
    def inner():
        with req.get(url) as request:
            if request.status_code == 200:
                return pd.DataFrame(request.json())

    return inner


def get_mta_url(dataset_id: str) -> str:
    return '{}/{}.json?$limit={}'.format(OPEN_DATA_NYC_BASE_URL, dataset_id, MAXRECS)


def load_to_dolt(dolt_dir: str, commit_data: bool = False):
    repo = Dolt(dolt_dir)
    assert repo.repo_is_clean(), 'Must be operating on a clean repo'

    def build_dataset(dataset: MTADataSet):
        tramsformers = [] if dataset.pk_cols else [insert_unique_key]
        pk_cols = ['hash_id'] if not dataset.pk_cols else dataset.pk_cols

        return Dataset(dataset.table_name,
                       get_mta_data_as_df(get_mta_url(dataset.dataset_id)),
                       pk_cols,
                       tramsformers)

    dolt_datasets = [build_dataset(dataset) for dataset in DATASETS]
    workload = ETLWorkload(repo, dolt_datasets)
    workload.load_to_dolt(commit_data)
