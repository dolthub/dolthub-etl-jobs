import pandas as pd
from doltpy_etl import Dataset
import io
import requests
from typing import Mapping, Callable, List
import gzip


DATASET_COLUMNS = {'IPv4ToCountry': ['IPFrom',
                                     'IpTo',
                                     'Registry',
                                     'AssignedDate',
                                     'CountryCode2Letter',
                                     'CountryCode3Letter',
                                     'Country'],
                   'IPv6ToCountry': ['IPRange',
                                     'CountryCode2Letter',
                                     'Registry',
                                     'AssignedDate']}
FILENAME = 'IpToCountry.csv'


class IpToCountryDataset:

    def __init__(self,
                 name: str,
                 url: str,
                 pk_cols: List[str],
                 int_cols: List[str]):
        self.name = name
        self.columns = DATASET_COLUMNS[self.name]
        self.url = url
        self.pk_cols = pk_cols
        self.int_cols = int_cols


ip_to_country_datasets = [
    IpToCountryDataset('IPv4ToCountry',
                       'http://software77.net/geo-ip/?DL=1',
                       ['IPFrom', 'IpTo'],
                       ['IPFrom', 'IpTo', 'AssignedDate']),
    IpToCountryDataset('IPv6ToCountry',
                       'http://software77.net/geo-ip/?DL=7',
                       ['IPRange'],
                       ['AssignedDate'])
]


def fetch_data(ip_to_country_dataset: IpToCountryDataset) -> io.BytesIO:

    print('Fetching zipfile from URL {} for dataset {}'.format(ip_to_country_dataset.url,
                                                               ip_to_country_dataset.name))
    req = requests.get(ip_to_country_dataset.url)
    if req.status_code == 200:
        return io.BytesIO(req.content)
    else:
        raise ValueError('Request to URL {} failed with status code'.format(ip_to_country_dataset.url,
                                                                            req.status_code))


def process_gzip(raw: io.BytesIO,
                 line_filter: Callable[[str], bool],
                 line_processor: Callable[[str], Mapping[str, str]]) -> Mapping[str, str]:
    print('Mapping bytes to dictionaries representing lines in a data frame')
    with gzip.open(raw) as f:
        lines = f.readlines()
        for line in lines:
            try:
                decoded = line.decode('utf-8')
                if not line_filter(decoded):
                    yield line_processor(decoded)
            except UnicodeDecodeError as _:
                print('Error decoding line, skipping:\n{}'.format(line))


def get_line_processor(columns: List[str]) -> Callable[[str], Mapping[str, str]]:
    def inner(line: str) -> Mapping[str, str]:
        els = [el.replace('"', '') for el in line.rstrip().split(',')]
        assert len(els) == len(columns), 'Corrupt line: {}'.format(els)
        return {col: val for col, val in zip(columns, els)}

    return inner


def discard_line(line: str) -> bool:
    return line.startswith('#')


def get_df_builder(ip_to_country_dataset: IpToCountryDataset) -> Callable[[], pd.DataFrame]:
    def inner() -> pd.DataFrame:
        zip_file = fetch_data(ip_to_country_dataset)
        df = pd.DataFrame(process_gzip(zip_file,
                                       discard_line,
                                       get_line_processor(ip_to_country_dataset.columns)))
        return df.astype({col: 'int' for col in ip_to_country_dataset.int_cols})

    return inner


def get_dolt_datasets():
    def build_dataset(ip_to_country_dataset: IpToCountryDataset) -> Dataset:
        return Dataset(ip_to_country_dataset.name,
                       get_df_builder(ip_to_country_dataset),
                       ip_to_country_dataset.pk_cols)

    return [build_dataset(ip_to_country_dataset) for ip_to_country_dataset in ip_to_country_datasets]
