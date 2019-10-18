from datetime import datetime, timedelta
from doltpy_etl import dolthub_loader, DoltTableLoader
from mta.dolt_load import loaders as mta_loaders
from fx_rates_example.dolt_load import (raw_table_loaders as fx_rates_raw_loaders,
                                         transformed_table_loaders as fx_rates_transform_loaders)
from ip_to_country.dolt_load import ip_loaders as ip_to_country_loaders
from typing import List
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.configuration import conf

def get_default_args_helper(start_date: datetime):
    return {'owner': 'liquidata-etl',
            'depends_on_past': False,
            'start_date': start_date,
            'email': ['airflow@liquidata.co'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retry_delay': timedelta(minutes=5)}


def get_args_helper(loaders: List[DoltTableLoader], message: str, remote_url: str):
    return dict(loaders=loaders,
                dolt_dir=None,
                clone=True,
                branch='master',
                commit=True,
                push=True,
                remote_name='origin',
                message=message,
                dry_run=False,
                remote_url=remote_url)


# FX rates DAG
FX_RATES_REPO_PATH = 'oscarbatori/fx-test-data'
fx_rates_dag = DAG('fx_rates',
                   default_args=get_default_args_helper(datetime(2019, 10, 9)),
                   schedule_interval=timedelta(hours=1))


fx_rates_raw_data = PythonOperator(task_id='fx_rates_raw',
                                   python_callable=dolthub_loader,
                                   op_kwargs=get_args_helper(fx_rates_raw_loaders,
                                                             'Update raw data {}'.format(datetime.now()),
                                                             FX_RATES_REPO_PATH),
                                   dag=fx_rates_dag)

fx_rates_averages = PythonOperator(task_id='fx_rates_averages',
                                   python_callable=dolthub_loader,
                                   op_kwargs=get_args_helper(fx_rates_transform_loaders,
                                                             'Update averages {}'.format(datetime.now()),
                                                             FX_RATES_REPO_PATH),
                                   dag=fx_rates_dag)

fx_rates_averages.set_upstream(fx_rates_raw_data)


# MTA data DAG
MTA_REPO_PATH = 'oscarbatori/mta-data'
mta_dag = DAG('mta_data',
              default_args=get_default_args_helper(datetime(2019, 10, 8)),
              schedule_interval=timedelta(days=1))

raw_mta_data = PythonOperator(task_id='raw_mta_data',
                              python_callable=dolthub_loader,
                              op_kwargs=get_args_helper(mta_loaders,
                                                        'Update MTA data for date {}'.format(datetime.now()),
                                                        MTA_REPO_PATH),
                              dag=mta_dag)

# IP to country mappings
IP_TO_COUNTRY_REPO = 'Liquidata/ip-to-country'
ip_to_country_dag = DAG('ip_to_country',
                        default_args=get_default_args_helper(datetime(2019, 10, 8)),
                        schedule_interval=timedelta(days=1))

raw_ip_to_country = PythonOperator(task_id='ip_to_country',
                                   python_callable=dolthub_loader,
                                   op_kwargs=get_args_helper(ip_to_country_loaders,
                                                             'Update IP to Country for date {}'.format(datetime.now()),
                                                             IP_TO_COUNTRY_REPO),
                                   dag=ip_to_country_dag)

# Code Search Net database
code_search_net_dag = DAG('code_search_net',
                         default_args=get_default_args_helper(datetime(2019, 10, 21)),
                         schedule_interval=timedelta(days=7))

raw_usda_all_foods = BashOperator(task_id='import-data',
                                  bash_command='{{conf.get("core", "dags_folder")}}/code_search_net/import_from_source.pl ',
                                  dag=code_search_net_dag)
