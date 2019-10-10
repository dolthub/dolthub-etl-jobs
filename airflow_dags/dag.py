from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from doltpy_etl import dolthub_loader

GLOBAL_MODULE_PREFIX = 'liquidata_etl'


def get_default_args_helper(start_date: datetime):
    return {'owner': 'liquidata-etl',
            'depends_on_past': False,
            'start_date': start_date,
            'email': ['airflow@liquidata.co'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retry_delay': timedelta(minutes=5)}


def get_args_helper(dolt_load_module: str, message: str, remote_url: str):
    return dict(dolt_load_module=dolt_load_module,
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
FX_RATES_MODULE_PATH = '{}.fx_rates_example.dolt_load'.format(GLOBAL_MODULE_PREFIX)
fx_rates_dag = DAG('fx_rates',
                   default_args=get_default_args_helper(datetime(2019, 10, 9)),
                   schedule_interval=timedelta(hours=1))


fx_rates_raw_data = PythonOperator(task_id='fx_rates_raw',
                                   python_callable=dolthub_loader,
                                   op_kwargs=get_args_helper('{}.raw_table_loaders'.format(FX_RATES_MODULE_PATH),
                                                             'Update raw data {}'.format(datetime.now()),
                                                             FX_RATES_REPO_PATH),
                                   dag=fx_rates_dag)

fx_rates_averages = PythonOperator(task_id='fx_rates_averages',
                                   python_callable=dolthub_loader,
                                   op_kwargs=get_args_helper('{}.transformed_table_loaders'.format(FX_RATES_MODULE_PATH),
                                                             'Update averages {}'.format(datetime.now()),
                                                             FX_RATES_REPO_PATH),
                                   dag=fx_rates_dag)

fx_rates_averages.set_upstream(fx_rates_raw_data)


# MTA data DAG
MTA_REPO_PATH = 'oscarbatori/mta-data'
MTA_MODULE_PATH = '{}.mta.dolt_load'.format(GLOBAL_MODULE_PREFIX)
mta_dag = DAG('mta_data',
              default_args=get_default_args_helper(datetime(2019, 10, 8)),
              schedule_interval=timedelta(days=1))

raw_mta_data = PythonOperator(task_id='raw_mta_data',
                              python_callable=dolthub_loader,
                              op_kwargs=get_args_helper('{}.loaders'.format(MTA_MODULE_PATH),
                                                        'Update MTA data for date {}'.format(datetime.now()),
                                                        MTA_REPO_PATH),
                              dag=mta_dag)

# IP to country mappings
IP_TO_COUNTRY_REPO = 'Liquidata/ip-to-country'
IP_TO_COUNTRY_MODULE_PATH = '{}.ip_to_country.dolt_load'.format(GLOBAL_MODULE_PREFIX)
ip_to_country_dag = DAG('ip_to_country',
                        default_args=get_default_args_helper(datetime(2019, 10, 8)),
                        schedule_interval=timedelta(days=1))

raw_ip_to_country = PythonOperator(task_id='ip_to_country',
                                   python_callable=dolthub_loader,
                                   op_kwargs=get_args_helper('{}.ip_loaders'.format(IP_TO_COUNTRY_REPO),
                                                             'Update IP to Country for date {}'.format(datetime.now()),
                                                             IP_TO_COUNTRY_REPO),
                                   dag=ip_to_country_dag)
