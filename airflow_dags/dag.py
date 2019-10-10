from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from doltpy_etl import dolthub_loader

REPO_URL = 'https://doltremoteapi.dolthub.com/oscarbatori/fx-test-data'
MODULE_PATH = 'liquidata_etl.fx_rates_example.dolt_load'

default_args = {
    'owner': 'liquidata-etl',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 6),
    'email': ['airflow@liquidata.co'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5)
}

# FX rates DAG
dag = DAG('fx_rates',
          default_args=default_args,
          schedule_interval=timedelta(hours=1))


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


# TODO refactor the arg generation, as only a few things seem to change
raw_data = PythonOperator(task_id='fx_rates_raw',
                          python_callable=dolthub_loader,
                          op_kwargs=get_args_helper('{}.raw_table_loaders'.format(MODULE_PATH),
                                                    'Update raw data {}'.format(datetime.now()),
                                                    REPO_URL),
                          dag=dag)

transformed_data = PythonOperator(task_id='fx_rates_averages',
                                  python_callable=dolthub_loader,
                                  op_kwargs=get_args_helper('{}.transformed_table_loaders'.format(MODULE_PATH),
                                                            'Update averages {}'.format(datetime.now()),
                                                            REPO_URL),
                                  dag=dag)

transformed_data.set_upstream(raw_data)

#