from datetime import datetime, timedelta
from doltpy.etl import dolthub_loader, DoltTableLoader
from mta.dolt_load import loaders as mta_loaders
from fx_rates_example.dolt_load import (raw_table_loaders as fx_rates_raw_loaders,
                                         transformed_table_loaders as fx_rates_transform_loaders)
from ip_to_country.dolt_load import ip_loaders as ip_to_country_loaders
from wikipedia_word_frequency.dolt_load import get_loaders as get_wikipedia_loaders
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


def get_args_helper(loaders: List[DoltTableLoader], message: str, remote_url: str, branch='master'):
    return dict(loaders=loaders,
                dolt_dir=None,
                clone=True,
                branch=branch,
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


# WordNet database
word_net_dag = DAG('word_net',
                   default_args=get_default_args_helper(datetime(2019, 10, 22)),
                   schedule_interval=timedelta(days=7))

raw_word_net = BashOperator(task_id='import-data',
                            bash_command='{{conf.get("core", "dags_folder")}}/word_net/import_from_source.pl ',
                            dag=word_net_dag)


# Code Search Net database
code_search_net_dag = DAG('code_search_net',
                         default_args=get_default_args_helper(datetime(2019, 10, 23)),
                         schedule_interval=timedelta(days=7))

code_search_net = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/code_search_net/import_from_source.pl ',
    dag=code_search_net_dag
)

# USDA All Foods database
usda_all_foods_dag = DAG(
    'usda_all_foods',
    default_args=get_default_args_helper(datetime(2019, 10, 24)),
    schedule_interval=timedelta(days=7)
)

raw_usda_all_foods = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/usda_all_foods/import_from_source.pl ',
    dag=usda_all_foods_dag
)

# Tatoeba sentence translations
tatoeba_sentence_translations_dag = DAG(
    'tatoeba_sentence_translations',
    default_args=get_default_args_helper(datetime(2019, 10, 21)),
    schedule_interval=timedelta(days=7)
)

raw_tatoeba_sentence_translations = BashOperator(
    task_id='import-data',
    bash_command='{{conf.get("core", "dags_folder")}}/tatoeba_sentence_translations/import-from-source.pl ',
    dag=tatoeba_sentence_translations_dag
)

# Facebook Neural Code Search Evaluation
neural_code_search_eval_dag = DAG('neural_code_search_eval',
                                  default_args=get_default_args_helper(datetime(2019, 10, 25)),
                                  schedule_interval=timedelta(days=7))

raw_neural_code_search_eval = BashOperator(task_id='import-data',
                                           bash_command='{{conf.get("core", "dags_folder")}}/neural_code_search_eval/import_from_source.pl ',
                                           dag=neural_code_search_eval_dag)

# Wikipedia word frequency
WIKIPEDIA_REPO = 'Liquidata/wikipedia-word-frequency'
DUMP_DATE = datetime.now() - timedelta(days=4)
# FORMATTED_DATE = DUMP_DATE.strftime("%-m-%d-%y")
FORMATTED_DATE = '10-8-19'
FILTERS = ['no_numbers', 'no_abbreviations', 'ASCII_only', 'strict', 'convert_to_ASCII', 'stemmed']
# XML dumps released on the 1st and 20th of every month. This job should run 4 days after.
CRON_EXPRESSION = '0 8 5,24 * *'


def define_wikipedia_dag():
    wikipedia_dag = DAG('wikipedia-word-frequency',
                        default_args=get_default_args_helper(datetime(2019, 10, 18)),
                        schedule_interval=CRON_EXPRESSION)

    wikipedia_no_filter = PythonOperator(task_id='no_filter',
                                         python_callable=dolthub_loader,
                                         op_kwargs=get_args_helper(get_wikipedia_loaders('no_filter'),
                                                                   'Update Wikipedia word frequencies for {} XML dump'.format(FORMATTED_DATE),
                                                                   WIKIPEDIA_REPO),
                                         dag=wikipedia_dag)

    # TODO: create branch with dump date and push to origin before applying filters

    for word_filter in FILTERS:
        branch_name = '{}/filter_{}'.format(FORMATTED_DATE, word_filter)
        wikipedia_with_filter = PythonOperator(task_id=word_filter,
                                               python_callable=dolthub_loader,
                                               op_kwargs=get_args_helper(get_wikipedia_loaders(branch_name),
                                                                         'Update Wikipedia word frequencies with {} filter for {} XML dump'.format(word_filter, FORMATTED_DATE),
                                                                         WIKIPEDIA_REPO,
                                                                         branch=branch_name),
                                               dag=wikipedia_dag)

        wikipedia_with_filter.set_upstream(wikipedia_no_filter)

define_wikipedia_dag()