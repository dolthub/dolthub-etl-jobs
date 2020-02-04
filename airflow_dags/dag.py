from datetime import datetime, timedelta
from doltpy.etl import dolthub_loader, DoltLoaderBuilder
from mta.dolt_load import get_loaders as get_mta_loaders
from fx_rates_example.dolt_load import (get_raw_table_loaders as get_fx_rates_raw_loaders,
                                        get_transformed_table_loaders as get_fx_rates_transform_loaders)
from ip_to_country.dolt_load import get_dolt_datasets as get_ip_loaders
from wikipedia.word_frequency.dolt_load import get_wikipedia_loaders
from wikipedia.ngrams.dolt_load import get_dolt_datasets as get_ngram_loaders
from five_thirty_eight.polls import get_loaders as get_five_thirty_eight_polls_loaders
from five_thirty_eight.soccer_spi import get_loaders as get_five_thirty_eight_soccer_spi_loaders
from five_thirty_eight.nba_forecasts import get_loaders as get_five_thirty_eight_nba_forecasts_loaders
from five_thirty_eight.nfl_forecasts import get_loaders as get_five_thirty_eight_nfl_forecasts_loaders
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from functools import partial
from typing import Tuple


def get_default_args_helper(start_date: datetime):
    return {'owner': 'liquidata-etl',
            'depends_on_past': False,
            'start_date': start_date,
            'email': ['airflow@liquidata.co'],
            'email_on_failure': False,
            'email_on_retry': False,
            'catchup': False,
            'retry_delay': timedelta(minutes=5)}


def get_args_helper(loader_builder: DoltLoaderBuilder, remote_url: str):
    return dict(loader_builder=loader_builder,
                dolt_dir=None,
                clone=True,
                push=True,
                remote_name='origin',
                dry_run=False,
                remote_url=remote_url)


# FX rates DAG
FX_RATES_REPO_PATH = 'oscarbatori/fx-test-data'
fx_rates_dag = DAG('fx_rates',
                   default_args=get_default_args_helper(datetime(2019, 10, 9)),
                   schedule_interval=timedelta(hours=1))


fx_rates_raw_data = PythonOperator(task_id='fx_rates_raw',
                                   python_callable=dolthub_loader,
                                   op_kwargs=get_args_helper(get_fx_rates_raw_loaders, FX_RATES_REPO_PATH),
                                   dag=fx_rates_dag)

fx_rates_averages = PythonOperator(task_id='fx_rates_averages',
                                   python_callable=dolthub_loader,
                                   op_kwargs=get_args_helper(get_fx_rates_transform_loaders, FX_RATES_REPO_PATH),
                                   dag=fx_rates_dag)

fx_rates_averages.set_upstream(fx_rates_raw_data)


# MTA data DAG
MTA_REPO_PATH = 'oscarbatori/mta-data'
mta_dag = DAG('mta_data',
              default_args=get_default_args_helper(datetime(2019, 10, 8)),
              schedule_interval=timedelta(days=1))

raw_mta_data = PythonOperator(task_id='raw_mta_data',
                              python_callable=dolthub_loader,
                              op_kwargs=get_args_helper(get_mta_loaders, MTA_REPO_PATH),
                              dag=mta_dag)

# IP to country mappings
IP_TO_COUNTRY_REPO = 'Liquidata/ip-to-country'
ip_to_country_dag = DAG('ip_to_country',
                        default_args=get_default_args_helper(datetime(2019, 10, 8)),
                        schedule_interval=timedelta(days=1))

raw_ip_to_country = PythonOperator(task_id='ip_to_country',
                                   python_callable=dolthub_loader,
                                   op_kwargs=get_args_helper(get_ip_loaders, IP_TO_COUNTRY_REPO),
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

# PPDB - This just checks to make sure PPDB has not changed
# I accidentally deleted the import code. If PPDB changes, I will rewrite :-(
ppdb_dag = DAG('ppdb',
               default_args=get_default_args_helper(datetime(2019, 12, 11)),
               schedule_interval=timedelta(days=7))

raw_ppdb = BashOperator(task_id='is-changed',
                        bash_command='{{conf.get("core", "dags_folder")}}/ppdb/is-changed.pl ',
                        dag=ppdb_dag)

# Stanford Questions and Answers Database (SQuAD)
squad_dag = DAG('squad',
                default_args=get_default_args_helper(datetime(2019, 12, 13)),
                schedule_interval=timedelta(days=7))

raw_squad = BashOperator(task_id='import-data',
                         bash_command='{{conf.get("core", "dags_folder")}}/squad/import-data.pl ',
                         dag=squad_dag)

# Stanford Natural Language Inference (SNLI)
snli_dag = DAG('snli',
               default_args=get_default_args_helper(datetime(2020, 1, 3)),
               schedule_interval=timedelta(days=7))

raw_snli = BashOperator(task_id='import-data',
                        bash_command='{{conf.get("core", "dags_folder")}}/snli/import-data.pl ',
                        dag=snli_dag)

# Multi Natural Language Inference (Multi NLI)
multinli_dag = DAG('multinli',
                   default_args=get_default_args_helper(datetime(2020, 1, 3)),
                   schedule_interval=timedelta(days=7))

raw_multinli = BashOperator(task_id='import-data',
                            bash_command='{{conf.get("core", "dags_folder")}}/multi-nli/import-data.pl ',
                            dag=multinli_dag)

# Google Landmarks
google_landmarks_dag = DAG('google_landmarks',
                           default_args=get_default_args_helper(datetime(2019, 12, 18)),
                           schedule_interval=timedelta(days=7))

raw_google_lanadmarks = BashOperator(task_id='import-data',
                                     bash_command='{{conf.get("core", "dags_folder")}}/google-landmarks/import-data.pl ',
                                     dag=google_landmarks_dag)

# Baseball Databank
baseball_databank_dag = DAG('baseball_databank',
                            default_args=get_default_args_helper(datetime(2020,1, 6)),
                            schedule_interval=timedelta(days=7))

raw_baseball_databank = BashOperator(task_id='import-data',
                                     bash_command='{{conf.get("core", "dags_folder")}}/baseball-databank/import-data.pl ',
                                     dag=baseball_databank_dag)

# US Baby Names
us_baby_names_dag = DAG('us_baby_names',
                        default_args=get_default_args_helper(datetime(2020,1,15)),
                        schedule_interval=timedelta(days=7))

raw_us_baby_names = BashOperator(task_id='import-data',
                                 bash_command='{{conf.get("core", "dags_folder")}}/us_baby_names/import-data.pl ',
                                 dag=us_baby_names_dag)

# Wikipedia word frequency
WIKIPEDIA_REPO = 'Liquidata/wikipedia-word-frequency'
# Wikipedia dump variables
DUMP_DATE = datetime.now() - timedelta(days=4)
FORMATTED_DATE = DUMP_DATE.strftime("%Y%m%d")
# XML dumps released on the 1st and 20th of every month. These jobs should run 4 days after.
CRON_FORMAT = '0 8 5,24 * *'

# Wikipedia word frequency
WIKIPEDIA_WORDS_REPO = 'Liquidata/wikipedia-word-frequency'
wikipedia_dag = DAG(
    'wikipedia-word-frequency',
    default_args=get_default_args_helper(datetime(2019, 10, 18)),
    schedule_interval=CRON_FORMAT)

wikipedia_word_frequencies = PythonOperator(task_id='import-data',
                                            python_callable=dolthub_loader,
                                            op_kwargs=get_args_helper(partial(get_wikipedia_loaders, FORMATTED_DATE),
                                                                      WIKIPEDIA_WORDS_REPO),
                                            dag=wikipedia_dag)

# Wikipedia ngrams
WIKIPEDIA_NGRAMS_REPO = 'Liquidata/wikipedia-ngrams'
DUMP_TARGET = 'latest'
wikipedia_ngrams_dag = DAG(
    'wikipedia-ngrams',
    default_args=get_default_args_helper(datetime(2019, 11, 5)),
    schedule_interval=CRON_FORMAT
)

wikipedia_ngrams = PythonOperator(task_id='import-data',
                                  python_callable=dolthub_loader,
                                  op_kwargs=get_args_helper(partial(get_ngram_loaders, FORMATTED_DATE, DUMP_TARGET),
                                                            WIKIPEDIA_NGRAMS_REPO),
                                  dag=wikipedia_ngrams_dag)

# Backfill Wikipedia ngrams
wikipedia_ngrams_backfill_dag = DAG(
    'wikipedia-ngrams-backfill',
    default_args=get_default_args_helper(datetime(2019, 12, 2)),
    schedule_interval='@once',
)

dump_dates = ['20190901', '20190920', '20191001', '20191020', '20191101', '20191120', '20191201', '20191220']
tasks_list = []
for i, dump_date in enumerate(dump_dates):
    tasks_list.append(PythonOperator(task_id='backfill-data-{}'.format(dump_date),
                                               python_callable=dolthub_loader,
                                               op_kwargs=get_args_helper(partial(get_ngram_loaders, dump_date, dump_date),
                                                                         WIKIPEDIA_NGRAMS_REPO),
                                               dag=wikipedia_ngrams_backfill_dag))
    if i != 0:
        tasks_list[i-1] >> tasks_list[i]



# FiveThirtyEight data
def get_five_thirty_eight_loader(repo_name: str,
                                 task_id: str,
                                 interval: timedelta,
                                 start_date: datetime,
                                 loaders: DoltLoaderBuilder) -> Tuple[DAG, PythonOperator]:
    path = 'liquidata-demo-data/{}'.format(repo_name)
    task_id = 'five_thirty_eight_{}'.format(task_id)
    dag = DAG(task_id, default_args=get_default_args_helper(start_date), schedule_interval=interval)

    operator = PythonOperator(task_id=task_id,
                              python_callable=dolthub_loader,
                              op_kwargs=get_args_helper(loaders, path),
                              dag=dag)

    return dag, operator


# Polls
five_thirty_eight_polls_dag, five_thirty_eight_polls = get_five_thirty_eight_loader('polls',
                                                                                    'polls',
                                                                                    timedelta(hours=1),
                                                                                    datetime(2019, 12, 3),
                                                                                    get_five_thirty_eight_polls_loaders)

# Soccer
five_thirty_eight_soccer_spi_dag, five_thirty_eight_soccer_spi = get_five_thirty_eight_loader(
    'soccer-spi',
    'soccer_spi',
    timedelta(hours=1),
    datetime(2019, 12, 3),
    get_five_thirty_eight_soccer_spi_loaders
)

# NBA
five_thirty_eight_nba_forecasts_dag, five_thirty_eight_nba_forecasts = get_five_thirty_eight_loader(
    'nba-forecasts',
    'nba_forecasts',
    timedelta(hours=1),
    datetime(2019, 12, 3),
    get_five_thirty_eight_nba_forecasts_loaders
)

# NFL
five_thirty_eight_nfl_forecasts_dag, five_thirty_eight_nfl_forecasts = get_five_thirty_eight_loader(
    'nfl-forecasts',
    'nfl_forecasts',
    timedelta(hours=1),
    datetime(2019, 12, 3),
    get_five_thirty_eight_nfl_forecasts_loaders
)

# Example Go Program
go_example_dag = DAG('go_example',
                     default_args=get_default_args_helper(datetime(2020, 2, 2)),
                     schedule_interval=timedelta(days=1))

go_example = BashOperator(
    task_id='hello-world',
    bash_command='{{conf.get("core", "dags_folder")}}/go_example/run.sh ',
    dag=go_example_dag
)
