# How To Create a Job

## Install Airflow

You are going to want to test your jobs locally, so install Airflow. Airflow requires python3 and pip3. I used homebrew to 
get python3 and pip3. Then I aliased my python to python3 and my pip to pip3. Then I used `pip install apache-airflow` to 
get airflow locally. That made an airflow directory in my home directory and put the airflow command line utility in my 
path.

## Point Airflow at this package

To run DAGs configured in this package, your local airflow needs to know about this code. There is a configuration file 
called `~/airflow/airflow.cfg`. Change the first value, `dags_folder`, in that file to point to where you have this Git 
repository checked out like so:

    [core]
    # The folder where your airflow pipelines live, most likely a
    # subfolder in a code repository
    # This path must be absolute
    dags_folder = /Users/timsehn/liquidata/git/liquidata-etl-jobs/airflow_dags

Now, run `airflow initdb` to have airflow load your configuration. If everything worked, you should be able to run 
`airflow list_dags` and see the DAGs in this package like `ip_to_country` or `word_net`.

## Write the import job

The general flow of most of these jobs is:

1. Clone the dolt repo you want to operate on
1. Download the data you want to import
1. Check and see if the data has changed since last job run. If no change, exit.
1. Import the data into the dolt repo. If the data downloaded from the web is assumed to be truth, delete the current 
data before importing or use `dolt table import -r`.
1. Run `dolt diff`. Exit if nothing has changed.
1. `dolt add .` to stage the changes
1. `dolt commit` with a descriptive automated commit message
1. `dolt push origin master`

For some imports, you may want to make a branch and not make automated changes on master. In that case, after you clone the 
repository, make a branch with `dolt checkout -b <branch name>` and push to that branch at the end instead of master,
`dolt push origin <branch name>`.

By convention, we put the source code to these import jobs in a directory named after the dolt repository you are importing
to (with '-'s changed to '_'s).

Airflow creates a new temporary directory every time your job runs. It deletes that temporary directory afterwards. The 
script airflow is executing is using that temporary directory as its root. Thus, you can just `dolt clone` the repository 
in every job run and not worry about cleaning it up. Same goes with any files (ie. data) you download to process the 
import job. 

There are examples included here that use Python (with DoltPy as a helper) and Perl to perform the import.

## Create the Airflow DAG

In `dag.py`, create an entry for your new import job. In there simplest form you define a dag and a task in that dag.

*Bash example*

Use the `bash_command` to run any arbitrary import. There's a bunch of Perl script examples in this package but you 
can execute any bash command this way.

    word_net_dag = DAG('word_net',
                       default_args=get_default_args_helper(datetime(2019, 10, 21)),
                       schedule_interval=timedelta(days=7))

    raw_word_net = BashOperator(task_id='import-data',
                                bash_command='{{conf.get("core", "dags_folder")}}/word_net/import_from_source.pl ',
                                dag=word_net_dag)

Note the space after the `bash_command` string. That's important. Without it, airflow will try to interpret that string as a
Jinja template and you'll get really crytic errors. 

*Python Example*

We have a Python library called DoltPy that helps with Dolt imports. It exposes an interface named `dolthub_loader`.

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

## Test your job

Make sure your running airflow can see your new DAG by running `airflow list_dags`. Assuming it's there, you can test it
locally using `airflow test <dag> <task> <date>` (ie. `airflow test word_net import-data 2019-10-22`). This runs the job as
airflow would in production. Fix any errors that occur.

## Create a pull request to this repository

Once you have your job working, create a pull request to this directory. Someone here at Liquidata will review it and 
suggest any changes. Once the PR is reviewed, the code will be deployed and you'll be able to see your DAG and the status 
of its runs here: https://airflow.awsdev.ld-corp.com/admin/.

## Running in production

The production deployment of airflow can be viewed here: https://airflow.awsdev.ld-corp.com/admin/. You can view the 
status of your jobs or kick off runs manually there. 

You need to give write permission to `LiquidataSystemAccount` user on DoltHub to allow these jobs to update your 
Dolt repositories on DoltHub.

The instances that execute these jobs in production are not full Linux distributions so any utilities you make use of in 
your import jobs will have to be installed. For instance, we had to install `curl` to download files. 
