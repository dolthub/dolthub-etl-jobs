## Background
[DoltHub](https://www.dolthub.com) hosts [Dolt](https://github.com/liquidata-inc/dolt) databases. If you don't know what Dolt is, then you will need to start with by familiarizing yourself with it before proceeding with this repo.

Liquidata is the company that created Dolt, and DoltHub. We host public data for free, and this repository is how much of the public data under the Liquidata organization is created and maintained. At a high level each module maintains a public repository. Each repository roughly corresponds to a data source.

## How does this work?
Dolt is a relational database, and thus one way to create Dolt tables is to create `pandas.DataFrame` objects, and then use `doltpy` to load them into a Dolt database. We run an [Airflow](https://airflow.apache.org/) instance that executes jobs that produce interesting public datasets, and then load them to Dolt.

You can help us make interesting data public and clean by adding a dataset to this repository, or adding jobs that transform and clean existing data on DoltHub. 

## Development
To start developing ETL jobs for Liquidata's public datasets you need to clone the code, and then install `doltpy` from `PyPi`:
```bash
$ mkdir doltdev && cd "$_"
$ pip install doltpy
$ git clone https://github.com/liquidata-inc/liquidata-etl-jobs liquidata-etl-jobs
$ pip install -e liquidata-etl-jobs
```
Verify that the required commands are available in your path:
```
$ dolt-load
usage: dolt-load [-h] --dolt-dir DOLT_DIR [--commit] [--message MESSAGE]
                 [--branch BRANCH] [--dry-run]
                 dolt_load_module

$ dolthub-load
usage: dolthub-load [-h] [--dolt-dir DOLT_DIR] [--commit] [--message MESSAGE]
                    [--branch BRANCH] [--clone] --remote-url REMOTE_URL
                    [--remote-name REMOTE_NAME] [--push] [--dry-run]
                    dolt_load_module
```

## Running
There are several options for running scripts and where data will be written, they are enumerated below.

### Write to Local Dolt
There is a package called `local_write_example` that shows how to use the script `shared_loaders/dolt_load.py`. To start with ensure that you have a test Dolt repo set up:
```
[~/test-dolt] >> dolt init
Successfully initialized dolt data repository.
[~:test-dolt] >> dolt ls
No tables in working set
```
You can now run the script at the command line and see the results:
```
$ dolt-load \
    --commit \ 
    --dolt-dir $HOME/test-dolt \
    --message "Added some great players" \
    liquidata_etl.local_write_example
Commencing load to Dolt with the following options, and the following options
                - module    local_write_example
                - dolt_dir  /Users/oscarbatori/test-dolt
                - commit    True
                - branch    master
        
Loading Dolt repo at /Users/oscarbatori/test-dolt
Loading data to table great_players with primary keys ['name']
Importing 5 rows to table great_players in dolt directory located in /Users/oscarbatori/Documents/liquidata-etl-jobs, import mode create
Dropped 0 records with null values in the primary key columns ['name']
Adding great_players
Committing changes
```
Now you can go and run some SQL and check that you have indeed committed data to your Dolt database:
```
$ cd ~/test-dolt
$ dolt sql
doltsql> select * from great_players;
+----------------+--------------------+
| name           | all_time_greatness |
+----------------+--------------------+
| Marat Saffin   | 1                  |
| Novak Djokovic | 3                  |
| Pete Sampras   | 5                  |
| Rafael Nadal   | 4                  |
| Roger Federer  | 2                  |
+----------------+--------------------+

```
Try creating your own module that emits some useful data. 

Ready to start pushing data to DoltHub and contributing to DoltHub repos? Read on!
### Write to DoltHub
A similar tool exists for writing to DoltHub, which is also installed as part of the `doltpy` package, and is called `dolthub-load`. You can pass an expanded set of arguments to get the behavior that you want:
```
$ dolthub-load \
    --push \
    --remote Liquidata/ip-to-country \
    --message "2019-10-01 update to dataset" 
    --commit \
    --clone \
    ip_to_country.dolt_load.ip_loaders
```
This will perform the following steps:
1. clone the remote repository `Liquidata/ip-to-country` into a temporary directory
2. execute the function in the module `ip_to_country.get_dolt_datasets` and write those datasets to the local copy of the repository
3. generate a commit with the message specified by the `message` argument
4. push the commit back
Since we didn't specify a branch, we will default to `master`.

Try updating a public dataset you have write access to (and coming soon creating a pull request to on that you can't write to). Or, create your own repository and define a Python  module to update it.

### Airflow
Our workflow manager is [Apache Airflow](https://airflow.apache.org/), a tutorial that briefly explains the concept of DAGs and how to define them can be found [here](https://airflow.apache.org/tutorial.html). In order to automate a job define a module in `airflow_dags/you_dataset`. Then in `airflow_dags/dag.py` write code that looks something like:
```
from your_dataset.dolt_load import loaders as your_dataset_loaders

# Define DAG here
```
Then commit your code, and it will appear in the DAG [view](https://airflow.awsdev.ld-corp.com/admin/) of Airflow. You can test your module using the `dolt-load` and `dolthub-load` command line tools that come with Doltpy.
