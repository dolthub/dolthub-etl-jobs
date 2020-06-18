## Background
[Open Elections](http://openelections.net/) is a project to make high quality election data freely available. The main challenge in doing so is that the various state and county bodies that administer elections do not have uniform reporting methods. At a high level the process involves:
- identifying elections of interest, i.e. defining the scope of the data to be collected
- using some kind of task management tool to break it up into tasks that can be taken on by a volunteer
- checking the submissions and then publishing data

The project is managed via [GitHub](https://github.com/openelections/), where each state has a repository for sources, and a repository containing the CSV files that those sources are transformed into.

## Goal
Our goal is to transform the per state data repositories into a single Dolt repository containing every election result available. This is a database design problem as not all states have the same schema, but they do share some common properties. 

The outcome of loading a state should be twofold: data that we can actually look at in the repo, but perhaps more importantly your state specific module and whatever we add to the loader script to make it work for that script constitutes creating a repeatble process for parsing this data.

## Details
Each state reports at a minimum the vote acounts for each candidate or proposition at county, and potentially precinct, level. We aim to capture this data. However, states differ in how much detail they provide about the various types of voting that occur. For example some states may count absentee ballots, others merely give a total. With this in mind the schema design is something like:
```
Tables
    - elections                 : an index of federal and local elections per state
    - <state>_county_votes      : the county level vote tabulation for a given state
    - <state>_precinct_votes    : the precinct level vote tabulation for a given state
```
Thus we have to engage in a "schema discovery" step for each state to define that state's vote tables, and then we can proceed with loading the data. We now move onto the techncial details of how to achieve this.

### Output
The output of this work is two things: a DoltHub PR for the data loaded to a branch, and a GitHub PR to `liquidata-etl-jobs` that augments the `airflow_dags.open_elections` module with state specific details.

### Prerquisites
There are a few things needed before you start:
1. clone the Open Elections repository for whichever state you are interested in, for example California can be found [here](https://github.com/openelections/openelections-data-ca)
2. clone [liquidata-etl-jobs](https://github.com/liquidata-inc/liquidata-etl-jobs) and fetch branch `oscarbatori/open-elections` which contains the relevant code, if you need to update this code make a branch off of this branch and then make a pull request against (it's almost certain you will need to make changes)
3. ensure Doltpy 1.0.3 is installed as this contains critical code

You may also have to augment or define `PYTHONPATH` such that it uses the `liquidata-etl-jobs` in the module search path, like:
```
export PYTHONPATH=/absolute/path/to/liquidata-etl-jobs
``` 

This will ensure that when you pass in the state specific module created, say `airflow_dags.open_elections.ca` to `load_by_state` the import facilities will be able to resolve the module and grab the metadata needed to perform the load. 

### Schema Discovery
In order to create the schema you need to figure what the collection of vote count columns for each state are. For example this is California's vote count column set in `airflow_dags.open_elections.ca`: 
```python
VOTE_COUNT_COLS = ['votes',
                   'mail',
                   'election_day',
                   'absentee',
                   'reg_voters',
                   'total_ballots',
                   'early_votes',
                   'provisional']
```

Which is quite different from New York's:
```python
VOTE_COUNT_COLS = ['absentee',
                   'election_day',
                   'votes',
                   'assembly district',
                   'election district',
                   'machine',
                   'affidavit',
                   'federal',
                   'absentee_affidavit',
                   'absentee_hc',
                   'military',
                   'uocava',
                   'assembly_district',
                   'public_counter_votes',
                   'emergency_votes',
                   'absentee_military_votes',
                   'federal_votes',
                   'affidavit_votes',
                   'machine_votes']
```

This is a concrete example of the earlier statement about not all states having the same vote tabulation buckets. To help you discover the labels for you particular state you can `load_by_state.py` in show column mode. Here is an example where i use `$OE_DATA_ROOT` as a variable that stores the location of the cloned state by state data, and the Dolt repo I am loading them to. For now we just need the location of the state specific data:
```
$ python airflow_dags/open_elections/load_by_state.py --state ca --base-dir $OE_DATA_ROOT/openelections-data-ca --show-columns
```

This produces output as follows:
```
Potential vote count columns for county data are:
    ['votes']
ca_county_votes should have PK ['election_id', 'county', 'party', 'candidate']

Potential vote count columns for precinct data are:
    ['county', 'votes', 'election_day', 'mail', 'absentee', 'reg_voters', 'total_ballots', 'early_votes', 'provisional', 'polling', 'early_voting']
ca_county_votes should have PK ['election_id', 'precinct', 'party', 'candidate']

```
What this out put is telling us the columns that are not part of the nationwide elections table, or primary keys of the state specific table. Each state table as the same set of primary keys, but a different set of voting columns. This tells us those voting columns. Note, we can't generate SQL from this because of columns like `polling` that is a floating point value representing polling at the time of the election. So, this does _most_ of the work for us, but we still need to examine the data to make sure we are doing the right thing.

We then implement a state specific file, for California it looks like this:
```python 
from airflow_dags.open_elections.load_by_state import StateMetadata

VOTE_COUNT_COLS = ['votes',
                   'mail',
                   'election_day',
                   'absentee',
                   'reg_voters',
                   'total_ballots',
                   'early_votes',
                   'provisional']

metadata = StateMetadata(None, 'ca', VOTE_COUNT_COLS, None, None)
```

Note we the presence of a `metadata` variable. We pass this module path to the loader script and it reads this variable to configure the load. Before loading the data we need to use this information to create the requisite tables.

### Create Tables
Note one of the prerequisites was cloning the open elections data set from DoltHub. We can now use the information we got during the `--show-columns` run to create a schema:
```sql
CREATE TABLE ca_precinct_votes (
    `election_id` VARCHAR(256),
    `precinct` VARCHAR(256),
    `party` VARCHAR(256),
    `candidate` VARCHAR(256),
    `votes` INT,
    `mail` INT,
    `election_day` INT,
    `absentee` INT,
    `reg_voters` INT,
    `total_ballots` INT,
    `early_votes` INT,
    `provisional` INT,
    `polling` FLOAT,
    PRIMARY KEY (`election_id`, `precinct`, `party`, `candidate`)
)

CREATE TABLE ca_county_votes (
    `election_id` VARCHAR(256),
    `county` VARCHAR(256),
    `party` VARCHAR(256),
    `candidate` VARCHAR(256),
    `votes` INT,
    PRIMARY KEY (`election_id`, `county`, `party`, `candidate`)
)
```

Note the presence of the `polling` column that has datatype `float`, and primary keys that are generic across states. Now we have created the required tables we can proceed with loading the data.

### Data Loading
Earlier we created a staet specific module for California. We pass that to the script as follows, along with switches to indicate which datasets to load:
```
$  python airflow_dags/open_elections/load_by_state.py \
    --state ca \
    --base-dir $OE_DATA_ROOT/openelections-data-ca \
    --dolt-dir $OE_DATA_ROOT \
    --state-module airflow_dags.open_elections.ca \
    --load-elections \
    --load-precinct-votes \
    --load-county-votes
```

If it all worked out, then the data should get loaded into the your local copy of the repository. There are many issues with the data, and in the next section we cover some common hiccups.

## Trouble Shooting
This section covers some common issues which you might be able to solve yourself. The `load_by_state.py` script does not do a great job of breaking out state specific and shared logic, and so you might need to speak to me if something comes up that can't be fixed by adding a new case to, for example, the logic that removes weirdly formatted numbers. Here are some common issues:
1. the state contains files that do not match our file parsing logic, and so that logic needs to be augmented (note it is important because we extract metadata from the file name)
2. the voting count columns contain weird strings that extra logic is needed to extract an integer from
3. the state does not have the primary key columns for either the `elections` table or the state specific tables that share primary key

Let me know if anything seemingly intractable comes up.