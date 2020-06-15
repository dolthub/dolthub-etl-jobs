
## Background
[Open Elections](http://openelections.net/) is a project to make high quality election data freely available. The main challenge in doing so is that the various state and county bodies that administer elections do not have uniform reporting methods. At a high level the process involves:
- identifying elections of interest, i.e. defining the scope of the data to be collected
- using some kind of task management tool to break it up into tasks that can be taken on by a volunteer
- checking the submissions and then publishing data

The project is managed via [GitHub](https://github.com/openelections/), where each state has a repository for sources, and a repository containing the CSV files that those sources are transformed into.

## Goal
Our goal is to transform the per state data repositories into a single Dolt repository containing every election result available. This is a database design problem as not all states have the same schema, but they do share some common properties.

## Details
We break the schema up into an `elections` table, and then each state has results tables which is a child named `<state>_<county|precinct>_results`. So, for the current data we have, we would have something like:
```
open_elections> show tables;
+-------------------+
| Table             |
+-------------------+
| ca_county_votes   |
| ca_precinct_votes |
| elections         |
| ny_county_votes   |
| ny_precinct_votes |
+-------------------+
```

The file `tools.py` contains code that is pointed at a base directory for the repo containing the relevant data. These repositories have the following naming convention:
```
https://github.com/openelections/openelections-data-<state>
```

To get the data for a specific state in requires extracting a schema for the `<state>_<county|precinct>_votes` tables by inspecting the raw data. Each state tabulates votes in slightly different category. For example, California has the following tables:
```sql
CREATE TABLE ca_county_votes (
    `election_id` VARCHAR(256),
    `county` VARCHAR(256),
    `party` VARCHAR(256),
    `candidate` VARCHAR(256),
    `votes` INT,
    PRIMARY KEY (`election_id`, `county`, `party`, `candidate`)
)

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
```

The `election_id` is a foreign key referencing the parent table we created. The parent table schema is:
```sql
CREATE TABLE elections (
    `state` VARCHAR(256),
    `year` INT,
    `date` DATETIME,
    `election` VARCHAR(256),
    `special` BOOLEAN,
    `office` VARCHAR(256),
    `district` VARCHAR(256),
    `hash_id` VARCHAR(256),
    `count` INT,
    PRIMARY KEY (`hash_id`)
)
```

Thus to run the california sce