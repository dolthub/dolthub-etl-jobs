## Background
[DoltHub](https://www.dolthub.com) hosts [Dolt](https://github.com/liquidata-inc/dolt) databases. If you don't know what Dolt is, then you will need to start with by familiarizing yourself with it before proceeding with this repo.

Liquidata is the company that created Dolt, and DoltHub. We host public data for free, and this repository is how much of the public data under the Liquidata organization is created and maintained. At a high level each module maintains a public repository. Each repository roughly corresponds to a data source.

## How does this work?
Dolt is a relational database, and thus one way to create Dolt tables is to create `pandas.DataFrame` objects, and then use `doltpy` to load them into a Dolt database. We run an [Airflow](https://airflow.apache.org/) instance that executes jobs that produce interesting public datasets, and then load them to Dolt.

You can help us make interesting data public and clean by adding a dataset to this repository, or adding jobs that transform and clean existing data on DoltHub. 


## Development
To start developing ETL jobs for Liquidata's public datasets you need to clone the code, and then install `doltpy` from source control, since it's not yet on `PyPi`:
```bash
$ git clone https://github.com/liquidata-inc/liquidata-etl-jobs liquidata-etl-jobs
$ pip install https://github.com/liquidata-inc/doltpy/blob/master/dist/doltpy-0.1.tar.gz
```

Now you have two options, either edit existing jobs, or create a new one.

### Edit
Suppose that you think `public_holidays` could be improved. Then you can submit a pull request to the `public_holidays` module that either changes the tables that are written (for example cleaning up types) then augment the existing `public_holidays` repo, and submit a pull request.

### New Dataset
If you want to add a new dataset then create a new module, and create a pull request for the Liquidata team to review.