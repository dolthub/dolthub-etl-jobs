# DoltHub ETL Jobs

This package contains legacy code used in an [Airflow](https://airflow.apache.org/) pipeline to update public databases under the 
[`dolthub` organization](https://www.dolthub.com/organizations/dolthub) on [DoltHub](https://www.dolthub.com). It also stores some 
adhoc scripts that were never meant to be run continuously but were used to import the data from its source. If you find an 
interesting database on DoltHub under the `dolthub` organization, you may find the code that created it here. If you are wondering,
just ask us on our [Discord](https://discord.com/invite/RFwfYpu).

[DoltHub](https://www.dolthub.com), the company, focused on making [Dolt](https://github.com/dolthub/dolt) a full-fledged
[version controlled database](https://www.dolthub.com/blog/2021-09-17-database-version-control/). We deprecated our Airflow
instance. These scripts are here for posterity sake.
