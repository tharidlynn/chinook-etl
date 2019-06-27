# Chinook Database ETL Project
This project is a simple demonstration using [Pandas](https://pandas.pydata.org/) library to perform [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load). Writing code like Python has a lot of advantages such as reproducibility, debugging and being agnostic-tools/vendors.

## Installation prerequisites

* Pandas (Python)
* Sqlalchemy (Python)
* PostgreSQL Database
* Jupyter notebook (Optional)

## Data Source
The [Chinook](https://github.com/lerocha/chinook-database) is a sample database available for SQL Server, Oracle, MySQL, etc. It can be created by running a single SQL script. Chinook database is an alternative to the Northwind database, being ideal for demos and testing ORM tools targeting single and multiple database servers.

However, I decide to populate a table with the [PG modified SQL](https://github.com/xivSolutions/ChinookDb_Pg_Modified/blob/master/chinook_pg_serial_pk_proper_naming.sql) instead because it is much easier to install and also follows the PostgreSQL conventions; you can read more at [John Atten's blog](http://johnatten.com/2015/04/05/a-more-useful-port-of-the-chinook-database-to-postgresql/
)
### Data model

The Chinook data model represents a digital media store, including tables for artists, albums, media tracks, invoices and customers.


![chinook-model](https://github.com/tharid007/chinook-etl/blob/master/chinook-model.png?raw=true)

_Credit: https://github.com/lerocha/chinook-database/wiki/Chinook-Schema_


### Target model
The final model is a star schema optimized for a typical data warehouse. 

![chinook-star-schema](https://github.com/tharid007/chinook-etl/blob/master/chinook-star-schema.png?raw=true)


## Getting Started
1. Open the `.env` and modify all environments to match yours
2. Execute `./start.sh` | this script will automatically create, transform and load data into a "dwh" schema.
3. Explore some SQL scripts in `business_queries.ipynb`
    
