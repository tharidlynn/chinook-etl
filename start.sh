#!/usr/bin/env bash
source .env

# prepare database
psql -d $PGDEFAULTDATABASE -U $PGUSERNAME -c "DROP DATABASE IF EXISTS ${PGDATABASE};"
psql -d $PGDEFAULTDATABASE -U $PGUSERNAME -c "CREATE DATABASE ${PGDATABASE};"
psql -d $PGDATABASE -U $PGUSERNAME -c "CREATE SCHEMA dwh;"

# import source 
psql $PGDATABASE -1 -f chinook_pg_serial_pk_proper_naming.sql &>populate_log.txt
# drop all irrelevant tables
python drop_dvd_tables.py
# etl
python etl.py

