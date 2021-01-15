# Summary of the project, how to run the Python scripts, and an explanation of the files in the repository

A music streaming startup, Sparkify, has grown their user base and 
song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, 
as well as a directory with JSON metadata on the songs in their app.

This project is an example of building an ETL pipeline
that extracts their data from S3, stages them in Redshift, and transforms data
into a set of dimensional tables for their analytics team to continue finding
insights in what songs their users are listening to. You'll be able to test your
database and ETL pipeline by running queries given to you by the analytics team from
Sparkify and compare your results with their expected results.

First requirement is to create a redshift cluster, an IAM role and open incoming TCP 
port to access the cluster endpoint.

## 1. AWS Redshift Database configuration file.

After initial setup you can complete **dwh.cfg** with the correspondent values:

[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

## 2. Queries.

In sql_queries.py we have defined all the statements necessary to CREATE, DROP and INSERT the data in staging events and songs 
or facts and dimension tables which make a Star Schema Design.

This file is used in create_tables.py and etl.py to properly connect to the AWS Redshift Database.

## 3. Tables Creation

The first script to run is create_tables.py. The role of these files as its name suggests is to create the tables
we are using in this application or initially drop the existing tables (staging,fact and dimension) from the database if them exists and 
after create them.

## 4. ETL Processing and STAR SCHEMA

In etl.py once we have the tables in database we can retrieve the data from S3 and load it in the staging tables 
using the COPY command which offer us a better performance than simple insert query.
From staging tables the facts and dimension tables needs joining the tables and processing the data before insert. So in this case I'm using 
**INSERT INTO SELECT** statements to fill them.






