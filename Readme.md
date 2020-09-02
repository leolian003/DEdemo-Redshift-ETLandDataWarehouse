# ETL pipeline for a redshift-based Data Warehouse

## Overview 

This is an ETL pipeline that extracts log data from AWS S3, stages it in AWS Redshift, and transforms it into dimensional tables for further data analysis. This project is a part of [Udacity's Data Engineering nanodegree](https://eu.udacity.com/course/data-engineer-nanodegree--nd027).

## Getting started

1. Based on Step1 - Step5 of AWS's [guideline](https://docs.aws.amazon.com/redshift/latest/gsg/getting-started.html), create a Redshift cluster and keep important information on record. 
    * When creating an IAM Role (step 2), makes Redshift able to access S3 bucket (ReadOnly). 
    * When creating the Redshift Cluster, select:
        * "dc2.large" as node type
        * "multi-node" as cluster type
        * "4" as number of nodes
    * After the cluster is successfully created, take down the following key information 
        * Endpoint that hosts the cluster: `*.REGION.redshift.amazonaws.com`
        * Redshift IAM Role's ARN: `arn:aws:iam::*:role/*`
        * Database Name
        * Database User Name
        * Access Password
        * Database PORT (eg. 5439)

2. Edit the open field of AWS-setup.cfg in this repo. 
    * Put `endpoint` in the "Host" field.
    * Other fields are straightforward.

3. To build the pipeline, run the python scripts in the following order:
    * create_tables
    * etl.py

4. Log into the AWS console and use query editor to conduct data analysis. (Alternatively, use `troubleshooting.ipynb` in this repo to explore the database.)

## Project Background

As requested by a hypothetical music streaming service startup, Sparkify, we are asked to build an ETL pipeline for its activity log and song metadata that resides in S3 in a json format. 

## ETL design

A data warehouse (OLAP) is hosted on the AWS Redshift, which is a columnar database based on open source PostgreSQL. Before being brought to the redshift, the input data source is staged at AWS S3 (in json format) to accomodate the transition between different RBDMS. 

## Data Warehouse design based on customer's demand

The client provides several use cases of this data warehouse: 

* What is the most popular song for North America users in the past week? 
* Who is the second popular artist among teenagers in the Europe. 
* Top 5 genres that are played most frequently during the rush hours. 

Based on the demands, we can develop four dimensional tables and one star tables for the data warehouse. They are:  

* factSongPlays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
* dimSong: song_id, title, artist_id, year, duration
* dimArtist: artist_id, name, location, latitude, longitude
* dimUser: user_id, first_name, last_name, gender, level
* dimTime: start_time, hour, day, week, month, year, weekday
* dimLocation (optional): Locid, Location, city, county, state, country,longitude, latitude


## Overview of data source

### Log data

Hosted in S3 (s3://udacity-dend/log_data), the log data contains user's activity in the app, in the scope of this project we only care about the app usage when "Next Song" button is hitted (by users)


### Song metadata

Hosted in S3 (s3://udacity-dend/song_data), the song metadata contains the basic information of songs and related artists.


