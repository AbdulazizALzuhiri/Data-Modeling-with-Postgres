# Project: Data Modeling with Postgres

## Introduction

A project called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The project aim to make the data more usble for analytics team,by creating ETL pipeline for the current data to make an easy way to query the data.

## Project Description

In this project, we'll have an easy way to query users data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. We'll build an ETL pipeline using Python and define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.


## Datasets

Using two datasets Song Dataset `data/song_data` and Log Dataset `data/log_data` both in JSON format.

### Song Dataset format example

```
{"num_songs": 1,
"artist_id": "AR558FS1187FB45658",
"artist_latitude": null,
"artist_longitude": null,
"artist_location": "",
"artist_name": "40 Grit",
"song_id": "SOGDBUF12A8C140FAA",
"title": "Intro",
"duration": 75.67628,
"year": 2003}
```

### Log Dataset format example

```
{"artist":"Miami Horror",
"auth":"LoggedIn",
"firstName":"Kate",
"gender":"F",
"itemInSession":88,
"lastName":"Harrell",
"length":250.8273,
"level":"paid",
"location":"Lansing-East Lansing,MI", "method":"PUT",
"page":"NextSong",
"registration":1540472624796.0,
"sessionId":293,
"song":"Sometimes",
"status":200,
"ts":1541548876796,
"userAgent":"\"Mozilla\/5.0 (X11; Linux x86_64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/37.0.2062.94 Safari\/537.36\"",
"userId":"97"}
```

## Schema

Using the song and log datasets, we'll create a star schema optimized for queries on song play analysis. This includes the following tables.

![Sparkify star schema](sparkify_star_schema.png?raw=true)

## Files included and how to run it 

The source code has 3 main python scripts.

1. `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.

2. `sql_queries.py` contains all your sql queries, and is imported into the last three files above.

3. `etl.py` reads and processes files from `song_data` and `log_data` and loads them into your tables.
 You can fill this out based on your work in the ETL notebook.

To run it now first we run `create_tables.py` then `etl.py` so first we create the DB, tabels and insert the data using ETL pipeline.

The other files are:

4. `test.ipynb` displays the first few rows of each table to let you check your database.

5. `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into your tables.
 This notebook contains detailed instructions on the ETL process for each of the tables.

6. `data` where we store two datasets Song Dataset `data/song_data` and Log Dataset `data/log_data`.
