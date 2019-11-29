# Data Warehouse using Redshift

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. Database and ETL pipeline can be tested by running queries given by the analytics team from Sparkify.

## Project Description
In this project, I'll apply what I've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, I will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in data song and log directories into these tables in Postgres using Python and SQL.

## Getting started
To start ETL process, run the following from your terminal.
`Set HOSTNAME and IAM roles in dwh.cfg`
`python create_tables.py`</br>
`python etl.py`

## Python scripts and Jupyter notebooks

- create_tables.py: Clean previous schema and creates staging, fact and dimension tables.
- sql_queries.py: All queries used in the ETL pipeline.
- etl.py: Read JSON logs and JSON metadata and load the data into generated tables.
- README.md: Readme explaining this Data modeling project.

## Database Schema
![ERD](Sparkify.vpd.png)

- songplays: Records in log data associated with song plays
- users: Users in the app
- songs: Songs in music database
- artists: Artists in music database
- time: Timestamps of records in songplays broken down into specific units

## ETL Pipeline Details


### song_data ETL

#### Source dataset
Song data resides in S3 - s3://udacity-dend/song_data
Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

`song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
`

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```json
{
  "num_songs": 1,
  "artist_id": "ARJIE2Y34JH994AB7",
  "artist_latitude": null,
  "artist_longitude": null,
  "artist_location": "",
  "artist_name": "Joe Tayler",
  "song_id": "SE567HG8999HHG",
  "title": "Der Kleine Dompfaff",
  "duration": 68.98,
  "year": 1980
}
```

### log_data ETL

#### Source dataset
Data resides in S3
Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json

The log files are partitioned by year and month. For example, here are filepath in this dataset.

`log_data/2018/11/2018-11-01-events.json
`

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.
```json
{
  "artist": "Pavement",
  "auth": "Logged In",
  "firstName": "Sylvie",
  "gender": "F",
  "itemInSession": 0,
  "lastName": "Cruz",
  "length": 109.1683,
  "level": "free",
  "location": "San Francisco, CA",
  "method": "PUT",
  "page": "NextSong",
  "registration": 7867266185796.0,
  "sessionId": 775,
  "song": "Mercy:The Laundromat",
  "status": 200,
  "ts": 257890258796,
  "userAgent": "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.77.4 (KHTML, like Gecko) Version/7.0.5 Safari/537.77.4\"",
  "userId": "10"
}
```

#### Final tabes
Fact Table

* songplays - records in log data associated with song plays i.e. records with page NextSong

   **  _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

Dimension Tables

* users - users in the app

   ** _user_id, first_name, last_name, gender, level_

* songs - songs in music database

   ** _song_id, title, artist_id, year, duration_

* artists - artists in music database

    ** _artist_id, name, location, latitude, longitude_

* time - timestamps of records in songplays broken down into specific units

    ** _start_time, hour, day, week, month, year, weekday_


REFERENCES
* https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
* https://classroom.udacity.com/nanodegrees/nd027/parts/69a25b76-3ebd-4b72-b7cb-03d82da12844/modules/445568fc-578d-4d3e-ab9c-2d186728ab22/lessons/21d59f40-6033-40b5-81a2-4a3211d9f46e/concepts/0d489f89-73ba-4843-b715-34769879a64a
* https://classroom.udacity.com/nanodegrees/nd027/parts/69a25b76-3ebd-4b72-b7cb-03d82da12844/modules/445568fc-578d-4d3e-ab9c-2d186728ab22/lessons/21d59f40-6033-40b5-81a2-4a3211d9f46e/concepts/6afa911d-b5ec-4ced-b286-c4bdb354a9a7