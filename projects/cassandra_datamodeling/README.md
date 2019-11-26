# Data Modeling with Cassandra

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data resides in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions.
Database can be tested by running queries given by the analytics team from Sparkify to create the results.

## Project Description

In this project, I'll apply what I have learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python. To complete the project, I will need to model my data by creating tables in Apache Cassandra to run queries. ETL pipeline transfers data from a set of CSV files within event_data directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

## Getting started
To start ETL process, run the Project_1B_ Project_Template.ipynb jupyter notebook.


## Python scripts and Jupyter notebooks
- Project_1B_ Project_Template.ipynb: Notebook that reads CSV files and creates cassandra database.
- event_data: All songs and user_activity CSV files. Source dataset provided.
- images: image files screenshot of what the denormalized data should appear like in  <font color=red>event_datafile_new.csv</font> 
- etl.ipynb: Notebook to prototype etl scripts to load the data.
- event_datafile_new.csv: a smaller event data csv file that will be used to insert data into cassandra tables.
- README.md: Readme explaining this Data modeling project.

## Database Schema 

* session_activity - Session activity
   
   ** _session_id, item_in_session, artist, song, length_

* user_activity - songs in music database
   
   **  _user_id, session_id, item_in_session, artist, song, first_name, last_name_

* song_activity - artists in music database
   
   ** _song, user_id, first_name, last_name_

## REFERENCES
* http://cassandra.apache.org/doc/latest/cql/ddl.html?highlight=primary%20key#grammar-token-clustering-columns
* https://classroom.udacity.com/nanodegrees/nd027/parts/f7dbb125-87a2-4369-bb64-dc5c21bb668a/modules/c0e48224-f2d0-4bf5-ac02-3e1493e530fc/lessons/73fd6e35-3319-4520-94b5-9651437235d7/concepts/864ae5c9-e9fb-47ca-bcde-b152d00543a1
