import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Staging table for log data. See log-data.png for data modeling.
staging_events_table_create= ("""
CREATE TABLE staging_events 
(
    artist          VARCHAR(300),
    auth            VARCHAR(50),
    first_name      VARCHAR(50),
    gender          VARCHAR(1),
    item_in_session INTEGER, 
    last_name       VARCHAR(50),
    length          DECIMAL(10, 5),
    level           VARCHAR(10),
    location        VARCHAR(300),
    method          VARCHAR(6),
    page            VARCHAR(50),
    registration    DECIMAL(14, 1),
    session_id      INTEGER,
    song            VARCHAR(300),
    status          INTEGER,
    ts              BIGINT,
    user_agent      VARCHAR(150),
    user_id         VARCHAR(10)
)
""")

# Staging for song data in S3.
# Example row "num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
    num_songs        INTEGER,
    artist_id        VARCHAR(50), 
    artist_latitude  DECIMAL(10, 5),
    artist_longitude DECIMAL(10, 5),
    artist_location  VARCHAR(300),
    artist_name      VARCHAR(300),
    song_id          VARCHAR(50),
    title            VARCHAR(300),
    duration         DECIMAL(10, 5),
    year             INTEGER
)
""")

# FACT TABLE: songplays - records in event data associated with song plays i.e. records with page NextSong
#    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL, 
    user_id     VARCHAR(10),
    level       VARCHAR(10),
    song_id     VARCHAR(300) NOT NULL,
    artist_id   VARCHAR(50) NOT NULL,
    session_id  INTEGER,
    location    VARCHAR(300),
    user_agent  VARCHAR(150),
    UNIQUE (start_time, user_id, session_id)
)
""")

# DIM users - users in the app
#    user_id, first_name, last_name, gender, level
user_table_create = ("""
CREATE TABLE users (
    user_id    VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name  VARCHAR(50),
    gender     VARCHAR(1),
    level      VARCHAR(10)
)
""")

# DIM songs - songs in music database
#    song_id, title, artist_id, year, duration
song_table_create = ("""
CREATE TABLE songs (
    song_id   VARCHAR(50) PRIMARY KEY,
    title     VARCHAR(300) NOT NULL,
    artist_id VARCHAR(50),
    year      INTEGER,
    duration  DECIMAL(10, 5) NOT NULL
)
""")

# DIM artists - artists in music database
#    artist_id, name, location, lattitude, longitude
artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR(50) PRIMARY KEY,
    name      VARCHAR(300) NOT NULL,
    location  VARCHAR(300),
    lattitude DECIMAL(10, 5),
    longitude DECIMAL(10, 5)
)
""")

# DIM time - timestamps of records in songplays broken down into specific units
#    start_time, hour, day, week, month, year, weekday
time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
)
""")

# STAGING TABLES
# REFERENCE: https://classroom.udacity.com/nanodegrees/nd027/parts/69a25b76-3ebd-4b72-b7cb-03d82da12844/modules/445568fc-578d-4d3e-ab9c-2d186728ab22/lessons/21d59f40-6033-40b5-81a2-4a3211d9f46e/concepts/0d489f89-73ba-4843-b715-34769879a64a
# https://classroom.udacity.com/nanodegrees/nd027/parts/69a25b76-3ebd-4b72-b7cb-03d82da12844/modules/445568fc-578d-4d3e-ab9c-2d186728ab22/lessons/21d59f40-6033-40b5-81a2-4a3211d9f46e/concepts/6afa911d-b5ec-4ced-b286-c4bdb354a9a7

staging_events_copy = ("""
COPY staging_events FROM {} 
iam_role {}
FORMAT AS JSON {};
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'), 
    config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM {} 
iam_role {}
FORMAT AS JSON 'auto';
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES
# Ref: https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + (se.ts / 1000) * interval '1 second',
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
FROM staging_events se
INNER JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
WHERE se.page = 'NextSong'
""")

# Get most recent level of user.
user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT 
    se.user_id,
    se.first_name,
    se.last_name,
    se.gender,
    se.level
FROM staging_events se
WHERE NOT EXISTS (SELECT 1 FROM staging_events se2 WHERE se.user_id = se2.user_id AND se.ts < se2.ts) 
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    ss.song_id,
    ss.title,
    ss.artist_id,
    CASE WHEN ss.year != 0 THEN ss.year ELSE null END AS year,
    ss.duration
FROM staging_songs ss
""")

# REF: https://docs.aws.amazon.com/redshift/latest/dg/r_Examples_of_rank_WF.html
artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, lattitude, longitude)
SELECT 
    artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM (
  SELECT
      ss.artist_id,
      ss.artist_name,
      ss.artist_location,
      ss.artist_latitude,
      ss.artist_longitude,
      RANK() OVER(PARTITION BY ss.artist_id ORDER BY ss.year desc) AS rank
  FROM staging_songs ss
)
WHERE rank = 1
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT 
    start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(dow FROM start_time) AS weekday
FROM (
  SELECT DISTINCT sp.start_time FROM songplays sp
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
