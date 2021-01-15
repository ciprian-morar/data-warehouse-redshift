import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN=config.get('IAM_ROLE', 'ARN')
LOG_DATA=config.get('S3', 'LOG_DATA')
LOG_DATA_JSON=config.get('S3', 'LOG_JSONPATH')
SONG_DATA=config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    staging_event_id int identity(0,1),
    artist varchar,
    auth varchar,
    firstName varchar,
    gender char(1),
    itemInSession integer,
    lastname varchar,
    length real,
    level varchar(5),
    location varchar,
    method varchar(10),
    page varchar,
    registration double precision,
    sessionId int,
    song varchar,
    status smallint,
    ts timestamp,
    userAgent text,
    userId int,
    PRIMARY KEY(staging_event_id));
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    staging_song_id int identity(0,1),
    num_songs int,
    artist_id text,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location text,
    artist_name varchar,
    song_id varchar,
    title text,
    duration numeric,
    year int,
    PRIMARY KEY(staging_song_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar, 
    first_name varchar, 
    last_name varchar,
    gender char(1), 
    level varchar(5), 
    PRIMARY KEY(user_id))
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar, 
    name varchar, 
    location varchar, 
    latitude numeric, 
    longitude numeric, 
    PRIMARY KEY(artist_id))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar, 
    title varchar, 
    year int, 
    duration numeric, 
    artist_id varchar not null,
    FOREIGN KEY(artist_id)
        REFERENCES artists(artist_id),
    PRIMARY KEY(song_id))
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp, 
    hour int, day int, 
    week int, 
    month int, 
    year int, 
    weekday int,
    PRIMARY KEY(start_time))
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT identity(0,1), 
    start_time timestamp not null, 
    user_id varchar not null, 
    level varchar,
    song_id varchar DEFAULT NULL, 
    artist_id varchar DEFAULT NULL, 
    session_id int, location varchar,
    user_agent varchar,
    FOREIGN KEY(song_id)
          REFERENCES songs(song_id),
    FOREIGN KEY(artist_id)
          REFERENCES artists(artist_id),
    FOREIGN KEY(user_id)
          REFERENCES users(user_id),
    FOREIGN KEY(start_time)
          REFERENCES time(start_time),
    PRIMARY KEY(songplay_id))
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {} 
    credentials 'aws_iam_role={}' 
    json {}
    compupdate off region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
""").format(LOG_DATA, ARN, LOG_DATA_JSON)

staging_songs_copy = ("""
    copy staging_songs from {} 
    credentials 'aws_iam_role={}' 
    json 'auto'
    compupdate off region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""


begin transaction;
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
 SELECT   a.ts,
         a.userId,
         a.level,
         b.song_id,
         b.artist_id,
         a.sessionId,
         a.location,
         a.userAgent
FROM staging_events AS a
JOIN staging_songs AS b ON (a.artist = b.artist_name)
WHERE a.page='NextSong' AND a.artist IS NOT NULL;


end transaction; 

""")

user_table_insert = ("""
begin transaction;

INSERT INTO users(user_id, first_name, last_name, gender, level) 
select userId as user_id, firstName as first_name, lastName as last_name, gender, level
from staging_events a 
join (select max(staging_event_id) staging_event_id
           from staging_events 
           group by userId ) b on a.staging_event_id = b.staging_event_id
WHERE user_id IS NOT NULL;
end transaction;
""")

song_table_insert = ("""
begin transaction;
INSERT INTO songs(song_id, title, duration, year, artist_id)
SELECT a.song_id, a.title, a.year, a.duration, a.artist_id
FROM staging_songs a
     join (select max(staging_song_id) staging_song_id, song_id 
           from staging_songs 
           group by song_id ) b on a.song_id = b.song_id and a.staging_song_id = b.staging_song_id
WHERE a.song_id IS NOT NULL;
end transaction;
""")

artist_table_insert = ("""
begin transaction;
INSERT INTO artists (artist_id, name, location, latitude, longitude)
select a.artist_id, a.artist_name as name, a.artist_location, a.artist_latitude, a.artist_longitude
from staging_songs a
join (select max(staging_song_id) staging_song_id, artist_id 
           from staging_songs 
           group by artist_id ) b on a.artist_id = b.artist_id and a.staging_song_id = b.staging_song_id
WHERE a.artist_id IS NOT NULL;
end transaction;
""")

time_table_insert = ("""
begin transaction;
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
select a.ts,EXTRACT(hour from a.ts) AS hour, EXTRACT(day from a.ts) AS day,  EXTRACT(week from a.ts) AS week, EXTRACT(month from a.ts) AS month, 
EXTRACT(year from a.ts) AS year, EXTRACT(weekday from a.ts) AS weekday
from staging_events a 
join (select DISTINCT staging_event_id
           from staging_events 
           group by ts, staging_event_id ) b on a.staging_event_id = b.staging_event_id
WHERE a.ts IS NOT NULL;
end transaction;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create,  time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
