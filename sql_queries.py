import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = ("""CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1)
    , artist_name VARCHAR(255)
    , auth VARCHAR(50)
    , user_first_name VARCHAR(255)
    , user_gender  VARCHAR(1)
    , item_in_session INTEGER
    , user_last_name VARCHAR(255)
    , song_length DOUBLE PRECISION
    , user_level VARCHAR(50)
    , location VARCHAR(255)
    , method VARCHAR(25)
    , page VARCHAR(35)
    , registration VARCHAR(50)
    , session_id BIGINT
    , song_title VARCHAR(255)
    , status INTEGER
    , ts VARCHAR(50)
    , user_agent TEXT
    , user_id VARCHAR(100)
    , PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
    song_id VARCHAR(100)
    , num_songs INTEGER
    , artist_id VARCHAR(100)
    , artist_latitude DOUBLE PRECISION
    , artist_longitude DOUBLE PRECISION
    , artist_location VARCHAR(255)
    , artist_name VARCHAR(255)
    , title VARCHAR(255)
    , duration DOUBLE PRECISION
    , year INTEGER
    , PRIMARY KEY (song_id))
""")

songplay_table_create = ("""CREATE TABLE songplays(
    songplay_id INT IDENTITY(0,1)
    , start_time TIMESTAMP REFERENCES time(start_time)
    , user_id VARCHAR(50) REFERENCES users(user_id)
    , level VARCHAR(50)
    , song_id VARCHAR(100) REFERENCES songs(song_id)
    , artist_id VARCHAR(100) REFERENCES artists(artist_id)
    , session_id BIGINT
    , location VARCHAR(255)
    , user_agent TEXT
    , PRIMARY KEY (songplay_id))
""")

user_table_create = ("""CREATE TABLE users(
    user_id VARCHAR
    , first_name VARCHAR(255)
    , last_name VARCHAR(255)
    , gender VARCHAR(1)
    , level VARCHAR(50)
    , PRIMARY KEY (user_id))
""")

song_table_create = ("""CREATE TABLE songs(
    song_id VARCHAR(100)
    , title VARCHAR(255)
    , artist_id VARCHAR(100) NOT NULL
    , year INTEGER
    , duration DOUBLE PRECISION
    , PRIMARY KEY (song_id))
""")

artist_table_create = ("""CREATE TABLE artists(
    artist_id VARCHAR(100)
    , name VARCHAR(255)
    , location VARCHAR(255)
    , latitude DOUBLE PRECISION
    , longitude DOUBLE PRECISION
    , PRIMARY KEY (artist_id))
""")

time_table_create = ("""CREATE TABLE time(
    start_time TIMESTAMP
    , hour INTEGER
    , day INTEGER
    , week INTEGER
    , month INTEGER
    , year INTEGER
    , weekday INTEGER
    , PRIMARY KEY (start_time))
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY events_stage FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2' 
    JSON '{}'""").format(config.get('S3', 'LOG_DATA'),
                         config.get('IAM_ROLE', 'ARN'),
                         config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY songs_stage FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2' 
    JSON 'auto'
    """).format(config.get('S3', 'SONG_DATA'),
                config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays(start_time
    , user_id
    , level
    , song_id
    , artist_id
    , session_id
    , location
    , user_agent)
SELECT 
    TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time
    , e.user_id
    , e.level
    , s.song_id
    , e.artist_id
    , e.session_id
    , e.location
    , e.user_agent    
FROM staging_events e
    , staging_songs s
        WHERE e.page = 'NextSong'
        AND e.song_title = s.title
        AND user_id NOT IN (
    SELECT DISTINCT s.user_id 
    FROM songplays s 
        WHERE s.user_id = user_id
        AND s.start_time = start_time AND s.session_id = session_id )
""")

user_table_insert = ("""
INSERT INTO users(user_id
    , first_name
    , last_name
    , gender
    , level)
WITH unique_user AS (
    SELECT DISTINCT 
      pes.user_id
    , pes.first_name,
    , pes.last_name,
    , pes.gender,
    , pes.level
    FROM staging_events
        WHERE page = 'NextSong'
        AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
INSERT INTO songs(song_id
    , title
    , artist_id
    , year
    , duration)
SELECT DISTINCT
    pss.song_id
    , pss.title
    , pss.artist_id
    , pss.year
    , pss.duration
FROM songs_stage pss
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id
    , name
    , location
    , lattitude
    , longitude)
SELECT DISTINCT
    pss.artist_id
    , pss.artist_name
    , pss.artist_location
    , pss.artist_latitude 
    , pss.artist_longitude
FROM songs_stage pss;
""")

time_table_insert = ("""
INSERT INTO time(start_time
    , hour
    , day
    , week
    , month
    , year
    , weekday)
WITH time_parse AS
(
    SELECT
        DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time
    FROM events_stage
    )
SELECT
    tp.start_time AS start_time
    , EXTRACT (hour from tp.start_time) AS hour
    , EXTRACT (day from tp.start_time) AS day
    , EXTRACT (week from tp.start_time) AS week
    , EXTRACT (month from tp.start_time) AS month
    , EXTRACT (year from tp.start_time) AS year
    , EXTRACT (dow from tp.start_time) AS weekday
FROM time_parse tp;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create
    , staging_songs_table_create
    , songplay_table_create
    , user_table_create
    , song_table_create
    , artist_table_create
    , time_table_create]

drop_table_queries = [staging_events_table_drop
    , staging_songs_table_drop
    , songplay_table_drop
    , user_table_drop
    , song_table_drop
    , artist_table_drop
    , time_table_drop]

copy_table_queries = [staging_events_copy
    , staging_songs_copy]

insert_table_queries = [songplay_table_insert
    , user_table_insert
    , song_table_insert
    , artist_table_insert
    , time_table_insert]
