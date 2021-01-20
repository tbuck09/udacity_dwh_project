import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

HOST= config.get("CLUSTER", "HOST")
DB_NAME= config.get("CLUSTER", "DB_NAME")
DB_USER= config.get("CLUSTER", "DB_USER")
DB_PASSWORD= config.get("CLUSTER", "DB_PASSWORD")
DB_PORT= config.get("CLUSTER", "DB_PORT")

ARN= config.get("IAM_ROLE", "ARN")

LOG_DATA= config.get("S3", "LOG_DATA")
LOG_JSONPATH= config.get("S3", "LOG_JSONPATH")
SONG_DATA= config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_events (
        "artist" VARCHAR NULL,
        "auth" VARCHAR NOT NULL,
        "firstName" VARCHAR NULL,
        "gender" VARCHAR NULL,
        "itemInSession" INT NULL,
        "lastName" VARCHAR NULL,
        "length" NUMERIC NULL,
        "level" VARCHAR NOT NULL,
        "location" VARCHAR NULL,
        "method" VARCHAR NOT NULL,
        "page" VARCHAR NOT NULL,
        "registration" BIGINT NULL,
        "sessionId" INT NULL,
        "song" VARCHAR NULL,
        "status" INT NOT NULL,
        "ts" BIGINT NOT NULL,
        "userAgent" VARCHAR NULL,
        "userId" VARCHAR NULL
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stage_songs (
        "artist_id" VARCHAR NULL,
        "artist_latitude" NUMERIC NULL,
        "artist_location" VARCHAR NULL,
        "artist_longitude" NUMERIC NULL,
        "artist_name" VARCHAR NOT NULL,
        "duration" NUMERIC NULL,
        "num_songs" INT NULL,
        "song_id" VARCHAR NOT NULL,
        "title" VARCHAR NULL,
        "year" INT NULL
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        "songplay_id" INT   NOT NULL,
        "start_time" BIGINT   NULL,
        "user_id" VARCHAR   NULL,
        "level" VARCHAR   NULL,
        "song_id" VARCHAR  NULL,
        "artist_id" VARCHAR   NULL,
        "session_id" INT   NULL,
        "location" VARCHAR   NULL,
        "user_agent" VARCHAR   NULL,
        CONSTRAINT "pk_songplays" PRIMARY KEY (
            "songplay_id","session_id"
         )
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        "user_id" VARCHAR   NOT NULL,
        "first_name" VARCHAR   NULL,
        "last_name" VARCHAR   NULL,
        "gender" VARCHAR   NULL,
        "level" VARCHAR   NULL,
        CONSTRAINT "pk_users" PRIMARY KEY (
            "user_id"
            )
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        "song_id" VARCHAR   NOT NULL,
        "title" VARCHAR   NULL,
        "artist_id" VARCHAR   NULL,
        "year" INT   NULL,
        "duration" NUMERIC   NULL,
        CONSTRAINT "pk_songs" PRIMARY KEY (
            "song_id"
         )
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        "artist_id" VARCHAR   NOT NULL,
        "name" VARCHAR   NULL,
        "location" VARCHAR   NULL,
        "latitude" NUMERIC   NULL,
        "longitude" NUMERIC   NULL,
        CONSTRAINT "pk_artists" PRIMARY KEY (
            "artist_id"
        )
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        "start_time" BIGINT   NOT NULL,
        "hour" INT   NOT NULL,
        "day" INT   NOT NULL,
        "week" INT   NOT NULL,
        "month" INT   NOT NULL,
        "year" INT   NOT NULL,
        "weekday" INT   NOT NULL,
        CONSTRAINT "pk_time" PRIMARY KEY (
            "start_time"
         )
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY stage_events FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF REGION 'us-west-2'
    JSON 'auto ignorecase'
""").format(LOG_DATA, ARN)

staging_songs_copy = ("""
    COPY stage_songs FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF REGION 'us-west-2'
        JSON 'auto ignorecase'
""").format(SONG_DATA, ARN)

# FINAL TABLES

user_table_insert = ("""
    INSERT INTO users (user_id,first_name,last_name,gender,level)
    SELECT 
        se1.userId, 
        se1.firstName, 
        se1.lastName, 
        se1.gender, 
        se1.level 
    FROM stage_events se1 
    WHERE se1.userId != '' AND ts = (
        SELECT MAX(ts) 
        FROM stage_events se2 
        WHERE se1.userId = se2.userId
    )
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT 
        DISTINCT(song_id),
        title,
        artist_id,
        year,
        duration
    FROM stage_songs
    WHERE song_id IS NOT NULL AND song_id <> ''
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT
        DISTINCT(artist_id),
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM stage_songs
    WHERE artist_id IS NOT NULL AND artist_id <> ''
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        DISTINCT(ts),
        EXTRACT(hour FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS hour,
        EXTRACT(day FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS day,
        EXTRACT(week FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS week,
        EXTRACT(month FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS month,
        EXTRACT(year FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS year,
        EXTRACT(weekday FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS weekday
    FROM stage_events
    WHERE ts IS NOT NULL
""")

songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT
        se.itemInSession,
        se.ts,
        se.userId,
        se.level,
        artists.artist_id,
        songs.song_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM stage_events se
    LEFT JOIN songs ON se.song = songs.title
    LEFT JOIN artists ON se.artist = artists.name
    WHERE se.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
# insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
