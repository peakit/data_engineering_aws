import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
DWH_IAM_ROLE = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events(
        artist                  VARCHAR(50) NOT NULL,
        auth                    VARCHAR(25) NOT NULL,
        firstName               VARCHAR(25) NOT NULL,
        gender                  VARCHAR(1) NOT NULL,
        itemInSession           INTEGER NOT NULL,
        lastName                VARCHAR(25) NOT NULL,
        length                  real NULL,
        level                   VARCHAR(10) NOT NULL,
        location                VARCHAR(50) NOT NULL,
        method                  VARCHAR(10) NOT NULL,
        page                    VARCHAR(25) NOT NULL,
        registration            INTEGER NOT NULL,
        sessionId               INTEGER NOT NULL,
        song                    VARCHAR(50) NOT NULL,
        status                  SMALLINT NOT NULL,
        ts                      INTEGER NOT NULL,
        userAgent               VARCHAR(50) NOT NULL,
        userId                  INTEGER NOT NULL
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs               INTEGER NOT NULL,
        artist_id               VARCHAR(50) NOT NULL,
        artist_latitude         real,
        artist_longitude        real,
        artist_location         VARCHAR(25) NOT NULL,
        artist_name             VARCHAR(25) NOT NULL,
        song_id                 VARCHAR(50) NOT NULL,
        title                   VARCHAR(50) NOT NULL,
        duration                real NOT NULL,
        year                    INTEGER NOT NULL
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplay(
        songplayid      IDENTITY(0,1) NOT NULL,
        start_time      DATE NOT NULL   SORTKEY,
        user_id         INTEGER NOT NULL,
        level           VARCHAR(10) NOT NULL,
        song_id         VARCHAR(50) NOT NULL    DISTKEY,
        artist_id       VARCHAR(50) NOT NULL,
        session_id      INTEGER NOT NULL,
        location        VARCHAR(50) NOT NULL,
        user_agent      VARCHAR(50) NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id         INTEGER NOT NULL,
        first_name      VARCHAR(25) NOT NULL,
        last_name       VARCHAR(25) NOT NULL,
        gender          VARCHAR(1) NOT NULL,
        level           VARCHAR(10) NOT NULL
    )distkey all;
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id         VARCHAR(50) NOT NULL,
        title           VARCHAR(50) NOT NULL,
        artist_id       VARCHAR(50) NOT NULL,
        year            INTEGER NOT NULL,
        duration        real NOT NULL
    )distkey all;
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id       VARCHAR(50) NOT NULL,
        name            VARCHAR(25) NOT NULL,
        location        VARCHAR(50) NOT NULL,
        lattitude       real,
        longitude       real
    )distkey all;
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time      DATE NOT NULL,
        hour            INTEGER NOT NULL,
        day             INTEGER NOT NULL,
        week            INTEGER NOT NULL,
        month           INTEGER NOT NULL,
        year            INTEGER NOT NULL,
        weekday         INTEGER NOT NULL
    )distkey all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM '{log_data_loc}'
    iam_role '{aws_iam_role}'
    json  '{log_jsonpath_loc}'
""").format(log_data_loc=LOG_DATA,
            aws_iam_role=DWH_IAM_ROLE,
            log_jsonpath_loc=LOG_JSONPATH)

staging_songs_copy = ("""
    COPT staging_songs
    FROM '{songs_data_loc}'
    iam_role '{aws_iam_role}'
    json 'auto'
""").format(songs_data_loc=SONG_DATA,
            aws_iam_role=DWH_IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DATETIME(ts), userId, level, song_id, artist_id, sessionId, location, userAgent
      FROM staging_events e
      JOIN staging_songs s
        ON s.artist_name = e.artist
           AND s.title = e.song
     WHERE page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO user(user_id, first_name, last_name, gender, level)
    SELECT userId, firstName, lastName, gender, level
      FROM staging_events
""")

song_table_insert = ("""
    INSERT INTO song(song_id, title, artist_id, year, duration)
    SELECT st_e.song_id, st_s.title, st_s.artist_id, st_s.year, st_s.duration
      FROM staging_events st_e
      JOIN staging_songs st_s
        ON st_s.artist_name = st_e.artist
           AND st_s.title = st_e.song
""")

artist_table_insert = ("""
    INSERT INTO artist(artist_id, name, location, lattitude, longitude)
    SELECT s.artist_id, e.artist, e.location, e.latitude, e.longitude
      FROM staging_events e
      JOIN staging_songs s
        ON e.artist = s.artist_name
           AND e.song = s.title
""")

# TODO: Convert int timestamp to datetime
time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DATETIME(ts), EXTRACT('hour', DATETIME(ts)), EXTRACT('day', DATETIME(ts)), 
           EXTRACT('week', DATETIME(ts)), EXTRACT('month', DATETIME(ts)), 
           EXTRACT('year', DATETIME(ts)), EXTRACT('weekday', DATETIME(ts))
      FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
