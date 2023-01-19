
# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS dist.staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS dist.staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS dist.songplay;"
user_table_drop = "DROP TABLE IF EXISTS dist.user;"
song_table_drop = "DROP TABLE IF EXISTS dist.song;"
artist_table_drop = "DROP TABLE IF EXISTS dist.artist;"
time_table_drop = "DROP TABLE IF EXISTS dist.time;"

# CREATE SCHEMA
create_dist_schema = ("""
    CREATE SCHEMA IF NOT EXISTS dist;
""")
set_schema = ("""
    SET search_path to dist;
""")

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE dist.staging_events(
        artist                  VARCHAR(200) NULL,
        auth                    VARCHAR(25) NOT NULL,
        firstName               VARCHAR(25) NULL,
        gender                  VARCHAR(1) NULL,
        itemInSession           INTEGER NOT NULL,
        lastName                VARCHAR(25) NULL,
        length                  real NULL,
        level                   VARCHAR(10) NOT NULL,
        location                VARCHAR(50) NULL,
        method                  VARCHAR(10) NOT NULL,
        page                    VARCHAR(25) NOT NULL,
        registration            BIGINT NULL,
        sessionId               INTEGER NOT NULL,
        song                    VARCHAR(200) NULL,
        status                  SMALLINT NOT NULL,
        ts                      BIGINT NOT NULL,
        userAgent               VARCHAR(200) NULL,
        userId                  INTEGER NULL
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE dist.staging_songs(
        num_songs               INTEGER NOT NULL,
        artist_id               VARCHAR(50) NOT NULL,
        artist_latitude         real,
        artist_longitude        real,
        artist_location         VARCHAR(500) NOT NULL,
        artist_name             VARCHAR(500) NOT NULL,
        song_id                 VARCHAR(50) NOT NULL,
        title                   VARCHAR(200) NOT NULL,
        duration                real NOT NULL,
        year                    INTEGER NOT NULL
    )
""")

songplay_table_create = ("""
    CREATE TABLE dist.songplay(
        songplayid      INT GENERATED ALWAYS AS IDENTITY NOT NULL,
        start_time      DATE NOT NULL   SORTKEY,
        user_id         INTEGER NOT NULL,
        level           VARCHAR(10) NOT NULL,
        song_id         VARCHAR(50) NOT NULL    DISTKEY,
        artist_id       VARCHAR(50) NOT NULL,
        session_id      INTEGER NOT NULL,
        location        VARCHAR(50) NULL,
        user_agent      VARCHAR(200) NULL
    );
""")

user_table_create = ("""
    CREATE TABLE dist.user(
        user_id         INTEGER NULL,
        first_name      VARCHAR(25) NULL,
        last_name       VARCHAR(25) NULL,
        gender          VARCHAR(1) NULL,
        level           VARCHAR(10) NOT NULL
    )diststyle all;
""")

song_table_create = ("""
    CREATE TABLE dist.song(
        song_id         VARCHAR(50) NOT NULL,
        title           VARCHAR(200) NOT NULL,
        artist_id       VARCHAR(50) NOT NULL,
        year            INTEGER NOT NULL,
        duration        real NOT NULL
    )diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE dist.artist(
        artist_id       VARCHAR(50) NOT NULL,
        name            VARCHAR(500) NULL,
        location        VARCHAR(500) NULL,
        latitude       real,
        longitude       real
    )diststyle all;
""")

time_table_create = ("""
    CREATE TABLE dist.time(
        start_time      DATE NOT NULL,
        hour            INTEGER NOT NULL,
        day             INTEGER NOT NULL,
        week            INTEGER NOT NULL,
        month           INTEGER NOT NULL,
        year            INTEGER NOT NULL,
        weekday         INTEGER NOT NULL
    )diststyle all;
""")

# STAGING TABLES

staging_events_copy = """
    COPY dist.staging_events 
    FROM {log_data} 
    iam_role '{role_arn}'
    json  {log_jsonpath}
"""

staging_songs_copy = ("""
    COPY dist.staging_songs
    FROM {song_data}
    iam_role '{role_arn}'
    json 'auto'
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO dist.songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DATETIME(ts), userId, level, song_id, artist_id, sessionId, location, userAgent
      FROM dist.staging_events e
      JOIN dist.staging_songs s
        ON s.artist_name = e.artist
           AND s.title = e.song
     WHERE page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO dist.user(user_id, first_name, last_name, gender, level)
    SELECT userId, firstName, lastName, gender, level
      FROM dist.staging_events
""")

song_table_insert = ("""
    INSERT INTO dist.song(song_id, title, artist_id, year, duration)
    SELECT st_e.song_id, st_s.title, st_s.artist_id, st_s.year, st_s.duration
      FROM dist.staging_events st_e
      JOIN dist.staging_songs st_s
        ON st_s.artist_name = st_e.artist
           AND st_s.title = st_e.song
""")

artist_table_insert = ("""
    INSERT INTO dist.artist(artist_id, name, location, lattitude, longitude)
    SELECT s.artist_id, e.artist, e.location, e.latitude, e.longitude
      FROM dist.staging_events e
      JOIN dist.staging_songs s
        ON e.artist = s.artist_name
           AND e.song = s.title
""")

time_table_insert = ("""
    INSERT INTO dist.time(start_time, hour, day, week, month, year, weekday)
    SELECT ts::datetime, date_part(hour, ts::datetime), date_part(day, ts::datetime), 
           date_part(week, ts::datetime), date_part(month, ts::datetime), 
           date_part(year, ts::datetime), date_part(weekday, ts::datetime)
      FROM dist.staging_events
""")

# QUERY LISTS
create_schema_query = [create_dist_schema, set_schema]
create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
