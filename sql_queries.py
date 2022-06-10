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

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(   artist          VARCHAR(300),
    auth            VARCHAR(25),
    first_name      VARCHAR(25),
    gender          VARCHAR(1),
    item_in_session INTEGER, 
    last_name       VARCHAR(25),
    legnth          DECIMAL(9, 5),
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
);""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(   num_songs        INTEGER,
    artist_id        VARCHAR(25), 
    artist_latitude  DECIMAL(10, 5),
    artist_longitude DECIMAL(10, 5),
    artist_location  VARCHAR(300),
    artist_name      VARCHAR(300),
    song_id          VARCHAR(25),
    title            VARCHAR(300),
    duration         DECIMAL(9, 5),
    year             INTEGER
);""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay 
(   songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL, 
    user_id     VARCHAR(10),
    level       VARCHAR(10),
    song_id     VARCHAR(300) NOT NULL,
    artist_id   VARCHAR(25) NOT NULL,
    session_id  INTEGER,
    location    VARCHAR(300),
    user_agent  VARCHAR(150)
);""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user 
(   user_id    VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(25),
    last_name  VARCHAR(25),
    gender     VARCHAR(1),
    level      VARCHAR(10)
);""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song 
(   song_id   VARCHAR(25) PRIMARY KEY,
    title     VARCHAR(300) NOT NULL,
    artist_id VARCHAR(25),
    year      INTEGER,
    duration  DECIMAL(9, 5) NOT NULL
);""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist 
(   artist_id VARCHAR(25) PRIMARY KEY,
    name      VARCHAR(300) NOT NULL,
    location  VARCHAR(300),
    lattitude DECIMAL(10, 5),
    longitude DECIMAL(10, 5)
);""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(   start_time TIMESTAMP PRIMARY KEY,
    hour       INTEGER,
    day        INTEGER,
    week       INTEGER,
    month      INTEGER,
    year       INTEGER,
    weekday    INTEGER
);""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON '{}';
    """).format(config['S3']['log_data'], config['IAM_ROLE']['arn'], config['S3']['log_jsonpath'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
    """).format(config['S3']['song_data'], config['IAM_ROLE']['arn'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay 
    (songplay_id, start_time, user_id, level, song_id,
     artist_id, session_id, location, user_agent)
SELECT DISTINCT 
    (songplay_id, start_time, user_id, level, song_id,
     artist_id, session_id, location, user_agent)
FROM 
    staging_events
WHERE
    songplay_id IS NOT NULL;
    
""")

user_table_insert = ("""
INSERT INTO user
    (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    (user_id, first_name, last_name, gender, level)
FROM
    staging_events
WHERE
    user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO song
    (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    (song_id, title, artist_id, year, duration)
FROM 
    staging_songs
WHERE
    song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artist
    (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT
    (artist_id, name, location, lattitude, longitude)
FROM
    staging_songs
WHERE 
    artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    (start_time, hour, day, week, month, year, weekday)
FROM
    staging_events
WHERE
    start_time IS NOT NULL;
""")

# TEST ANALYSIS QUERIE

top_ten_songs =("""
    SELECT 
        s.title as top_song, 
        a.name as artist, 
        COUNT(sp.songplay_id) AS play_times
    FROM 
        songplays AS sp
    JOIN 
        songs AS s 
    ON 
        sp.song_id = s.song_id
    JOIN 
        artists As a 
    ON 
        sp.artist_id = a.artist_id
    GROUP BY 
        s.title, a.name
    ORDER BY 
        play_times DESC
    LIMIT 10;
""")

top_ten_artists =("""
     SELECT 
         a.name as top_artists, 
         COUNT(sp.songplay_id) AS no_play_times
    FROM 
        songplays AS sp
    JOIN 
        artists As a 
    ON 
        sp.artist_id = a.artist_id
    GROUP BY 
        a.name
    ORDER BY 
        no_play_times DESC
    LIMIT 10;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analysis_queries =[top_ten_songs,top_ten_artists]
