# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay_f;"
user_table_drop = "DROP TABLE IF EXISTS users_d;"
song_table_drop = "DROP TABLE IF EXISTS songs_d;"
artist_table_drop = "DROP TABLE IF EXISTS artists_d;"
time_table_drop = "DROP TABLE IF EXISTS timestamp_d;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS
                                songplay_f (songplay_id SERIAL PRIMARY KEY,
                                            start_time BIGINT,
                                            user_id INT,
                                            level VARCHAR,
                                            song_id VARCHAR,
                                            artist_id VARCHAR,
                                            session_id INT,
                                            location VARCHAR,
                                            user_agent VARCHAR);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS
                                    users_d (user_id INT PRIMARY KEY,
                                            first_name VARCHAR,
                                            last_name VARCHAR,
                                            gender VARCHAR,
                                            level VARCHAR);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS
                                    songs_d (song_id VARCHAR PRIMARY KEY,
                                            title VARCHAR,
                                            artist_id VARCHAR,
                                            year INT,
                                            duration FLOAT);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS
                                  artists_d (artist_id VARCHAR PRIMARY KEY,
                                            artist_name VARCHAR,
                                            location VARCHAR,
                                            latitude FLOAT,
                                            longitude FLOAT);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS
                                timestamp_d (start_time BIGINT PRIMARY KEY,
                                            hour INT,
                                            day INT,
                                            week INT,
                                            month INT,
                                            year INT,
                                            weekday INT);""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplay_f (songplay_id, start_time, user_id, level, song_id,
                           artist_id, session_id, location, user_agent)
    VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
    INSERT INTO users_d (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

song_table_insert = ("""
    INSERT INTO songs_d (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists_d (artist_id, artist_name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

time_table_insert = ("""
    INSERT INTO timestamp_d (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT
        s.song_id,
        a.artist_id
    FROM
        songs_d s join artists_d a
        ON s.artist_id = a.artist_id
    WHERE
        s.title = %s
        AND a.artist_name = %s
        AND s.duration = %s
""")

# QUALITY CHECKS

count_check = ("""
    SELECT
        count(*)
    FROM
        songplay_f
    where
        song_id is NOT NULL
        and artist_id is NOT NULL;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]