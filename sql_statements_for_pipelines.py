copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        
    """

staging_events_table_create2 = """CREATE TABLE events_source2
        ( artist varchar(200),
        auth varchar(50),
        firstName varchar(100),
        gender varchar(20),
        itemInSession bigint,
        lastname varchar(200),
        length numeric(10,4),
        level varchar(10),
        location varchar(300),
        method varchar(10),
        page varchar(20),
        registration bigint,
        sessionId bigint,
        song varchar(200),
        status bigint,
        ts timestamp,
        userAgent varchar(200),
        userId bigint)
        """




class SqlQueries:

    staging_events_table_create = ("""CREATE TABLE events_source
        ( artist varchar(200),
        auth varchar(50),
        firstName varchar(100),
        gender varchar(20),
        itemInSession bigint,
        lastname varchar(200),
        length numeric(10,4),
        level varchar(10),
        location varchar(300),
        method varchar(10),
        page varchar(20),
        registration bigint,
        sessionId bigint,
        song varchar(200),
        status bigint,
        ts timestamp,
        userAgent varchar(200),
        userId bigint)
        """) 
    
    staging_events_table_create3 = ("""CREATE TABLE events_source3
        ( artist varchar(200),
        auth varchar(50),
        firstName varchar(100),
        gender varchar(20),
        itemInSession bigint,
        lastname varchar(200),
        length numeric(10,4),
        level varchar(10),
        location varchar(300),
        method varchar(10),
        page varchar(20),
        registration bigint,
        sessionId bigint,
        song varchar(200),
        status bigint,
        ts bigint,
        userAgent varchar(200),
        userId bigint)
        """) 


    staging_songs_table_create = ("""CREATE TABLE songs_source
        ( num_songs bigint,
        artist_id varchar(300),
        artist_latitude numeric(9,6),
        artist_longitude numeric(9,6),
        artist_location varchar(300),
        artist_name varchar(300),
        song_id varchar(300),
        title varchar(300),
        duration numeric(10,6),
        year bigint)
        """)


    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
