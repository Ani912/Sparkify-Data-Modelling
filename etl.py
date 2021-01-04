import os
import glob
import time
import logging
import psycopg2
import pandas as pd
from sql_queries import *


def bulk_commit(cur, conn, df, table_insert, enable_list=False):
    """
    Function to perform bulk write ops to postgres
    """
    for i, row in df.iterrows():
        if not enable_list:
            cur.execute(table_insert, row)
            conn.commit()
        else:
            cur.execute(table_insert, list(row))
            conn.commit()


def process_song_file(cur, conn, filelist):
    """
    Populate songs and artist data to respective dimension tables
    """
    # open song files and populate df
    songs = []
    for file in filelist:
        songs.append(pd.read_json(path_or_buf=file, lines=True))

    df_songs = pd.concat(songs, ignore_index=True)

    # insert song record
    song_data = df_songs[['song_id', 'title', 'artist_id', 'year', 'duration']]
    bulk_commit(cur, conn, song_data, song_table_insert)
    print("Successfully written data to songs_d table")

    # insert artist record
    artist_data = df_songs[['artist_id', 'artist_name', 'artist_location', \
                    'artist_latitude', 'artist_longitude']]
    bulk_commit(cur, conn, artist_data, artist_table_insert)
    print("Successfully written data to artist_d table")


def process_log_file(cur, conn, filelist):
    """
    - Populate users and timestamp data to respective dimension tables
    - Populate all relevant facts to fact tables
    """
    # open log file
    logs = []
    for file in filelist:
        logs.append(pd.read_json(path_or_buf=file, lines=True))

    log_df = pd.concat(logs, ignore_index=True)

    # filter by NextSong action
    df = log_df.loc[log_df['page'] == 'NextSong'].astype({'ts': 'datetime64[ms]'})

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    # hour, day, week of year, month, year, and weekday
    time_data = [t.dt.to_pydatetime(), t.dt.hour.values.tolist(),
                 t.dt.day.values.tolist(), t.dt.weekofyear.values.tolist(),
                 t.dt.month.values.tolist(), t.dt.year.values.tolist(),
                 t.dt.weekday.values.tolist()]
    column_labels = ['start_time', 'hour', 'day',
                    'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data).T
    time_df.columns = column_labels

    bulk_commit(cur, conn, time_df, time_table_insert, enable_list=True)
    print("Successfully written data to timestamp_d table")

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    bulk_commit(cur, conn, user_df, user_table_insert)
    print("Successfully written data to users_d table")

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, \
                        row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()
    print("Successfully written data to songplay_f table")


def get_files(filepath):
    """
    Function returns list of all the files on the given path
    """
    all_files = []
    for root, dirs, files in os.walk(f"./data/{filepath}"):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    return all_files


def process_data(cur, conn, filepath, func):
    """
    - Reads json files and populates it into df
    - Writes entire df to postgres implicitly
    """
    # get all files matching extension from directory
    all_files = get_files(filepath)

    # get total number of files found
    num_files = len(all_files)
    logging.info('{} files found in {}'.format(num_files, filepath))

    # bulk process over all files and bulk insert
    func(cur, conn, all_files)
    logging.info('Total {} files processed from {}.....'.format(num_files, filepath))


def main():
    # setting up logging to info
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
                                    user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='song_data', func=process_song_file)
    process_data(cur, conn, filepath='log_data', func=process_log_file)

    print("....All tables populated with data....")

    cur.execute(count_check)
    assert cur.fetchone()[0] == 1, 'Duplicate songid or artistid found in the table'
    print("!!!DATA CHECK COMPLETE!!!")

    conn.close()


if __name__ == "__main__":
    main()
