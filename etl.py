import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Get song data from a JSON `filepath` file and insert the data into
    song and artist tables.

    Parameters:
        cur: postgres cursor
            cursor obtained from active session to execute PostgreSQL commands.
        filepath: filepath
            path to the songs file.
    '''
    df = pd.read_json(filepath,lines=True)

    song_data = (df[['song_id','title','artist_id','year','duration']].values[0]).tolist()
    cur.execute(song_table_insert, song_data)
    
    artist_data = (df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0]).tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Get logs data from a JSON `filepath` file, then filter the logs to only
    show useractivity that has level=='NextSong',then insert the data into
    users,time and songplay tables.
    
    Parameters:
        cur: postgres cursor
            cursor obtained from active session to execute PostgreSQL commands.
        filepath: filepath
            path to the songs file.
    '''
    df = pd.read_json(filepath,lines=True)

    df = df[df['page'] == 'NextSong']

    time = pd.to_datetime( df['ts'] ,unit='ms')
    
    time_data = [time,time.dt.hour,time.dt.day,time.dt.week,time.dt.month,time.dt.year,time.dt.dayofweek]
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = (df[['userId', 'firstName', 'lastName', 'gender', 'level']])

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = ([pd.to_datetime(row.ts ,unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Process data, get all files matching extension from directory
    then get total number of files found, and finaly iterate over files and process.
    
    Parameters:
        cur: postgres cursor
            cursor obtained from active session to execute PostgreSQL commands.
        filepath: filepath
            path to the songs file.
        conn: postgres connection
            postgres connection to database using psycopg2.connect
        func: the func that been used
            either process_song_file or process_log_file
    '''
    
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    Sparkify main func, create ETL Pipeline
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()