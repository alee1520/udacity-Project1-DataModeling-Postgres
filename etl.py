import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """
    Process Song file function 
    Opens the json song data and load the data into 2 dimension tables per category 
    songs and artist
    """
    # open song file       
    df = pd.read_json(filepath,typ='series')  

    # insert song record              
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist() 
    cur.execute(song_table_insert, song_data)            
            
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    
    """
    Process Log file function 
    Opens the json log file and load the users activties 
    The data is loaded into 2 dimensions tables and 1 fact table
    time and users dimension tables and songplay_data fact table
    """    
    
    # open song file
    df = pd.read_json(filepath,lines=True)  

    # filter by NextSong action
    df = df[df['page'] =='NextSong'] 

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')  
    
    #Create a dataframe with the converted timestamp
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('ts', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))
    
    # insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame(df[['userId', 'firstName', 'lastName', 'gender', 'level']].values.tolist()) 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        #convert milliseconds to a readable timestamp format before loading to songplay table   
        start_time = pd.to_datetime(row.ts, unit='ms')
        
        #Build the tuple to load rows into the songplay_data table
        songplay_data = start_time, row.userId, songid, artistid, row.sessionId, row.location, row.userAgent, row.level
        
         # insert songplay record where songid is populated
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    
    """
    Process Data Function
    Captures a list of json file from the directoy
    and pass the name file(s) and call supporting functions
    to load data to the Sparkify Database
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    #Establish a connection to the database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()