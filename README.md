Sparkify DB readme.

The purpose of the Sparkify Database is to gather and maintain activity on the sparkify app from the active users and provide analytics to gauge the use of the app.

The database scheme was built to support 3 subject matters

1. Users
2. Songs
3. Artists

To load the data into the Sparkify DB you must do the following:

Open jupyter notebook
Select python version

Load file test.ipynb

	1. Run command %load_ext sql to execute sql statements
	2. Run command %run create_tables.py to create SparkifyDB and create the following tables

		1. Users
		2. Songs
		3. Artists
		4. Time
		5. SongPlays
		
		NOTE:  create_tables.py uses the supporting script sql_queries.py which contains create/drop table statements.  Everytime the script is execute,
			   the database is dropped and created along with the tables.
	
	3. Run command %run etl.py to execute the load process.

		1. The process will get the lists of files in the directory with an .json extention.
		2. Read the json file(s)
		3. Extract the data
		4. Load into the tables
			
			The load table process is supported by sql_queries.py which contains the insert statments need to load data

Below are a few queries to help provide insight 

Shows user, location and times songs have been accessed

%sql select u.user_id, first_name, last_name, start_time, location, user_agent  from users u inner join songplays sd on u.user_id = sd.user_id 

![image](https://github.com/alee1520/hello-world/blob/master/user_activity.png)

Total of distinct users on the app

%sql select count(distinct user_id) from songplays

![image](https://github.com/alee1520/hello-world/blob/master/count.png?raw=true)

Total count of songs by name played in the app
% sql select count(sp.*), name from songplays sp inner join artists a on a.artist_id = sp.artist_id inner join songs s on s.song_id = sp.song_id group by name

![image](https://github.com/alee1520/hello-world/blob/master/total_songs_activity.png)
