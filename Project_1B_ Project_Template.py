#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[1]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[4]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[5]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[6]:


try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[7]:


try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# # Query 1

# ### Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession = 4

# create music_history table that contain : 
# - artist  
# - song_title 
# - song_length
# - sessionId
# - itemInSession 
# 
# - primary key : 
#     - sessionId : partition key
#     - itemInSession : clustering column

# In[8]:


delete_session_details = "DROP TABLE IF EXISTS session_details"
session.execute(delete_session_details)


# In[9]:


query = "CREATE TABLE IF NOT EXISTS session_details (sessionId int,        itemInSession int,        artist text,        song_title text,        song_length float,        primary key(sessionId,itemInSession) )"
try:
    session.execute(query)
except Exception as e:
    print(e)


                    


# ### insert data into music_history table 

# In[10]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "insert into session_details (artist , song_title , song_length , sessionId , itemInSession)                 VALUES (%s, %s, %s, %s, %s) "
        session.execute(query, (line[0], line[9], float(line[5]), int(line[8]), int(line[3])))


# #### SELECT to verify that the data have been inserted into each table

# In[11]:


query = "select artist, song_title, song_length from session_details WHERE sessionId=338 and itemInSession = 4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist, row.song_title, row.song_length)    


# # Query 2

# ### Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

# create user_music table that contain : 
# - artist  
# - song_title 
# - sessionId
# - itemInSession
# - User_ID 
# - user_fname
# - user_lname
# 
# - primary key : 
#     - (User_ID, sessionId) : partition key
#     - itemInSession : clustering column

# In[12]:


delete_user_music = "DROP TABLE IF EXISTS user_music"
session.execute(delete_user_music)


# In[13]:


query = "CREATE TABLE IF NOT EXISTS user_music (User_ID int,        sessionId int,        itemInSession int,        artist text,        song_title text,        user_fname text,        user_lname text,        primary key((User_ID, sessionId) , itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)                   


# ### insert data into user_music table 

# In[14]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "insert into user_music (artist , song_title , sessionId , itemInSession, User_ID , user_fname, user_lname)                 VALUES (%s, %s, %s, %s, %s, %s, %s) "

        session.execute(query,(line[0], line[9], int(line[8]), int(line[3]), int(line[10]), line[1], line[4] ))


# #### SELECT to verify that the data have been inserted into each table

# In[15]:


query = "select artist, song_title, user_fname, user_lname from user_music WHERE User_ID = 10 and sessionId = 182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist, row.song_title, row.user_fname, row.user_lname)   


# # Query 3
# 

# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 

# create song table that contain : 
#    
# - song_title 
#  
#  
# - User_ID 
# - user_fname
# - user_lname
# 
# - primary key : 
#     - song_title : partition key
#     - User_ID : clustering column

# In[16]:


delete_song_listens  = "DROP TABLE IF EXISTS song_listens "
session.execute(delete_song_listens)                    


# In[17]:


query = "CREATE TABLE IF NOT EXISTS song_listens (song_title text,        User_ID int,        user_fname text,        user_lname text,        primary key(song_title , User_ID))"
try:
    session.execute(query)
except Exception as e:
    print(e)                   


# ### insert data into song table 

# In[18]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "insert into song_listens (song_title , User_ID , user_fname , user_lname )                 VALUES (%s, %s, %s, %s) "

        session.execute(query,(line[9], int(line[10]),  line[1] , line[4]))


# #### SELECT to verify that the data have been inserted into each table

# In[19]:


query = "select user_fname , user_lname from song_listens   WHERE song_title='All Hands Against His Own' "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.user_fname, row.user_lname)    


# ### Drop the tables before closing out the sessions

# In[20]:


## TO-DO: Drop the table before closing out the sessions

query = "DROP TABLE if exists session_details"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "DROP TABLE if exists user_music"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
query = "DROP TABLE if exists song_listens "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)    


# ### Close the session and cluster connection¶

# In[21]:


session.shutdown()
cluster.shutdown()


# In[ ]:




