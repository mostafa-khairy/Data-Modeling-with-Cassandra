# Data Modeling with Cassandra
# introduction
- A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

- They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.


# Data 
- event_data. The directory of CSV files partitioned by date
```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```
And below is an example of what the data in each file.

![image_event_datafile_new](https://user-images.githubusercontent.com/87584678/209083421-2e4b8f69-cd8d-4c37-a2b4-2788de473b8c.jpg)







