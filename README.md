### Data Lake Project Overview
The purpose of this project is to create a data lake to store the song data and user log for the startup Sparkify. The reason to use data lake approach is because they have accumulated huge amount of data. Therefore, we utilize Spark and S3 to achieve distributed computing. Historically, these users and songs data are reside in JSON format in a directory, which is not easy to query, summarize, and perform analytical computation. Therefore, our approach here is to build an ETL pipeline to pull these JSON files and convert these into a set of dimentional tables stored in parquet format which works well with Spark. As such, we can achieve schema-on-read to quickly load large amount of data into Spark for querying and analysis, which serve as a star schema database, and users can easily explore the data to understand their users better and more efficiently.  


### Database design
We purposely build our data lake with **Star Schema** to optimize the query analysis since we know the objective of this database is to query for specific user activity log for analytical computation, and are likely to have multiple people access it paired with large amount of existing and new log activity pull in, so the simplicity of star schema is most suitable for this purpose. We have 2 type of original datasets, firstly, the song dataset containing song and artist metadata such as title are artist name, and secondly, the log dataset containing user activities including the timestamp they start to listen, the song they listen to, their gender and location etc. We split our dataset and build 5 tables to be our fact table and dimension tables as follows:

1. Fact table: 
    - songplays
2. Dimension table:
    - users
    - songs
    - artists
    - time

### ETL pipeline
The ETL code is in file **etl.py**.  we set up our Spark cluster, and locate the location of the songs and users log files in JSON format, and process them to save them into various fact and dimension tables stored in parquet format on S3 as illustrated in previous section.  

### Files in this repository
1. etl.py: This contains the automated code for ETL to pull all JSON files from 2 directories, and process them into 5 data sets and save them in specified S3 buckets with parquet format.
2. dl.cfg: it contains the access key and secret key to the AWS account to access the S3 bucket (The actual value are removed in this file to keep confidential from public access.)


### Example queries and analysis

1. Top 5 most count of songplay by locations
```sql
SELECT location, count(*) 
FROM songplays 
GROUP BY location 
ORDER BY count(*) 
DESC LIMIT 5;
```

| location | count |
| ---------| -------|
|San Francisco-Oakland-Hayward, CA | 168 |
|Lansing-East Lansing, MI | 128 |
|Portland-South Portland, ME | 124 |
|Waterloo-Cedar Falls, IA | 84 |
|Tampa-St. Petersburg-Clearwater, FL | 72 |



2. Count of songplay by gender and by level
```sql
SELECT songplays.level AS user_type, 
    COUNT(CASE WHEN users.gender = 'F' THEN 1 ELSE NULL END) AS Female_count, 
    COUNT(CASE WHEN users.gender = 'M' THEN 1 ELSE NULL END) AS Male_count 
    FROM songplays 
    JOIN users ON songplays.user_id = users.user_id 
    GROUP BY songplays.level;
```

|user_type | female_count | male_count |
| ---------| -------------| ----------|
| free | 1240 | 400 |
| paid | 136 | 132 |



3. Count of songplay by weekday
```sql
SELECT time.weekday, 
    COUNT(songplays.songplay_id) AS number_of_play 
    FROM songplays 
    JOIN time ON songplays.start_time = time.start_time 
    GROUP BY time.weekday 
    ORDER BY weekday;
```

|weekday | number_of_play |
| ---------| -------------|
| 0 | 156 |
| 1 | 220 |
| 2 | 244 |
| 3 | 168 |
| 4 | 180 |
| 5 | 160  |
| 6 | 204  |


