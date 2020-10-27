# Dev Overview

An ETL step using **Apache Spark** to build data lake. This process is deployed on a EMR AWS cluster.
It intends to make big data hosted on AWS S3 available to BI applications.
As previously mentioned, the deployment of it becomes a task of almost DataOps

# Business case

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# The Paradigm

Current processes live and follow an ETL approach. For this one I'm doing and ELT (extract load and transform) approach. There is some noticable difference between these two. One is we are doing more with the same setup. Another is one is 💰. Last one is better decision taking.

With the ELT approach, I'm loading the data sources and doing the data transformation to perform the analysis on, within my cluster. Where as before this was done before submitting it to the cluster and then save to an external source; parquet files.

# D-POV
 From a developer point of view or data Analyst point of view the setup is much simpler. for example:
 
 ```python
 new_log_data_frame = spark.sql("""
    SELECT *
    FROM log_data
    WHERE page = 'NextSong'
""")
```

It's the same query that a data analyst would do, but there is not a whole lot of setup needed and there is more quantity of data available.