# **TODOs**

1) Identify storage location for analyzed datasets
  - S3, not sure where exactly
2) Identify what resource will intake raw data for processing
  - Spark will be used for parallel processing
  - Spark deployed on EMR cluster (can this be programmatic?)
3) Steps for etl
  songs:
    - read in data from each song files
    - extract columns from song files
    - write new table data to parquet files partitioned by year and artists
    - extract columns to create artist table
    - -write artists table to parquet files
  logs:
    - read in data from each log data files
    - filter by actions for songplays
    - extract columns for users table
    - 

Skills to use
- accessing AWS resources
- spark functions for filtration and transformation
- how to write to parkay files
