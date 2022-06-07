# **SUMMARY**
The purpose of this project is to leverage Apache Spark in building an ETL pipeline for a data lake hosted on AWS S3. The Spark process will be deployed on an AWS EMR cluster (Elastic map reduce). 
The data for the analytics tables will be loaded back into S3 after being processed by Spark.
with spark in distributed mode
- EMR service is a scalable set of EC2 machines that are already configured to run spark (handled dependency of needing spark on each machine)
- who is the cluster manager?
- who are the nodes?
- EMR cluster settings:
    - release = emr-5.20.0 or later
    - applications = Spark: 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0
    - instance type = m3.xlarge
    - number of instance = 3
    - EC2 key pair: Proceed without an EC2 key pair or feel free to use your own
- EMR notebook config
    - Notebook name = Sparkify
    - Choose existing cluster => the one you just created
    - default setting for "AWS service role" => "EMR_Notebooks_DefaultRole" or "Create default role" if you haven't done it before
    - keep the rest of the settings
- i will use my own s3 bucket for storing parkay files. access to this bucket will be blocked by users that aren't under my account. this means that the person testing this code is responsible for
setting a new location on an s3 bucket that they have permissions for instead of mine

# **INSTRUCTIONS**
Launch EMR Cluster
Create Notebook in EMR Console
Run etl.py:
    -"python etl.py --input_path [location of s3 bucket containing song and log data] --output_path [where to write parquet files] --debug_mode [True if user wishes to run etl with reduced song dataset]"
NOTE: i had to write my files to my personal aws bucket since i ran out of credits in federated udacity account

# **MANIFEST**
etl.py
dl.cfg
README.md (this file)