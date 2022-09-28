# Data-Pipline-Apache-Airflow

## Overview
This project is about building an Airflow ETL Pipeline for Sparkify Company. The company wants to automate and monitor their data warehousing ETL on AWS.
The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to. Also, wants Data Quality tests run against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

## Prerequisites:
1- Create an IAM User in AWS. </br>
Attach Policies: `AdministratorAccess`, `AmazonRedshiftFullAccess` and `AmazonS3FullAccess`

2- Create a redshift cluster.

## Airflow Connection:
1- Connect Airflow and AWS (AWS Credentials). </br>
Run `/opt/airflow/start.sh`, Click on the Admin tab and select Connections. </br>
Then create Amazon Web Services conn you will Enter `Access Key` in login and `Secret key` in password from the IAM User credentials.

2- Connect Airflow to the AWS Redshift Cluster. </br>
Create Postgres Conn with credentials to Redshift

## Project Dataset
There are two datasets that reside in S3:

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

## Database Schema Design
[schema](imgs/schema.png)
        
## Project Template
Project files<br>

1. `dl.cfg`: Contains AWS credentials.
2. `etl.py`: Reads data from S3, processes that data using Spark, and writes them back to S3.
3. `README.md`: Provides discussion on the project.

## ETL pipeline
- Extract data from AWS S3, `Song data` and `Log data`.
- Transform to create dimenstional and fact tables using Apache Spark.
- Load them back to AWS S3 Data Lake partitioned parquet files. <br>
 We used Parquet format because: Low storage consumption and higher execution speed.


## Confguration
To get AWS Credentials:
1. Create IAM User with `AmazonS3FullAccess` Policy.
2. Then you will get the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

## How to run the Python Scripts
  
Run `etl.py`.

  ``` python etl.py```

## Author
Esraa Ahmed | <a href="https://linkedin.com/in/esraa-ahmed-ibrahim2" target="blank"><img align="center" src="https://raw.githubusercontent.com/rahuldkjain/github-profile-readme-generator/master/src/images/icons/Social/linked-in-alt.svg" alt="esraa-ahmed-ibrahim2" height="15" width="15" /></a>

Created on 10/09/2022
