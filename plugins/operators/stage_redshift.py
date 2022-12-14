#Created on 28/09/2022 by esraa ahmed
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import DAG


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    """
   Custom Operator Description:
       Load data from S3 to Staging tables in Redshift
  
       Args:
       redshift_conn_id: redshift connection id
       aws_credentials_id: AWS connctions id
       table: target table, staging table in redshift.
       s3_bucket: S3 bucket source
       s3_key: the key name of the s3 bucket
       region: the region of redshift that database exists in
       json: the format of data
    """
   
    #create a task    
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            JSON '{}' 
        """    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id ="",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 region = "",
                 json = "auto",
                 *args, **kwargs):

        
       super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
       # Map params here
       self.redshift_conn_id = redshift_conn_id
       self.aws_credentials_id = aws_credentials_id
       self.table = table
       self.s3_bucket = s3_bucket
       self.s3_key = s3_key
       self.region = region    
       self.json = json
        
        
    def execute(self, context):
       
        #AWS and Redshift connections
        AWS_hook = AwsHook(self.aws_credentials_id)
        credentials = AWS_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        #delete rows
        self.log.info(f"Clearing data from {self.table} Redshift table")
        redshift.run(f"Delete FROM {self.table} ")
        
        #copy data from S3 to Redshift
        self.log.info("Copying Data From S3 To Redshift")
        
        #get rendered key 
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(s3_path)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table, 
                s3_path, 
                credentials.access_key,
                credentials.secret_key, 
                self.region,
                self.json
            )
        
        self.log.info(formatted_sql)
        #execute formatted_sql
        redshift.run(formatted_sql)
#Created on 28/09/2022 by esraa ahmed
