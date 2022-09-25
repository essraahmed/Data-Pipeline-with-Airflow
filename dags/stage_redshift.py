from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import DAG

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
       
    #create a task    
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION {}
            FORMAT AS JSON '{}' 
        """
    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id ="",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 region = "",
                 format_type = "",
                 *args, **kwargs):
        


        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region    
        self.format_type = format_type
        
        
    def execute(self, context):
        AWS_hook = AwsHook(self.aws_credentials_id)
        credentials = AWS_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f"Clearing data from {self.table} Redshift table")
        redshift.run(f"Delete FROM {self.table} ")
        
        self.log.info("Copying Data From S3 To Redshift")
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.format_type
        )
        redshift.run(formatted_sql)





