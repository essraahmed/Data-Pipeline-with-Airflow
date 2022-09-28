from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

#default args
default_args = {
    'owner': 'ess',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'email_on_retry': False,
    'catchup' : False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5)
} 

#dag scheduled hourly 
dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1
        )

#start operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#create tables
create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)

#staging tables in S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "public.staging_events",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",  
    region = "us-west-2",
    json ="s3://udacity-dend/log_json_path.json",
    provide_context = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "public.staging_songs",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    region = "us-west-2",
    json = "auto",
    provide_context = True
)

#load fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songplays",
    fact_sql = SqlQueries.songplay_table_insert,
    append_only = True
)

# load dimension tables: users, songs, artists and time
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "users",
    sql_stmt = SqlQueries.user_table_insert,
    Truncate = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songs",
    sql_stmt = SqlQueries.song_table_insert,
    Truncate = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "artists",
    sql_stmt = SqlQueries.artist_table_insert,
    Truncate = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "time",
    sql_stmt = SqlQueries.time_table_insert,
    Truncate = True
)

#check quality
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    provide_context = True,
    tables = ['songplays', 'users', 'songs', 'artists', 'time']
)

#end operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Task Dependencies
start_operator.set_downstream(create_tables)

create_tables.set_downstream(stage_events_to_redshift)
create_tables.set_downstream(stage_songs_to_redshift)

load_songplays_table.set_upstream(stage_events_to_redshift)
load_songplays_table.set_upstream(stage_songs_to_redshift)

load_songplays_table.set_downstream(load_user_dimension_table)
load_songplays_table.set_downstream(load_song_dimension_table)
load_songplays_table.set_downstream(load_artist_dimension_table)
load_songplays_table.set_downstream(load_time_dimension_table)

load_user_dimension_table.set_downstream(run_quality_checks)
load_song_dimension_table.set_downstream(run_quality_checks)
load_artist_dimension_table.set_downstream(run_quality_checks)
load_time_dimension_table.set_downstream(run_quality_checks)

run_quality_checks.set_downstream(end_operator)
