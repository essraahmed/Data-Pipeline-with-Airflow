from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    """
   Custom Operator Description:
       Load data from Staging tables to dimension tables 
    """
    
    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table= "",
                 sql_stmt = "",
                 truncate = True,
                 *args, **kwargs):

        """
    Args:
       redshift_conn_id: redshift connection id
       table: target table, dimension tables in redshift.
       sql_stmt: SQL command to load dimension tables from staging table 
       truncate: truncate the table before loading
    """
        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.truncate = truncate
       
    
    def execute(self, context):
        
        #redshift connection
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        #truncate table
        if self.truncate:
            self.log.info("Clearing data from {} table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
            
        #load table    
        self.log.info("Load data in dim table{}".format(self.table))
        redshift.run(f"INSERT INTO {self.table} {self.sql_stmt}")
        
