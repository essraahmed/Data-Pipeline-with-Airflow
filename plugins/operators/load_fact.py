from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    """
   Custom Operator Description:
       Load data from Staging tables to fact table 
    Args:
       redshift_conn_id: redshift connection id
       table: target table, fact table in redshift.
       fact_sql: SQL command to load fact table from staging table 
       append_only: if true will append data to the table 
    """
    
    ui_color = '#F98866'
    facts_sql_template = """
         DROP TABLE IF EXISTS {destination_table};
         CREATE TABLE {destination_table} AS
         SELECT
             {groupby_column},
             MAX({fact_column}) AS max_{fact_column},
             MIN({fact_column}) AS min_{fact_column},
             AVG({fact_column}) AS average_{fact_column}
        FROM {origin_table}
        GROUP BY {groupby_column};
        """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 fact_sql = "",
                 append_only = "",
                 *args, **kwargs):
               
        super(LoadFactOperator, self).__init__(*args, **kwargs)
         # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.fact_sql = fact_sql
        self.append_only = append_only

    def execute(self, context):
        #redshift connection
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
 
        #if true will append data to the table
        if not self.append_only:
            self.log.info(f"Delete {self.table} fact table")
            redshift.run(f"DELETE FROM {self.table}") 
            fact_sql = (f"INSERT INTO {self.table} {self.fact_sql}")
            redshift.run(fact_sql)
            
        self.log.info(f"Insert data into {self.table} fact table")

        if self.append_only:
            fact_sql = (f"INSERT INTO {self.table} {self.fact_sql}")
            redshift.run(fact_sql)
