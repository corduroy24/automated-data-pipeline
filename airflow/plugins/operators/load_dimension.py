from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='',
                 target_table='',
                 sql='',
                 truncate_table = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql = sql
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info(f"Load data into {self.target_table} dimension table")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info("Truncating dimension table")
            redshift.run(f"TRUNCATE TABLE {self.target_table}")
        sql_insert = f"INSERT INTO {self.target_table} {self.sql}"
        redshift.run(sql_insert)