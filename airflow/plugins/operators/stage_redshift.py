from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    load_to_s3_template = '''
        COPY {} 
        FROM {}
        ACCESS_KEY_ID {}
        SECRET_ACCESS_KEY
        JSON {}
    '''
    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 aws_credentials_id="",
                 target_table="",
                 json_path="",
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(**kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id= redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.target_table=target_table
        self.json_path=json_path

    def execute(self, context):
        self.log.info('Load data from s3 to redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        load_to_s3 = StageToRedshiftOperator.load_to_s3_template.format(
            self.target_table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            json_path=self.json_path,
        )
        redshift.run(load_to_s3)



