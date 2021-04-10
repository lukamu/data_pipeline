from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    This Operator loads json formatted files from S3 into Redshift.
    """

    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql_query = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
    """

    @apply_defaults
    def __init__(self, table, drop_table,
                 aws_connection_id, redshift_connection_id,
                 s3_bucket, s3_key, copy_options,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.drop_table = drop_table
        self.aws_connection_id = aws_connection_id
        self.redshift_connection_id = redshift_connection_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_options = copy_options

    def execute(self, context):
        self.log.info('StageToRedshiftOperator started.')
        self.hook = PostgresHook(postgres_conn_id=self.redshift_connection_id)
        self.aws_instance = AwsHook(aws_conn_id=self.aws_connection_id)
        credentials = self.aws_instance.get_credentials()
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_query = StageToRedshiftOperator.copy_sql_query.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.copy_options
        )
        if self.drop_table:
            self.log.info('{} table already exists. Start dropping it.'.format(
                self.table))
            self.hook.run("DROP TABLE IF EXISTS {}".format(self.table))
            self.log.info(
                "Table {} dropped successfully.".format(
                    self.table))
        
        self.log.info('Copying data running the copy_sql_query.')
        self.hook.run(formatted_query)
        self.log.info("StageToRedshiftOperator data copy successfully completed.")





