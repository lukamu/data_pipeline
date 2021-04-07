from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This Operator checks the quality of ingested data.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 target_tables=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.target_tables = target_tables
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)

    def execute(self, context):
        self.log.info('DataQualityOperator started.')
        for table in self.target_tables:
            records = self.hook.get_records(
                "SELECT COUNT(*) FROM {}".format(table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    "DataQualityOperator error. \
                    {} returned no results".format(table))
            num_records = records[0][0]

            if num_records < 1:
                self.log.info(
                    "No records found in table {}.".format(table))
                raise ValueError(
                    "No records found in table {}.".format(table))

            self.log.info(
                '''DataQualityOperator successfully completed scanning {} \ 
                records on table {}.'''.format(num_records, table))