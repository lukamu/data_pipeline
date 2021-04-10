from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This Operator loads data into Dimension Table from Staging Table.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, conn_id, drop_table,
                 target_table, insert_query,
                 append, *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.drop_table = drop_table
        self.target_table = target_table
        self.insert_query = insert_query
        self.append = append

    def execute(self, context):
        self.log.info('LoadDimensionOperator started.')
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
        if self.drop_table:
            self.log.info('{} table already exists. Start dropping it.'.format(
                self.target_table))
            self.hook.run("DROP TABLE IF EXISTS {}".format(self.target_table))
            self.log.info(
                "Table {} dropped successfully.".format(
                    self.target_table))

        if not self.append:
            self.log.info("Removing data from {}".format(self.target_table))
            self.hook.run("DELETE FROM {}".format(self.target_table))

        self.log.info('Inserting data from staging table.')
        self.hook.run(f'INSERT INTO {self.target_table} {self.insert_query}')
        self.log.info("LoadDimensionOperator insert data successfully completed.")
