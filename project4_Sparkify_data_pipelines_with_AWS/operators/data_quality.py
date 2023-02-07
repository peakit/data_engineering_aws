from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        self.log.info(f'Performing Data Quality check')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for chk in self.checks:
            records = redshift_hook.get_records(chk.get('check_sql'))
            if records[0][0] != chk.get('expected_result'):
                self.log.info(f"records={records[0][0]} and expected={chk.get('expected_result')}")
                raise ValueError("Data quality check failed.")

        self.log.info(f"Data quality check passed")
