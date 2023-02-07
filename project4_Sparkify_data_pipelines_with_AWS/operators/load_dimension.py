from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_query="",
                 truncate_query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_query = insert_query
        self.truncate_query = truncate_query

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_query is not None \
            and self.truncate_query != "":
            self.log.info("Truncating dimension table")
            redshift_hook.run(self.truncate_query)

        self.log.info('Loading dimension table')
        redshift_hook.run(self.insert_query)

