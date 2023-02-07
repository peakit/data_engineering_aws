from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON 'auto ignorecase'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        self.log.info("Copying data from S3 to Redshift")
        metastore_backend = MetastoreBackend()
        aws_connection = metastore_backend.get_connection(self.aws_credentials_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info("---> {} to {}".format(s3_path, self.table))
        sql_stmt = StageToRedshiftOperator.copy_sql.format(
            self.table, 
            s3_path, 
            aws_connection.login, 
            aws_connection.password
        )
        redshift_hook.run(sql_stmt)

