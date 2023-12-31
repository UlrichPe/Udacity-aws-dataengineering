from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)

    copy_sql_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {} '{}'
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 append_only = False,
                 table_init = "",
                 region = "",
                 file_format = "",
                 jsonpath = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table_init = table_init
        self.region = region
        self.file_format = file_format
        self.jsonpath = jsonpath
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook    = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift    = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Create Table
        self.log.info("Checking / Creating Table ")
        self.log.info(self.table_init)        
        redshift.run(self.table_init)

        # Copy Data
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        copy_sql = StageToRedshiftOperator.copy_sql_template.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format,
            self.jsonpath,
        )
        self.log.info(copy_sql)
        redshift.run(copy_sql)