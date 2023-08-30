import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        quality_errs = 0

        for t in self.table:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {t}")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] <1:
                logging.info(len(records))
                quality_errs += 1
                logging.info(f"Data quality on table {t} indecated problems")

        if (quality_errs > 0):
            self.log.error("Data Quality Problems in {} tables".format(quality_errs))
            raise ValueError("Data Quality Problems in {} tables {}".format(quality_errs))
        else:
            logging.info('Data quality o.k.') 