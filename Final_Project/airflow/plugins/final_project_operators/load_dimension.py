from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    sql_template = """
        INSERT INTO {} {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",  
                 append_only = False,
                 table_init = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.table_init = table_init
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Create Table
        self.log.info("Checking / Creating Table ")
        self.log.info(self.table_init)        
        redshift.run(self.table_init)

        if not self.append_only:
            self.log.info("Delete {} dimension table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))  

        self.log.info("Insert data into dimension table: {}".format(self.table))
        redshift.run(LoadDimensionOperator.sql_template.format(self.table, self.sql))