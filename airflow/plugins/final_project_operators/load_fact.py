from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'

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
                 table_query = "",
                 *args, **kwargs):
                    
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.table_init = table_init
        self.table_query = table_query
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('#PARAMS#') 
        self.log.info(self.table) 
        self.log.info(self.sql) 

        # Create Table
        self.log.info(" Creating Table if not exist")
        self.log.info(self.table_init)        
        redshift.run(self.table_init)

        if not self.append_only:
            self.log.info("Delete {} fact table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))         

        self.log.info("Insert data into fact table: {}".format(self.table))
        redshift.run(LoadFactOperator.sql_template.format(self.table, self.sql))