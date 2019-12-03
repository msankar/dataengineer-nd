from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """ Airflow Operator to load fact tables.
    Args:
    * redshift_conn_id :  Redshift connection ID
    * table: Fact table to load from staging data
    * insert_select_query: SQL query used to insert rows.
    * insert_after_delete: Boolean flag indicating if data should be deleted before load
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_select_query="",
                 insert_after_delete=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_select_query = insert_select_query
        self.insert_after_delete = insert_after_delete

    def execute(self, context):
        ''' Load fact table from staging table. Delete if required before inserting rows.
        '''
        self.log.info(f"Staging fact table {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if(self.insert_after_delete):
            self.log.info(f"Truncate fact table: {self.table}")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info(f"Populate fact table: {self.table}")
        insert_query = """
            INSERT INTO {table}
            {insert_select_query}
        """.format(table = self.table,
                   insert_select_query = self.insert_select_query)
        redshift.run(insert_query)
        
        self.log.info(f"Completed inserting into fact table. {self.table}")
