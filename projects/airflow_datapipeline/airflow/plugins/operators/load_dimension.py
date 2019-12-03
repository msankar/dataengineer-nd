from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """ Airflow Operator to load dimension tables.
    Args:
    * redshift_conn_id : Redshift connection ID
    * table: Dimension table to load from staging data
    * insert_select_query: SQL query used to insert rows.
    * insert_after_delete: Boolean flag indicating if data should be deleted before load
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_select_query="",
                 insert_after_delete=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_select_query = insert_select_query
        self.insert_after_delete = insert_after_delete

    def execute(self, context):
        ''' Truncate table if required. Insert rows into dim table
            from staging table.
        '''
        self.log.info(f"Loading data from staging tables to the {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if(self.insert_after_delete):
            self.log.info(f"Truncate dimension table: {self.table}")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info(f"Populate dimension table: {self.table}")
        insert_query = """
            INSERT INTO {table}
            {insert_select_query}
        """.format(table = self.table,
                   insert_select_query = self.insert_select_query)
        redshift.run(insert_query)
        
        self.log.info(f"Finished populating dimension table {self.table}")
