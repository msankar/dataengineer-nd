from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """ Airflow Operator to verify data quality in tables loaded by tasks.
    Args:
    * redshift_conn_id      -- AWS Redshift connection ID
    * check_tables          -- List of table to validate
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 error_message="",
                 check_tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_tables = check_tables
        self.error_message = error_message

    def execute(self, context):
        ''' Checks data quality of tables specified.
        '''
        self.log.info(f"Checking data quality")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.check_tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))        
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error("{} is invalid".format(table))
                raise ValueError("Data quality check failed. {} is invalid".format(table))
            num_records = records[0][0]
            if num_records == 0:
                self.log.error("Empty table {}".format(table))
                raise ValueError("Empty table{}".format(table))
            self.log.info("Quality check for {} passed, it has {} records".format(table, num_records))
        
        self.log.info(f"Finished checking data quality")