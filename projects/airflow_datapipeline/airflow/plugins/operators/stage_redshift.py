from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import datetime

class StageToRedshiftOperator(BaseOperator):
    """ Airflow Operator to load stage tables by reading in data from S3.
        Data may be either in JSON or CSV format. 
    Args:
    * aws_credentials_id: AWS credentials to read from S3
    * redshift_conn_id :  Redshift connection ID
    * table: Staging table to load from data file in S3
    * s3_source: S3 source that has the staging files
    * json_paths: JSON paths of stage data files
    * file_type: File type of data files. JSON for example
    * delimiter: Delimiter used in stage data file
    * execution_date: Date of airflow run
    * backfill: Boolean flag indicating if backfill of data is needed
    """
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,                 
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_source="",
                 file_type= "", #CSV or JSON
                 json_paths="",
                 delimiter=",",
                 ignore_headers=1,
                 execution_date="",
                 backfill=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_source = s3_source
        self.file_type = file_type
        self.json_paths = json_paths
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.execution_date = execution_date
        self.backfill = backfill

    def execute(self, context):
        ''' Copy data from S3 into stage table. Data may be in either json or csv format
        Support back fill of data if required.
        '''
        self.log.info(f"Staging data to {self.table}")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            
        self.log.info(f"Truncate stage table: {self.table}")
        redshift.run("TRUNCATE TABLE {}".format(self.table))  

        if self.backfill:
            exec_date = self.execution_date.format(**context)
            self.log.info("Execution_date: {}".format(exec_date))
            exec_date_obj = datetime.datetime.strptime( exec_date ,'%Y-%m-%d')
            year = exec_date_obj.year
            month = exec_date_obj.month
            s3_source = self.s3_source+'/'+year+'/'+month
            self.s3_source = s3_source
            self.log.info("Execution_date: {}".format(s3_source))
        
        self.log.info(f"Load data to staging table: {self.table}")
        if self.file_type == "JSON":
            copy_query = """
                COPY {table}
                FROM '{s3_source}' 
                ACCESS_KEY_ID '{access_key}'
                SECRET_ACCESS_KEY '{secret_key}'
                {file_type} '{json_paths}';
            """.format(table=self.table,
                       s3_source=self.s3_source,
                       access_key=credentials.access_key,
                       secret_key=credentials.secret_key,
                       file_type=self.file_type,
                       json_paths=self.json_paths)
        elif self.file_type == "CSV":
            copy_query = """
                COPY {table}
                FROM '{s3_source}'
                ACCESS_KEY_ID '{access_key}'
                SECRET_ACCESS_KEY '{secret_key}'
                IGNOREHEADER {}
                DELIMITER '{}'
                {file_type};
            """.format(table=self.table,
                       s3_source=self.s3_source,
                       access_key=credentials.access_key,
                       secret_key=credentials.secret_key,
                       file_type=self.file_type,
                       delimiter=self.delimiter,
                       ignore_headers=self.ignore_headers)
        else:
            self.log.error("Unsupported input file type")
            raise ValueError("Unsupported input file type.")

        redshift.run(copy_query)
        
        self.log.info(f"Completed loading data to {self.table}")





