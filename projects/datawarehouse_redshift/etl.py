import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Load staging tables by reading files from S3
    
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    Note: See copy_table_queries in sql_queries.py
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Populate fact and dimension tables by reading data from staging tables.
    
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    Note: See insert_table_queries in sql_queries.py
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Main function that builds sparkify's ETL pipeline.
    First stage data from S3 into staging_events and staging_songs tables.
    Second populate fact and dimension tables from data staged in staging tables.
    
    Note: See sql_queries.py for details on tables used for staging, fact and dimensions.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Stage data from S3
    load_staging_tables(cur, conn)
    
    # Build fact and dimension tables from staged data.
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()