import configparser
import psycopg2
from sql_queries import create_dim_table_queries, drop_dim_table_queries, create_fact_table_queries, drop_fact_table_queries


def drop_dim_tables(cur, conn):
    """ Drops all the tables defined in "drop_dim_table_queries"
    
    Args:
        cur: Cursor (psycopg2) to execute queries in specified database
        conn: Connection to sparkify database
    Note:
        See drop_table_queries in sql_queries.py
    """
    for query in drop_dim_table_queries:
        cur.execute(query)
        conn.commit()


def create_dim_tables(cur, conn):
    """ Creates all the tables defined in "create_dim_table_queries"
    
    Args:
        cur: Cursor (psycopg2) to execute queries in specified database
        conn: Connection to sparkify database
    Note:
        See create_table_queries in sql_queries.py
    """
    for query in create_dim_table_queries:
        cur.execute(query)
        conn.commit()

def drop_fact_tables(cur, conn):
    """ Drops all the tables defined in "drop_fact_table_queries"
    
    Args:
        cur: Cursor (psycopg2) to execute queries in specified database
        conn: Connection to sparkify database
    Note:
        See drop_table_queries in sql_queries.py
    """
    for query in drop_fact_table_queries:
        cur.execute(query)
        conn.commit()


def create_fact_tables(cur, conn):
    """ Creates all the tables defined in "create_fact_table_queries"
    
    Args:
        cur: Cursor (psycopg2) to execute queries in specified database
        conn: Connection to sparkify database
    Note:
        See create_table_queries in sql_queries.py
    """
    for query in create_fact_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """ Connects to database specified in config (dwh.cfg) file.
    Drops and creates stage, fact and dimension tables in specified db.
    
    Note:
        See drop_table_queries and create_table_queries in sql_queries.py
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_dim_tables(cur, conn)
    create_dim_tables(cur, conn)
    drop_fact_tables(cur, conn)
    create_fact_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()