import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Drops all the tables defined in "drop_table_queries"
    
    Args:
        cur: Cursor (psycopg2) to execute queries in specified database
        conn: Connection to sparkify database
    Note:
        See drop_table_queries in sql_queries.py
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Creates all the tables defined in "create_table_queries"
    
    Args:
        cur: Cursor (psycopg2) to execute queries in specified database
        conn: Connection to sparkify database
    Note:
        See create_table_queries in sql_queries.py
    """
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()