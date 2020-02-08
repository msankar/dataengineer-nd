import configparser
import psycopg2
import pandas as pd
import re
from collections import defaultdict
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine
from sql_queries import data_quality_sql

import pandas as pd
import psycopg2

# REF: https://docs.python.org/3/howto/logging.html
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s  [%(name)s] %(message)s')
LOG = logging.getLogger('immig_etl')

def insert_visatype(cur, conn):
    """ Populate visatype dimension table.
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    """
    LOG.info('Populating visa type')
    insert_visatype = ("""INSERT INTO d_visatype (id, visa_type) VALUES (1, 'Business'), (2, 'Pleasure'), (3, 'Student');""")
    cur.execute(insert_visatype)
    conn.commit()
    LOG.info('Finished inserting visa type')
    
def insert_arrivalmode(cur, conn):
    """ Populate arrivalmode dimension table.
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    """
    LOG.info('Populating arrival mode dim table')
    insert_arrivalmode = ("""INSERT INTO d_arrivalmode (id, mode) VALUES (1, 'Air'), (2, 'Sea'), (3, 'Land'), (9, 'Not reported');""")
    cur.execute(insert_arrivalmode)
    conn.commit
    LOG.info('Finshed populating arrival mode dim table')
           
def insert_temperature(cur, conn):
    """ Populate temperature dimension table from GlobalLandTemperaturesByCity.csv.
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    """
    LOG.info('Populating temperature dim table')
    insert_temp = ("""INSERT INTO d_temperature(dt, avg_temp, avg_temp_uncertainty, city, country, latitude, longitude)
                     VALUES (%s, %s, %s, %s, %s, %s, %s);""")
    
    temperature_df = pd.read_csv('../../data2/GlobalLandTemperaturesByCity.csv')
    for _, row in temperature_df.iterrows():
        cur.execute(insert_temp, [t if pd.notna(t) else None for t in row])
        conn.commit
    LOG.info('Finished populating temperature dim table')
    
def insert_address(cur,conn):
    """ Populate address dimension table from I94_SAS_Labels_Descriptions.SAS
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    """
    LOG.info('Populating address dim table')
    insert_address = 'INSERT INTO d_address (id, name) VALUES (%s, %s);'
    
    with open('data/I94_SAS_Labels_Descriptions.SAS') as f:
        desc_lines = f.readlines()
        
    address_lines = desc_lines[981:1036]
    regex = re.compile(r"^\s*'(?P<id>..)'\s*=\s*'(?P<name>.+)'.*$")
    addresses = [regex.match(line) for line in address_lines]
    adds = {a.group('id'): a.group('name') for a in addresses}
    assert len(adds) == len(address_lines)
    
    for it in sorted(adds.items()):
        cur.execute(insert_address, list(it))
        conn.commit
    LOG.info('Finished populating address dim table')
    
def insert_arrivalport(cur,conn):
    """ Populate ports dimension table from I94_SAS_Labels_Descriptions.SAS
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    """
    LOG.info('Populating arrival port dim table')
    query = 'INSERT INTO d_port (id, name) VALUES (%s, %s);'
    
    with open('data/I94_SAS_Labels_Descriptions.SAS') as f:
        desc_lines = f.readlines()
        
    port_lines = desc_lines[302:962]        
    regex = re.compile(r"^\s*'(?P<id>...?)'\s*=\s*'(?P<name>.+)'.*$")
    ports = [regex.match(line) for line in port_lines]
    allports = {p.group('id'): p.group('name').strip() for p in ports}
    assert len(allports) == len(port_lines)
    
    for it in sorted(allports.items()):
        cur.execute(query, list(it))
        conn.commit
    LOG.info('Finished populating arrival port dim table')
    
def insert_city(cur, conn):
    """ Populate city dimension table from us-cities-demographics.csv.
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    """
    LOG.info('Populating city dim table')
    insert_city = ("""INSERT INTO d_city
    (city, state, median_age, male_pop, female_pop, total_pop, num_vets, foreign_born, avg_household_size, state_code, race, count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""")
    
    city_df = pd.read_csv('data/us-cities-demographics.csv', delimiter=';')
    for _, row in city_df.iterrows():
        cur.execute(insert_city, [city if pd.notna(city) else None for city in row])
        conn.commit
    LOG.info('Finished populating city dim table')
    
def insert_state(cur, conn):
    """ Populate state dimension table from us-cities-demographics.csv.
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    """
    LOG.info('Populating state dim table')
    insert_state = """INSERT INTO d_state (state_code, male_pop, female_pop, total_pop, foreign_born)
                      VALUES (%s, %s, %s, %s, %s);"""
    
    city_df = pd.read_csv('data/us-cities-demographics.csv', delimiter=';')
    state_df = city_df[['State Code', 'Male Population', 'Female Population', 'Total Population', 'Foreign-born']].groupby(['State Code']).sum().reset_index()
       
    for _, row in state_df.iterrows():
        cur.execute(insert_state, list(row))
        conn.commit
    LOG.info('Finished populating state dim table')
    
def insert_country(cur, conn):
    """ Populate country dimension table.
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    """
    LOG.info('Populating country dim table')
    query = """INSERT INTO d_country (id, name) VALUES (%s, %s);"""
    
    with open('data/I94_SAS_Labels_Descriptions.SAS') as f:
        desc_lines = f.readlines()
        
    country_lines = desc_lines[9:298]
    regex = re.compile(r"^\s*(?P<id>\d+)\s*=\s*'(?P<name>.+)'.*$")
    countries = [regex.match(l) for l in country_lines]
    allcountries = {int(c.group('id')): c.group('name') for c in countries}
    assert len(allcountries) == len(country_lines)
                                
    for it in sorted(allcountries.items()):
        cur.execute(query, list(it))
        conn.commit
    LOG.info('Finished populating country dim table')
    
def insert_date(cur, conn):
    """ Populate date dimension table. This is done because date in immigration data is a SAS date numeric field and  a 
        permament format has not been applied. We see that Jan 1, 2016 is 20454. So use that as id in the dim table.
        
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    """
    LOG.info('Populating date dim table')
    insert_date = """INSERT INTO d_date(id, year, month, day, day_of_week, ymd_dash, ymd_nodash, mdy_nodash)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
    end_date = datetime(2019, 12, 31)
    start_date = datetime(1900, 1, 1)
    one = timedelta(days=1)
    id = -21915 #calculated based on 20454 for jan 1, 2016
    
    date = start_date
    while date <= end_date:
        cur.execute(insert_date, 
               [id, date.year, date.month, date.day, date.weekday(), date.strftime('%Y-%m-%d'),
                date.strftime('%Y%m%d'), date.strftime('%d%m%Y')]
               )
        date = date + one
        id +=1
        conn.commit
    LOG.info('Finished populating date dim table')
        
def insert_dimension_tables(cur, conn):
    """  Insert all dimension tables
        
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    """
    insert_arrivalmode(cur, conn)
    insert_visatype(cur, conn)
    insert_temperature(cur, conn)
    insert_address(cur,conn)
    insert_arrivalport(cur,conn)
    insert_city(cur, conn)
    insert_state(cur, conn)
    insert_country(cur, conn)
    insert_date(cur, conn)
    
def dim_data_quality(cur, conn):
    """ Data quality checks of dim tables
    
    Args:
        cur: Cursor (psycopg2) to execute queries in specified database
        conn: Connection to sparkify database
    """
    dim_tables = ['d_arrivalmode', 'd_visatype', 'd_address', 'd_port', 'd_country', 'd_city', 'd_state', 'd_temperature']
    for table in dim_tables:
        query = "SELECT COUNT(*) FROM " + table
        cur.execute(query)
        conn.commit
        
def fact_data_quality(cur, conn):
    """ Data quality checks defined in "create_dim_table_queries"
    
    Args:
        cur: Cursor (psycopg2) to execute queries in specified database
        conn: Connection to sparkify database
    """
    for query in data_quality_sql:
        cur.execute(query)
        conn.commit()
        
def insert_fact_table(cur, conn):
    """  Insert fact tables
        
    Args:
        cur: Cursor to execute queries against database
        conn: Connection to Redshift DB.
    """
    #conn.set_session(autocommit=True)
    #conn_string= 'postgresql://dwhuser:Passw0rd@dwhcluster1.crewo3ntppii.us-west-2.redshift.amazonaws.com:5439/dwh'
    engine=create_engine(conn)
    immig_files = ['../../data/18-83510-I94-Data-2016/i94_jan16_sub.sas7bdat',
                           '../../data/18-83510-I94-Data-2016/i94_feb16_sub.sas7bdat',
                           '../../data/18-83510-I94-Data-2016/i94_mar16_sub.sas7bdat',
                           '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat',
                           '../../data/18-83510-I94-Data-2016/i94_may16_sub.sas7bdat',
                           '../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat',
                           '../../data/18-83510-I94-Data-2016/i94_jul16_sub.sas7bdat',
                           '../../data/18-83510-I94-Data-2016/i94_aug16_sub.sas7bdat',
                           '../../data/18-83510-I94-Data-2016/i94_sep16_sub.sas7bdat',
                           '../../data/18-83510-I94-Data-2016/i94_oct16_sub.sas7bdat',
                           '../../data/18-83510-I94-Data-2016/i94_nov16_sub.sas7bdat',
                           '../../data/18-83510-I94-Data-2016/i94_dec16_sub.sas7bdat']
    for filename in immig_files:
        LOG.info(f'reading {filename}')
        iterr = pd.read_sas(filename, 'sas7bdat', encoding='ISO-8859-1', chunksize=500000)
        
        for sub_df in iterr:
            try:
                immigration_df = pd.concat([immigration_df, sub_df], join='inner')
            except NameError:
                immigration_df = sub_df


        if 'may16' in filename:
            continue # June has the columns to be deleted.
        else:
            immigration_df.to_sql(
                'fact_immigration',
                engine,
                if_exists='append',
                chunksize=50000,
                index=False
            )
            logger.info('done inserting')
            del immigration_df  

            
def main():
    """ Main function that builds sparkify's ETL pipeline.
    populate fact and dimension tables 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg') 
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    insert_dimension_tables(cur,conn)
    insert_fact_table(curr,conn)

    dim_data_quality(cur,conn)
    fact_data_quality(cur,conn)
    
    conn.close()
if __name__ == "__main__":
    main()