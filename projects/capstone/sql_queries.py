import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP DIM and FACT tables
drop_arrivalmode = 'DROP TABLE IF EXISTS d_arrivalmode;'
drop_visatype = 'DROP TABLE IF EXISTS d_visatype;'
drop_date = 'DROP TABLE IF EXISTS d_date;'
drop_address = 'DROP TABLE IF EXISTS d_address;'
drop_port = 'DROP TABLE IF EXISTS d_port;'
drop_country = 'DROP TABLE IF EXISTS d_country;'
drop_city = 'DROP TABLE IF EXISTS d_city;'
drop_state = 'DROP TABLE IF EXISTS d_state;'
drop_immig_fact = 'DROP TABLE IF EXISTS fact_immigration;'
drop_temp = 'DROP TABLE IF EXISTS d_temperature;'

# CREATE DIM and FACT tables
create_arrivalmode = """CREATE TABLE IF NOT EXISTS d_arrivalmode
(id INT, mode CHAR(12));"""

create_visatype = """CREATE TABLE IF NOT EXISTS d_visatype
(id INT, visa_type CHAR(8));"""

create_address = 'CREATE TABLE IF NOT EXISTS d_address (id CHAR(5), name VARCHAR);'

create_port = 'CREATE TABLE IF NOT EXISTS d_port (id CHAR(5), name VARCHAR);'

create_country = """CREATE TABLE IF NOT EXISTS d_country
(id INT PRIMARY KEY, name VARCHAR NOT NULL);"""

create_city = """CREATE TABLE IF NOT EXISTS d_city
(city VARCHAR, state VARCHAR, median_age numeric, male_pop INT, female_pop INT, total_pop INT, num_vets INT,
foreign_born INT, avg_household_size FLOAT, state_code CHAR(2), race VARCHAR, count INT);
"""

create_state = """CREATE TABLE IF NOT EXISTS d_state
(state_code CHAR(2) PRIMARY KEY, male_pop INT, female_pop INT, total_pop INT, foreign_born INT);"""

create_date = """CREATE TABLE d_date
(id INT PRIMARY KEY, year INT NOT NULL, month INT NOT NULL,
 day INT NOT NULL, day_of_week INT NOT NULL, ymd_dash CHAR(10) NOT NULL,
 ymd_nodash CHAR(8) NOT NULL, mdy_nodash CHAR(8) NOT NULL);
"""

create_immig_fact = """CREATE TABLE IF NOT EXISTS fact_immigration 
(immigration_id INTEGER IDENTITY(0,1) PRIMARY KEY,
 cicid INT NOT NULL,
 i94yr INT NOT NULL,
 i94mon INT NOT NULL,
 i94cit INT,
 i94res INT,
 i94port CHAR(5),
 arrdate INT,
 i94mode INT,
 i94addr CHAR(5),
 depdate INT,
 i94bir INT,
 i94visa INT,
 count INT,
 dtadfile VARCHAR,
 visapost CHAR(5),
 occup CHAR(5),
 entdepa CHAR(1),
 entdepd CHAR(1),
 entdepu CHAR(1),
 matflag CHAR(1),
 biryear INT,
 dtaddto VARCHAR,
 gender CHAR(1),
 insnum VARCHAR,
 airline CHAR(3),
 admnum VARCHAR,
 fltno VARCHAR,
 visatype CHAR(3)
);
"""

create_temp = """CREATE TABLE IF NOT EXISTS d_temperature (
    dt date, avg_temp numeric, avg_temp_uncertainty numeric, city varchar,
    country varchar, latitude varchar, longitude varchar
);"""



check_immig = """select count (distinct immigration_id), i94mon
from fact_immigration
group by i94mon
order by i94mon;""" 

upd_immig = """update fact_immigration
set i94bir = NULL
WHERE i94bir < 0 or i94bir > 120;"""

upd_birth="""update fact_immigration
set biryear = NULL
where biryear < 1900 or biryear > 2016;"""

data_quality_sql = [check_immig, upd_immig, upd_birth]

drop_dim_table_queries = [drop_arrivalmode, drop_visatype, drop_date, drop_address, drop_port, drop_country, drop_city, drop_state, drop_temp]
create_dim_table_queries = [create_arrivalmode, create_visatype, create_date, create_address, create_port, create_country, create_city, create_state, create_temp]

drop_fact_table_queries = [drop_immig_fact ]
create_fact_table_queries = [create_immig_fact]


