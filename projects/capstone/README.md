
# US Immigration trends
### Data Engineering Capstone Project

#### Project Summary
In this capstone project, I used immigration dataset from 2016 to study immigration patterns into the US and answer following questions
 * where do immigrants come from 
 * what cities do immigrants go

The project follows the following steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up


```python
# Do all imports and installs here
import logging
import numpy as np
import pandas as pd
import psycopg2
from pprint import pprint
from collections import defaultdict
import configparser
import psycopg2
import pandas as pd
import re

from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine

```


```python
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s  [%(name)s] %(message)s')
LOG = logging.getLogger('immig_etl')
```

### Step 1: Scope the Project and Gather Data

#### Scope 
Immigration dataset from 2016 and city demographics dataset is used to study immigration patterns into the US. Used immigration data from US National Tourism and Trade office and City demographic data to understand these trends.  Used Pandas library to read and load data into Redshift. SQL and python scripts were created to build the ETL.

#### Data source used and description
* Immigration data - This data comes from the US National Tourism and Trade Office. https://travel.trade.gov/research/reports/i94/historical/2016.html
* US City demographic data - Data source is opensoft. https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/
* Airport code - Table of airport codes and cities. https://datahub.io/core/airport-codes#data 
* I94_SAS_Labels_Descriptions - Sas file provided gives a better understanding of the data in immigration dataset.

#### Data description and data gathering
Before loading the data into a Redshift db, I will do some exploratory data analysis here to find out what cleaning steps must be performed and understand the DDL I must define for my dim and fact tables.


### Step 2: Explore and Assess the Data
#### Data exploration 
## Immigration data exploration


```python
immigration_data_fnames = ['../../data/18-83510-I94-Data-2016/i94_jan16_sub.sas7bdat',
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
```


```python

filename = immigration_data_fnames[0]
iterator = pd.read_sas(
    filename, 'sas7bdat', encoding='ISO-8859-1', chunksize=20
)
idf = next(iterator)
print(idf.shape)
idf.head()
```

    (20, 28)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>cicid</th>
      <th>i94yr</th>
      <th>i94mon</th>
      <th>i94cit</th>
      <th>i94res</th>
      <th>i94port</th>
      <th>arrdate</th>
      <th>i94mode</th>
      <th>i94addr</th>
      <th>depdate</th>
      <th>...</th>
      <th>entdepu</th>
      <th>matflag</th>
      <th>biryear</th>
      <th>dtaddto</th>
      <th>gender</th>
      <th>insnum</th>
      <th>airline</th>
      <th>admnum</th>
      <th>fltno</th>
      <th>visatype</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>7.0</td>
      <td>2016.0</td>
      <td>1.0</td>
      <td>101.0</td>
      <td>101.0</td>
      <td>BOS</td>
      <td>20465.0</td>
      <td>1.0</td>
      <td>MA</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1996.0</td>
      <td>D/S</td>
      <td>M</td>
      <td>NaN</td>
      <td>LH</td>
      <td>346608285.0</td>
      <td>424</td>
      <td>F1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>8.0</td>
      <td>2016.0</td>
      <td>1.0</td>
      <td>101.0</td>
      <td>101.0</td>
      <td>BOS</td>
      <td>20465.0</td>
      <td>1.0</td>
      <td>MA</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1996.0</td>
      <td>D/S</td>
      <td>M</td>
      <td>NaN</td>
      <td>LH</td>
      <td>346627585.0</td>
      <td>424</td>
      <td>F1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>9.0</td>
      <td>2016.0</td>
      <td>1.0</td>
      <td>101.0</td>
      <td>101.0</td>
      <td>BOS</td>
      <td>20469.0</td>
      <td>1.0</td>
      <td>CT</td>
      <td>20480.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>M</td>
      <td>1999.0</td>
      <td>07152016</td>
      <td>F</td>
      <td>NaN</td>
      <td>AF</td>
      <td>381092385.0</td>
      <td>338</td>
      <td>B2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10.0</td>
      <td>2016.0</td>
      <td>1.0</td>
      <td>101.0</td>
      <td>101.0</td>
      <td>BOS</td>
      <td>20469.0</td>
      <td>1.0</td>
      <td>CT</td>
      <td>20499.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>M</td>
      <td>1971.0</td>
      <td>07152016</td>
      <td>F</td>
      <td>NaN</td>
      <td>AF</td>
      <td>381087885.0</td>
      <td>338</td>
      <td>B2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>11.0</td>
      <td>2016.0</td>
      <td>1.0</td>
      <td>101.0</td>
      <td>101.0</td>
      <td>BOS</td>
      <td>20469.0</td>
      <td>1.0</td>
      <td>CT</td>
      <td>20499.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>M</td>
      <td>2004.0</td>
      <td>07152016</td>
      <td>M</td>
      <td>NaN</td>
      <td>AF</td>
      <td>381078685.0</td>
      <td>338</td>
      <td>B2</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 28 columns</p>
</div>




```python
dfs = []
for fname in immigration_data_fnames:
    files = pd.read_sas(fname, 'sas7bdat', encoding='ISO-8859-1', chunksize=20)
    dfs.append(next(files))
    continue
# print(dfs[0])    
```


```python
with open('data/I94_SAS_Labels_Descriptions.SAS') as f:
    #readFile = f.read()
    #print(readFile)
    f.seek(0)
    lines = f.readlines()
comments = [line for line in lines if line.startswith('/*') and line.endswith('*/\n')]
print(comments)
```

    ['/* I94YR - 4 digit year */\n', '/* I94MON - Numeric month */\n', '/* I94CIT & I94RES - This format shows all the valid and invalid codes for processing */\n', '/* I94PORT - This format shows all the valid and invalid codes for processing */\n', '/* I94MODE - There are missing values as well as not reported (9) */\n', '/* I94BIR - Age of Respondent in Years */\n', '/* COUNT - Used for summary statistics */\n', '/* DTADFILE - Character Date Field - Date added to I-94 Files - CIC does not use */\n', '/* VISAPOST - Department of State where where Visa was issued - CIC does not use */\n', '/* OCCUP - Occupation that will be performed in U.S. - CIC does not use */\n', '/* ENTDEPA - Arrival Flag - admitted or paroled into the U.S. - CIC does not use */\n', '/* ENTDEPD - Departure Flag - Departed, lost I-94 or is deceased - CIC does not use */\n', '/* ENTDEPU - Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use */\n', '/* MATFLAG - Match flag - Match of arrival and departure records */\n', '/* BIRYEAR - 4 digit year of birth */\n', '/* DTADDTO - Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use */\n', '/* GENDER - Non-immigrant sex */\n', '/* INSNUM - INS number */\n', '/* AIRLINE - Airline used to arrive in U.S. */\n', '/* ADMNUM - Admission Number */\n', '/* FLTNO - Flight number of Airline used to arrive in U.S. */\n', '/* VISATYPE - Class of admission legally admitting the non-immigrant to temporarily stay in U.S. */\n']


#### Column descriptions
Understanding the columns in Immigration dataset.
* cicid - primary key
* i94yr - arrival year
* i94mon - arrival month
* i94cit - citizenship
* i94res - country of residence
* i94port - arrival port
* arrdate - arrival date
* i94mode - mode of arrival land, sea, air
* i94addr - arrival address
* depdate - departure date
* i94bir - age of immigrant in years
* i94visa - visa type: business, pleasure, student
* count - "used for summary statistics" 
* dtadfile - character date field CIC does not use
* visapost - Department of State where visa was issued - CIC does not use
* occup - occupation that will be performed. CIC does not use
* entdepd - Arrival Flag - admitted or paroled into the U.S. - CIC does not use
* entdepu - Departure Flag - Departed, lost I-94 or is deceased - CIC does not use
* matflag - match of arrival and departure records
* biryear - year of birth
* dtaddto - max date of stay - CIC does not use
* gender - gender
* insnum - INS number
* airline - airline of arrival
* admnum - admission number
* fltno - flight number of arrival flight
* visatype - class of admission legally admitting non-immigrant to temporarily stay in US


```python
zipped = zip(immigration_data_fnames, dfs)
# list(zipped)
```


```python
cnames_month = {z[0].split('/')[-1].split('_')[1][:3]: list(z[1].columns.values) for z in zipped}
```


```python
print(len(cnames_month['jan']))
for k, v in cnames_month.items():
    if (len(cnames_month[k]) != 28):
        print(k)
        #print(v)
        print(len(v))
```

    28
    jun
    34


Month of June has more 6 olumns than other months.
#### Cleaning Steps
Let's identify the columns that are not in other months and clean up the data.


```python
jan = list(dfs[0].columns.values)
jun = list(dfs[5].columns.values)

for z in (zip(jan, jun)):
    print(z)
```

    ('cicid', 'cicid')
    ('i94yr', 'i94yr')
    ('i94mon', 'i94mon')
    ('i94cit', 'i94cit')
    ('i94res', 'i94res')
    ('i94port', 'i94port')
    ('arrdate', 'arrdate')
    ('i94mode', 'i94mode')
    ('i94addr', 'i94addr')
    ('depdate', 'depdate')
    ('i94bir', 'i94bir')
    ('i94visa', 'i94visa')
    ('count', 'count')
    ('dtadfile', 'validres')
    ('visapost', 'delete_days')
    ('occup', 'delete_mexl')
    ('entdepa', 'delete_dup')
    ('entdepd', 'delete_visa')
    ('entdepu', 'delete_recdup')
    ('matflag', 'dtadfile')
    ('biryear', 'visapost')
    ('dtaddto', 'occup')
    ('gender', 'entdepa')
    ('insnum', 'entdepd')
    ('airline', 'entdepu')
    ('admnum', 'matflag')
    ('fltno', 'biryear')
    ('visatype', 'dtaddto')


We can see that June has 'validres' and 5 other columns that begin with delete_. When immigration data is loaded into fact tables skip these 6 columns.

## Temperature data exploration.


```python
temperature_df = pd.read_csv('../../data2/GlobalLandTemperaturesByCity.csv')
print(temperature_df.shape)
temperature_df.head()
```

    (8599212, 7)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>dt</th>
      <th>AverageTemperature</th>
      <th>AverageTemperatureUncertainty</th>
      <th>City</th>
      <th>Country</th>
      <th>Latitude</th>
      <th>Longitude</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1743-11-01</td>
      <td>6.068</td>
      <td>1.737</td>
      <td>Århus</td>
      <td>Denmark</td>
      <td>57.05N</td>
      <td>10.33E</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1743-12-01</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Århus</td>
      <td>Denmark</td>
      <td>57.05N</td>
      <td>10.33E</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1744-01-01</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Århus</td>
      <td>Denmark</td>
      <td>57.05N</td>
      <td>10.33E</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1744-02-01</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Århus</td>
      <td>Denmark</td>
      <td>57.05N</td>
      <td>10.33E</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1744-03-01</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Århus</td>
      <td>Denmark</td>
      <td>57.05N</td>
      <td>10.33E</td>
    </tr>
  </tbody>
</table>
</div>



## City demographics data exploration


```python
city_demo_df = pd.read_csv('data/us-cities-demographics.csv', delimiter=';')
print(city_demo_df.shape)
city_demo_df.head()
```

    (2891, 12)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>City</th>
      <th>State</th>
      <th>Median Age</th>
      <th>Male Population</th>
      <th>Female Population</th>
      <th>Total Population</th>
      <th>Number of Veterans</th>
      <th>Foreign-born</th>
      <th>Average Household Size</th>
      <th>State Code</th>
      <th>Race</th>
      <th>Count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Silver Spring</td>
      <td>Maryland</td>
      <td>33.8</td>
      <td>40601.0</td>
      <td>41862.0</td>
      <td>82463</td>
      <td>1562.0</td>
      <td>30908.0</td>
      <td>2.60</td>
      <td>MD</td>
      <td>Hispanic or Latino</td>
      <td>25924</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Quincy</td>
      <td>Massachusetts</td>
      <td>41.0</td>
      <td>44129.0</td>
      <td>49500.0</td>
      <td>93629</td>
      <td>4147.0</td>
      <td>32935.0</td>
      <td>2.39</td>
      <td>MA</td>
      <td>White</td>
      <td>58723</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Hoover</td>
      <td>Alabama</td>
      <td>38.5</td>
      <td>38040.0</td>
      <td>46799.0</td>
      <td>84839</td>
      <td>4819.0</td>
      <td>8229.0</td>
      <td>2.58</td>
      <td>AL</td>
      <td>Asian</td>
      <td>4759</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Rancho Cucamonga</td>
      <td>California</td>
      <td>34.5</td>
      <td>88127.0</td>
      <td>87105.0</td>
      <td>175232</td>
      <td>5821.0</td>
      <td>33878.0</td>
      <td>3.18</td>
      <td>CA</td>
      <td>Black or African-American</td>
      <td>24437</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Newark</td>
      <td>New Jersey</td>
      <td>34.6</td>
      <td>138040.0</td>
      <td>143873.0</td>
      <td>281913</td>
      <td>5829.0</td>
      <td>86253.0</td>
      <td>2.73</td>
      <td>NJ</td>
      <td>White</td>
      <td>76402</td>
    </tr>
  </tbody>
</table>
</div>



Let's look at one city Newark to understand data partition. We see that eatch row is at City/State/Race level.


```python
city_demo_df[city_demo_df['City'] == 'Newark'].head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>City</th>
      <th>State</th>
      <th>Median Age</th>
      <th>Male Population</th>
      <th>Female Population</th>
      <th>Total Population</th>
      <th>Number of Veterans</th>
      <th>Foreign-born</th>
      <th>Average Household Size</th>
      <th>State Code</th>
      <th>Race</th>
      <th>Count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4</th>
      <td>Newark</td>
      <td>New Jersey</td>
      <td>34.6</td>
      <td>138040.0</td>
      <td>143873.0</td>
      <td>281913</td>
      <td>5829.0</td>
      <td>86253.0</td>
      <td>2.73</td>
      <td>NJ</td>
      <td>White</td>
      <td>76402</td>
    </tr>
    <tr>
      <th>1770</th>
      <td>Newark</td>
      <td>New Jersey</td>
      <td>34.6</td>
      <td>138040.0</td>
      <td>143873.0</td>
      <td>281913</td>
      <td>5829.0</td>
      <td>86253.0</td>
      <td>2.73</td>
      <td>NJ</td>
      <td>Black or African-American</td>
      <td>144961</td>
    </tr>
    <tr>
      <th>1967</th>
      <td>Newark</td>
      <td>New Jersey</td>
      <td>34.6</td>
      <td>138040.0</td>
      <td>143873.0</td>
      <td>281913</td>
      <td>5829.0</td>
      <td>86253.0</td>
      <td>2.73</td>
      <td>NJ</td>
      <td>Asian</td>
      <td>7349</td>
    </tr>
    <tr>
      <th>2168</th>
      <td>Newark</td>
      <td>New Jersey</td>
      <td>34.6</td>
      <td>138040.0</td>
      <td>143873.0</td>
      <td>281913</td>
      <td>5829.0</td>
      <td>86253.0</td>
      <td>2.73</td>
      <td>NJ</td>
      <td>American Indian and Alaska Native</td>
      <td>2268</td>
    </tr>
    <tr>
      <th>2580</th>
      <td>Newark</td>
      <td>New Jersey</td>
      <td>34.6</td>
      <td>138040.0</td>
      <td>143873.0</td>
      <td>281913</td>
      <td>5829.0</td>
      <td>86253.0</td>
      <td>2.73</td>
      <td>NJ</td>
      <td>Hispanic or Latino</td>
      <td>100432</td>
    </tr>
  </tbody>
</table>
</div>



Let's check if city data needs cleaning up. Things to check include
* Male and Female population adds up to Total population in this dataset.
* Foreign born, Number of verterans populations are less than total population.
* Distinct states should be close to 50. DC and Puerto Rico are the other entities besides states


```python
# Confirm city data looks clean.
for _, row in city_demo_df.iterrows():
    if pd.notnull(row['Foreign-born']):
        assert row['Foreign-born'] <= row['Total Population']
    if pd.notnull(row['Male Population']):
        assert row['Male Population'] + row['Female Population'] == row['Total Population']
    if pd.notnull(row['Number of Veterans']):
        assert row['Number of Veterans'] <= row['Total Population']
    if pd.notnull(row['Count']):
        assert row['Count'] <= row['Total Population']
    for coll in ['Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born', 'Count']:
        assert pd.isnull(row[coll]) or row[coll] > 0
```

City dataset looks clean. Let's look at state data and do some sanity tests. We should expect CA, TX should be in the top 5 most populated states. The number of states should be at most 50 + DC and PR.


```python
state_df = city_demo_df[['State Code', 'Male Population', 'Female Population', 'Total Population', 'Foreign-born']]
state_df = state_df.groupby(['State Code']).sum().reset_index()
print(state_df.shape)
state_df.sort_values(by=['Total Population'], ascending=False).head() #we have 49 rows and CA, TX and NY are in top 3.
```

    (49, 5)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>State Code</th>
      <th>Male Population</th>
      <th>Female Population</th>
      <th>Total Population</th>
      <th>Foreign-born</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4</th>
      <td>CA</td>
      <td>61055672.0</td>
      <td>62388681.0</td>
      <td>123444353</td>
      <td>37059662.0</td>
    </tr>
    <tr>
      <th>44</th>
      <td>TX</td>
      <td>34862194.0</td>
      <td>35691659.0</td>
      <td>70553853</td>
      <td>14498054.0</td>
    </tr>
    <tr>
      <th>34</th>
      <td>NY</td>
      <td>23422799.0</td>
      <td>25579256.0</td>
      <td>49002055</td>
      <td>17186873.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>FL</td>
      <td>15461937.0</td>
      <td>16626425.0</td>
      <td>32306132</td>
      <td>7845566.0</td>
    </tr>
    <tr>
      <th>14</th>
      <td>IL</td>
      <td>10943864.0</td>
      <td>11570526.0</td>
      <td>22514390</td>
      <td>4632600.0</td>
    </tr>
  </tbody>
</table>
</div>



## Airport data exploration
To understand airport data
* Check the data size.
* Number of foreign airports.
* Do we have airports with coordinates that is NULL.
* Find out if all continent lengths are 2.


```python
airport_codes_df = pd.read_csv('data/airport-codes_csv.csv')
print(airport_codes_df.shape)
airport_codes_df.head()

```

    (55075, 12)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ident</th>
      <th>type</th>
      <th>name</th>
      <th>elevation_ft</th>
      <th>continent</th>
      <th>iso_country</th>
      <th>iso_region</th>
      <th>municipality</th>
      <th>gps_code</th>
      <th>iata_code</th>
      <th>local_code</th>
      <th>coordinates</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>00A</td>
      <td>heliport</td>
      <td>Total Rf Heliport</td>
      <td>11.0</td>
      <td>NaN</td>
      <td>US</td>
      <td>US-PA</td>
      <td>Bensalem</td>
      <td>00A</td>
      <td>NaN</td>
      <td>00A</td>
      <td>-74.93360137939453, 40.07080078125</td>
    </tr>
    <tr>
      <th>1</th>
      <td>00AA</td>
      <td>small_airport</td>
      <td>Aero B Ranch Airport</td>
      <td>3435.0</td>
      <td>NaN</td>
      <td>US</td>
      <td>US-KS</td>
      <td>Leoti</td>
      <td>00AA</td>
      <td>NaN</td>
      <td>00AA</td>
      <td>-101.473911, 38.704022</td>
    </tr>
    <tr>
      <th>2</th>
      <td>00AK</td>
      <td>small_airport</td>
      <td>Lowell Field</td>
      <td>450.0</td>
      <td>NaN</td>
      <td>US</td>
      <td>US-AK</td>
      <td>Anchor Point</td>
      <td>00AK</td>
      <td>NaN</td>
      <td>00AK</td>
      <td>-151.695999146, 59.94919968</td>
    </tr>
    <tr>
      <th>3</th>
      <td>00AL</td>
      <td>small_airport</td>
      <td>Epps Airpark</td>
      <td>820.0</td>
      <td>NaN</td>
      <td>US</td>
      <td>US-AL</td>
      <td>Harvest</td>
      <td>00AL</td>
      <td>NaN</td>
      <td>00AL</td>
      <td>-86.77030181884766, 34.86479949951172</td>
    </tr>
    <tr>
      <th>4</th>
      <td>00AR</td>
      <td>closed</td>
      <td>Newport Hospital &amp; Clinic Heliport</td>
      <td>237.0</td>
      <td>NaN</td>
      <td>US</td>
      <td>US-AR</td>
      <td>Newport</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>-91.254898, 35.6087</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Count foreign airports
airport_codes_df[airport_codes_df['iso_country'] != 'US'].shape
```




    (32318, 12)




```python
# Check if we have any coordinates that is NULL
airport_codes_df['coordinates'].isnull().values.any()
# Are all continents length 2 ?
airport_codes_df[airport_codes_df['continent'].str.len() > 2]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ident</th>
      <th>type</th>
      <th>name</th>
      <th>elevation_ft</th>
      <th>continent</th>
      <th>iso_country</th>
      <th>iso_region</th>
      <th>municipality</th>
      <th>gps_code</th>
      <th>iata_code</th>
      <th>local_code</th>
      <th>coordinates</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>



### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
Since the scope of the project is to understand immigration events, our data model will be a star schema with immigration data as the fact table.  Immigration data will be loaded into a table, data will be explored, validated and cleaned after it is loaded. We will then create several dimension tables around our fact immigration data which will be detailed below. To ensure my data model is reproducible I will be creating fact and dim tables in python code. See following python files in this workspace.
* sql_queries.py - has data model of fact and dim tables.
* create_tables.py - Creates dim and fact tables in Redshift.
* etl.py - Builds the pipeline to load the data.

First, here is the data model for immigration data. Mirrors the columns in the immigration data set.
CREATE TABLE IF NOT EXISTS fact_immigration 
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
Dimension table _**d_arrivalmode**_
* Foreign key is fact_immigration.i94mode
CREATE TABLE IF NOT EXISTS d_arrivalmode (id INT, mode CHAR(12));
Dimension table _**d_visatype**_
* Foreign key is fact_immigration.i94visa
CREATE TABLE IF NOT EXISTS d_visatype (id INT, visa_type CHAR(8));
Dimension table _**d_address**_
* Foreign key is fact_immigration.i94addr
CREATE TABLE IF NOT EXISTS d_address (id CHAR(5), name VARCHAR);
Dimension table _**d_port**_

Foreign key is fact_immigration.i94port
CREATE TABLE IF NOT EXISTS d_port (id CHAR(5), name VARCHAR);
Dimension table _**d_country**_
CREATE TABLE IF NOT EXISTS d_country(id INT PRIMARY KEY, name VARCHAR NOT NULL);
Dimension table _**d_city**_
CREATE TABLE IF NOT EXISTS d_city
(city VARCHAR, state VARCHAR, median_age numeric, male_pop INT, female_pop INT, total_pop INT, num_vets INT,
foreign_born INT, avg_household_size FLOAT, state_code CHAR(2), race VARCHAR, count INT);
Dimension table _**d_state**_
* Data aggregated from city table explained in data exploration.
CREATE TABLE IF NOT EXISTS d_state
(state_code CHAR(2) PRIMARY KEY, male_pop INT, female_pop INT, total_pop INT, foreign_born INT);
#### 3.2 Mapping Out Data Pipelines

Following steps will be performed to map out data pipelines. All data will be imported from python file **etl.py** in this workspace. Reason being we want this etl load process to be reproducible and re-runnable.
* Manually insert dimenstion arrival modes. This has 3 distinct modes.
* Manually insert dimension visa type. This has 4 distinct visa types.
* Populate dimension address by parsing out lines from header file.
* Populate dimension port table by parsing out lines from header file.
* Populate dimension country table by parsing out lines from header file.
* Populate dimension city table by reading data from us-cities-demographics.csv
* Populate dimension state table by simply aggregating state data from us-cities-demographics.csv
* Populate immigration fact table by reading 12 files from immigration data frame. Here we have to be careful importing June data and not import the 6 columns we identified in out data exploration.

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model

Build the data pipelines to create the data model. See **sql_queries.py** and **create_tables.py** for dropping and creating dimension and fact tables.
Run the following command to create table from terminal window. _**python create_tables.py**_

### Create the data pipeline from python scripts
Run the following python scripts to create the pipeline. I used jupyter notebook to design and check the scripts, but highly recommend running python scripts to re-create the pipeline.

* Create a REDSHIFT instance in AWS, Use StartupRedshift.ipynb as a template. Make sure your settings are cofigured in dwh.cfg
* _**python create_tables.py**_
* _**python etl.py**_


```python
import configparser
import psycopg2
from sql_queries import create_dim_table_queries, drop_dim_table_queries, create_fact_table_queries, drop_fact_table_queries
config = configparser.ConfigParser()
config.read('dwh.cfg')

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()
```


```python
# SQL queries to drop dimension and fact tables.
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
```


```python
# Drop all dimension tables
drop_dim_table_queries = [drop_arrivalmode, drop_visatype, drop_date, drop_address, drop_port, drop_country, drop_city, drop_state, drop_temp]
for query in drop_dim_table_queries:
    cur.execute(query)
    conn.commit()
```


```python
# Drop fact table
drop_fact_table_queries = [drop_immig_fact ]
for query in drop_fact_table_queries:
    cur.execute(query)
    conn.commit()
```


```python
# CREATE DIMENSION and FACT tables
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
```


```python
create_dim_table_queries = [create_arrivalmode, create_visatype, create_date, create_address, create_port, create_country, create_city, create_state, create_temp]
for query in create_dim_table_queries:
    cur.execute(query)
    conn.commit()
```


```python
create_fact_table_queries = [create_immig_fact]
for query in create_fact_table_queries:
    cur.execute(query)
    conn.commit()
```


```python
# Populate visa type dimension table
LOG.info('Populating visa type')
insert_visatype = ("""INSERT INTO d_visatype (id, visa_type) VALUES (1, 'Business'), (2, 'Pleasure'), (3, 'Student');""")
cur.execute(insert_visatype)
conn.commit()
LOG.info('Finished inserting visa type')
```

    2020-01-28 22:38:48,562 INFO  [immig_etl] Populating visa type
    2020-01-28 22:38:52,323 INFO  [immig_etl] Finished inserting visa type



```python
# Populate arrival mode dimension table
LOG.info('Populating arrival mode dim table')
insert_arrivalmode = ("""INSERT INTO d_arrivalmode (id, mode) VALUES (1, 'Air'), (2, 'Sea'), (3, 'Land'), (9, 'Not reported');""")
cur.execute(insert_arrivalmode)
conn.commit
LOG.info('Finshed populating arrival mode dim table')
```

    2020-01-28 22:40:52,286 INFO  [immig_etl] Populating arrival mode dim table
    2020-01-28 22:40:55,804 INFO  [immig_etl] Finshed populating arrival mode dim table



```python
# Populate address dimension by reading header file lines and parsing out the address lines.
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
LOG.info('Finished populating address dim table')
```

    2020-01-28 22:43:24,340 INFO  [immig_etl] Populating address dim table
    2020-01-28 22:43:34,361 INFO  [immig_etl] Finished populating address dim table



```python
# Populate port dimension table
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
LOG.info('Finished populating arrival port dim table')
```

    2020-01-28 22:59:19,966 INFO  [immig_etl] Populating arrival port dim table
    2020-01-28 23:00:12,727 INFO  [immig_etl] Finished populating arrival port dim table



```python
# Populate city dimension table
LOG.info('Populating city dim table')
insert_city = ("""INSERT INTO d_city
(city, state, median_age, male_pop, female_pop, total_pop, num_vets, foreign_born, avg_household_size, state_code, race, count)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""")

city_df = pd.read_csv('data/us-cities-demographics.csv', delimiter=';')
for _, row in city_df.iterrows():
    cur.execute(insert_city, [city if pd.notna(city) else None for city in row])

LOG.info('Finished populating city dim table')
```

    2020-01-28 23:51:06,269 INFO  [immig_etl] Populating city dim table
    2020-01-28 23:57:04,979 INFO  [immig_etl] Finished populating city dim table



```python
# Populate state dimension table.
LOG.info('Populating state dim table')
insert_state = """INSERT INTO d_state (state_code, male_pop, female_pop, total_pop, foreign_born)
                  VALUES (%s, %s, %s, %s, %s);"""

city_df = pd.read_csv('data/us-cities-demographics.csv', delimiter=';')
state_df = city_df[['State Code', 'Male Population', 'Female Population', 'Total Population', 'Foreign-born']].groupby(['State Code']).sum().reset_index()

for _, row in state_df.iterrows():
    cur.execute(insert_state, list(row))
    conn.commit
LOG.info('Finished populating state dim table')
```

    2020-01-28 23:57:47,114 INFO  [immig_etl] Populating state dim table
    2020-01-28 23:57:55,428 INFO  [immig_etl] Finished populating state dim table



```python
#Populating country dimension table
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

LOG.info('Finished populating country dim table')
```

    2020-01-29 00:02:25,604 INFO  [immig_etl] Populating country dim table
    2020-01-29 00:02:54,408 INFO  [immig_etl] Finished populating country dim table



```python
#Populating temperature dimension table.
LOG.info('Populating temperature dim table')
insert_temp = ("""INSERT INTO d_temperature(dt, avg_temp, avg_temp_uncertainty, city, country, latitude, longitude)
                 VALUES (%s, %s, %s, %s, %s, %s, %s);""")

temperature_df = pd.read_csv('../../data2/GlobalLandTemperaturesByCity.csv')
for _, row in temperature_df.iterrows():
    cur.execute(insert_temp, [t if pd.notna(t) else None for t in row])
    conn.commit
LOG.info('Finished populating temperature dim table')
```

    2020-01-29 03:55:39,512 INFO  [immig_etl] Populating temperature dim table

# Populate fact_immigration table from etl.py. This runs for a very long time. Easier to run from python script.
# June data has extra columns that need not be imported.

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
#### 4.2 Data Quality Checks
Confirm dimension tables are populated and have the expected rows.

# Perform data quality checks on dimension tables
dim_tables = ['d_arrivalmode', 'd_visatype', 'd_address', 'd_port', 'd_country', 'd_city', 'd_state', 'd_temperature']
for table in dim_tables:
    query = "SELECT COUNT(*) FROM " + table
    cur.execute(query)
    conn.commit
Confirm the fact_immigration table is populated
select count(distinct immigration_id), i94mon
from fact_immigration
group by i94mon
order by i94mon;
Birth year must be reasonable. Human life expectancy must be limited to 125.
UPDATE fact_immigration
SET i94bir = NULL
WHERE i94bir < 0 or i94bir > 125;

#### 4.3 Data dictionary 
fact_immigration - Fact table. Each entry represents an immigration event.

immigration_id: primary key
cicid: unique key within a month
i94yr: 4 digit year
i94mon: numeric month, 1-12
i94cit: immigrant's country of origin, foreign key to dim_country
i94res: immigrant's country of residence. foreign key to dim_country
i94port: port of entry; foreign key to dim_port
arrdate: arrival date of immigrant
i94mode: arrival mode; foreign key to dim_arrival_mode
i94addr: immigrant's address; foreign key to dim_address
depdate: departure date
i94bir: immigrant's age in years
i94visa: visa , foreign key to dim_visa_type
count: used for summary statistics; always 1 (for easy adding)
dtadfile: dates YYYYMMDD
visapost: visa issual post
occup: immigrants occupation
entdepa: arrival code
entdepd: departure code
entdepu: update code
matflag: M if the arrival and departure records match
biryear: birth year
dtaddto: MMDDYYYY end date of admission
gender: mostly M and F, also X and U
insnum: Immigration and Naturalization Services number
airline: Airline of entry for immigrant
admnum: admission number
fltno: flight number of immigrant
visatype: short visa codes like WT, B2, WB, etc.

================================================
d_city -  Population statistics on cities in the US. Each entry is city/state/race.

city: city's name
state: state of city
median_age: median age of city
male_pop: population of men in the city
female_pop: population of women in the city
total_pop: population of all people in the city
num_vets: population of veterans in the city
foreign_born: population of foreign-born people in the city
avg_household_size: average household size
state_code: State abbreviation
race: White, Hispanic or Latino, Asian, Black or African-American, or American Indian and Alaska Native
count: population of people of that race in the city

================================================
d_state - Aggregated statistics from dim_city by state

state_code: two-letter abbreviation for state
male_pop: population of men in the state
female_pop: population of women in the state
total_pop: population of all people in the state
foreign_born: population of foreign-born people in the state

================================================
d_country - A list of countries and their codes 

code: id
name: usually a name of a country. There are many that start with INVALID: as well as several different No Country Code([code]) values

================================================
d_address - immigrants  address

id: mostly two-letter abbrev for states. There's DC, GU (Guam), and 99 (All Other Codes) as well
name: name of state, region, etc.

================================================
d_port - A list of the ports of arrival

id: id of port
name: the name of the port; there are some No PORT Code ([code]) values too

================================================
d_date - Dates

code: the CIC code for date where 20454 is 1/1/2016
year: four-digit year
month: month; 1-12
day: day; 1-31
day_of_week: 0-7
ymd_dash: date formatted as YYYY-MM-DD
ymd_nodash: date formatted as YYYYMMDD
mdy_noash: date formatted as MMDDYYYY

================================================
d_arrival_mode - Arrival mode. Foreign key to fact_immigration.i94mode

id: 1, 2, 3, or 9
mode: Air, Sea, Land, or Not reported, respectively

================================================
d_visa_type - Visa type. Foreigy key to fact_immigration.i94visa

id: 1, 2, or 3
visa_type: Business, Pleasure, or Student, respectively
================================================
## Step 5: Summary

### Tools and Technologies
* Tools used are Amazom Redshift database. I used a relational database to make querying my fact and dimensional tables easy and gain analytics on immigration patterns. Immigration data was well structured, that I was able to use a relational Db.
* Chose Redshift because it is fast, scalable and highly performant.
* ETL itself was written using Python, and pandas. This allowed me to explore the data, understand and come up with a _star schema_ with immigration fact table and several dimension tables.
* By creating city, state, country dimension tables, we are able to see the states immigrants are immigrating to and it also helps us understand the demographics of the cities and states they are headed to.

### Update frequency
* fact_immigration must be updated monthly when each new dataset is available.
* Dim tables can be dropped and recreated completely whenever fact_immigration is updated.
* dim_city can be simply updated whenever city's metadata changes or inserted when a city is not found.
* dim_state can be simply updated whenever city's metadata changes. this is an aggregate.

### If data was increased 100x
* If data was increased 100x, I would use Redshift spectrum. Here SQL queries can be performed on data that is stored in Amazon S3 buckets.
* This can save time and money because it eliminates the need to move data from a storage service to a database, and instead directly queries data inside an S3 bucket. 
* Redshift Spectrum also expands the scope of a given query because it extends beyond a user's existing Redshift data warehouse nodes and into large volumes of unstructured S3 data lakes.

REF: 
* https://searchaws.techtarget.com/definition/Amazon-Redshift-Spectrum
* https://aws.amazon.com/blogs/big-data/amazon-redshift-spectrum-extends-data-warehousing-out-to-exabytes-no-loading-required/

### If dashboard must be updated by 7am daily
* I would build a concurrent data orchestration pipeline using Amazon EMR and Apache Spark. An Airflow DAG that has an S3 sensor could read data as soon as it arrives, parse the data and land it in S3 location immediately. Our Reshift spectrum detailed above could read the data immediately to populate the dashboard.

REF: 
* https://aws.amazon.com/blogs/big-data/build-a-concurrent-data-orchestration-pipeline-using-amazon-emr-and-apache-livy/
* https://aws.amazon.com/blogs/big-data/amazon-redshift-spectrum-extends-data-warehousing-out-to-exabytes-no-loading-required/

### If database needed to be accessed by 100+ people
* AWS Redshift spectrum with date partitioned data should perform well for multiple users.
* We could have data replicated to different nodes, if our users are global we could have replication nodes closer to user's location.

