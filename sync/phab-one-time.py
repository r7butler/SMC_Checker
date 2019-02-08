import os, time,datetime
import pymssql
import pandas as pd
import numpy as np
from pandas import DataFrame
from sqlalchemy import create_engine
import psycopg2


##### REQUIRED VARIABLES - YOU MUST SET ####
# database workspace
# original table name
table_name = "Base_DATA_PHab_AllDataProvidersCompiled"
# new table name
fc_name = "tmp_phab3"

#conn = pymssql.connect(server="192.168.1.8",port=1433,user="smc_inquire",password="$HarP*",database="SMCPhab")
conn = create_engine('mssql+pymssql://smc_inquire:$HarP*@192.168.1.8:1433/SMCPHab')

# connect to microsoft sql server, get rows, and put into dataframe
# select all rows from table
sql = "SELECT * FROM %s" % table_name
#sql = "SELECT * FROM %s WHERE RowNum BETWEEN 1 and 10" % table_name
results = conn.execute(sql)
df = DataFrame(results.fetchall())

# lower case column names
df.columns = results.keys()
df.columns = [x.lower() for x in df.columns]
conn.dispose()

df = df.rename(columns={'rownum': 'objectid', 'f-h': 'f_h'})
df['cleaned'] = df['cleaned'].astype('int')
df['deactivate'] = df['deactivate'].astype('int')
df['qaed'] = df['qaed'].astype('int')
df['metricscalculated'] = df['metricscalculated'].astype('int')


eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
status = df.to_sql(fc_name, eng, if_exists='append', index=False, chunksize=1000)
