import os
import time, datetime
import pymssql
from sqlalchemy import create_engine
from pandas import DataFrame
inengine = create_engine('mssql+pymssql://ReadOnlySwamp:ReadOnlySwamp1@165.235.37.226:1433/DW_Full')
stations = inengine.execute("SELECT * from StationDetailLookUp;")
df = DataFrame(stations.fetchall())
df.columns = stations.keys()
df.to_csv("/var/www/smc/sync/swamp-stations-5feb18.csv", sep=",", encoding="utf-8")
df.columns = [x.lower() for x in df.columns]

gettime = int(time.time())
TIMESTAMP = str(gettime)
def getRandomTimeStamp(row):
	row['objectid'] = int(TIMESTAMP) + int(row.name)
	return row
df = df.apply(getRandomTimeStamp, axis=1)

eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
status = df.to_sql('swamp_stationdetaillookup', eng, if_exists='append', index=False)
