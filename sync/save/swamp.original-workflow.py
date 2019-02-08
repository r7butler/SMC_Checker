import os
import pymssql
from sqlalchemy import create_engine

#eng = create_engine('mssql+pymssql://DarrinG@sccwrp.database.windows.net:3535Harbor@sccwrp.database.windows.net:1433/Bight2008RegionalMonitoring') # azure
inengine = create_engine('mssql+pymssql://DarrinG:i4get7@192.168.1.8:1433/Bight2008Staging')
outengine = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/bight2018')

inconnection = inengine.connect()
outconnection = outengine.connect()

result = inengine.execute("select * from DataTablesList")

count = 1
for row in result:
	#print("datatablename:", row['DataTableName'])
	sql = "insert into tmptest (objectid,datatablename) values (%i,'%s')" % (count,row['DataTableName'])
	print(sql)
	status = outengine.execute(sql,"sde","tmptest")
	print status
	count = count + 1
