import os
import time, datetime
import pandas as pd
from sqlalchemy import create_engine
from pandas import DataFrame 
import sys, logging, time, datetime

gettime = int(time.time())
TIMESTAMP = str(gettime)

eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
results_db = eng.execute("select * from swamp_taxonomyresults")
sampleinfo_db = eng.execute("select * from swamp_taxonomysampleinfo")

result = DataFrame(results_db.fetchall())
sampleinfo = DataFrame(sampleinfo_db.fetchall())
result.columns = results_db.keys()
sampleinfo.columns = sampleinfo_db.keys()

#bugs = pd.merge(result,sampleinfo[['stationcode','fieldsampleid','fieldreplicate','collectionmethodcode']], on=['stationcode','fieldsampleid','fieldreplicate'], how='left')
bugs = pd.merge(result,sampleinfo[['stationcode','fieldsampleid','fieldreplicate','collectionmethodcode']], on=['stationcode','fieldsampleid','fieldreplicate'], how='inner')

# drop all rows where collectionmethodcode is not bmi - drop algae records
# save nulls bugs_null = bugs[bugs['collectionmethodcode'].isnull()]
#bugs.dropna(subset = ['collectionmethodcode'], inplace=True)

list_of_unique_stations = pd.unique(bugs['stationcode'])
unique_stations = ','.join("'" + s + "'" for s in list_of_unique_stations)


#  major issue - trying to concatenate the whole thing at once leads to a killed python session - runs out of memory
#bugs["sampleid"] = bugs["stationcode"] + "_" + bugs["sampledate"].dt.strftime('%m%d%Y').map(str) + "_" + bugs["collectionmethodcode"] + "_" + bugs["fieldreplicate"]
# first get adjusted date
bugs["samplerealdate"] = bugs["sampledate"].dt.strftime('%m%d%Y').map(str)
# merge two
bugs["codeanddate"] = bugs.stationcode.astype(str).str.cat(bugs['samplerealdate'], sep='_')
# merge two
bugs["collectionandreplicate"] = bugs.collectionmethodcode.astype(str).str.cat(bugs['fieldreplicate'].astype(str), sep='_')
# merge both
bugs["sampleid"] = bugs.codeanddate.str.cat(bugs.collectionandreplicate, sep='_')
# drop temp columns
bugs.drop(['samplerealdate','codeanddate','collectionandreplicate'],axis=1,inplace=True)


#### BUGS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISSTATIONCODEXWALK
# BUT STATIONCODE SHOULD ACTUALLY BE GISCODE NOT STATIONCODE
# ResultsTable:StationCode links to Crosswalk:StationCode, which links to GISMetrics:GISCode
# call gisxwalk table using unique stationcodes and get databasecode and giscode
sqlwalk = 'select stationcode,databasecode,giscode from tmp_lu_gisstationcodexwalk where stationcode in (%s)' % unique_stations
gisxwalk = pd.read_sql_query(sqlwalk,eng)
bugs = pd.merge(bugs,gisxwalk[['stationcode','giscode','databasecode']], on = ['stationcode'], how='inner')
 
#### STATIONS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISMETRICS
sqlmetrics = 'select * from tbl_gismetrics'
gismetrics = pd.read_sql_query(sqlmetrics,eng)
# merge gismetrics and gisxwalk to get giscode into dataframe
# merge bugs/stationcode and gismetrics/giscode
stations = pd.merge(gismetrics,bugs[['giscode']], left_on = ['stationcode'], right_on = ['giscode'], how='inner') 
#stations = pd.merge(bugs[['giscode']],gismetrics, left_on = ['giscode'], right_on = ['stationcode'], how='inner')


# DELETE - OLD
'''
#bugs = pd.merge(bugs,df_sql[['stationcode','databasecode']], on=['stationcode'], how='left') 
# drop all records where databasecode is empty - should mean that we are getting records on bugs side that dont match
#bugs.dropna(subset = ['collectionmethodcode'], inplace=True)
#bugs = pd.merge(bugs,df_sql[['stationcode','databasecode']], on=['stationcode'], how='inner') 
#### STATIONS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISMETRICS
# call gismetrics table to build out stations file
#sql_stations = 'select * from tbl_gismetrics where stationcode in (%s)' % unique_stations
#df_stations = pd.read_sql_query(sql_stations,eng)
#stations = pd.merge(df_stations,bugs[['stationcode']], on=['stationcode'], how='left')
#stations = pd.merge(df_stations,bugs[['stationcode']], on=['stationcode'], how='inner')
'''

#### ARE THERE STATIONS IN CODEXWALK THAT ARE NOT IN GISMETRICS?

# bug fields
StationCode,SampleDate,FieldReplicate,FinalID,LifeStageCode,Distinct,BAResult,CollectionMethodCode,SampleID,DatabaseCode
# station fields

# drop unnecessary columns
bugs.drop(bugs[['fieldsampleid','unit','excludedtaxa','personnelcode_labeffort','personnelcode_results','enterdate','taxonomicqualifier','qacode','resultqualifiercode','labsampleid','benthicresultscomments','agencycode_labeffort','result']], axis=1, inplace=True)
# if row exists drop row, errors, and lookup_error
if 'row' in bugs.columns:
	bugs.drop(bugs[['row','errors']], axis=1, inplace=True)
if 'lookup_error' in bugs.columns:
	bugs.drop(bugs[['lookup_error']], axis=1, inplace=True)
stations.drop(stations[['objectid','gdb_geomattr_data','shape']], axis=1, inplace=True)
stations.drop(stations[['stationcode']], axis=1, inplace=True)
# rename field
bugs = bugs.rename(columns={'stationcode': 'StationCode', 'sampledate': 'SampleDate', 'fieldreplicate': 'FieldReplicate', 'collectionmethodcode': 'CollectionMethodCode', 'finalid': 'FinalID', 'lifestagecode': 'LifeStageCode', 'baresult': 'BAResult', 'databasecode': 'DatabaseCode', 'sampleid': 'SampleID','distinctcode': 'Distinct'})
# drop all duplicates
stations.drop_duplicates(inplace=True)


bugs.drop(bugs[['StationCode']], axis=1, inplace=True)
bugs.rename(columns={'giscode': 'StationCode'}, inplace=True)

stations.rename(columns={'giscode': 'stationcode'}, inplace=True)

# dump new bugs and stations dataframe to timestamp csv file location 
# timestamp.bugs.csv
bugs_filename = '/var/www/smc/sync/02feb18.bugs.csv'
stations_filename = '/var/www/smc/sync/02feb18.stations.csv'
bugs.to_csv(bugs_filename, sep=',', encoding='utf-8', index=False)
stations.to_csv(stations_filename, sep=',', encoding='utf-8', index=False)

##### TEMPORARY TEST TO RUN R SCRIPT AND LOAD RETURN VALUE INTO DATABASE #####
''' DISABLE
import subprocess
command = 'Rscript'
path2script = '/var/www/smc/proj/rscripts/sample.R'
args = ['11','3','9','42']
cmd = [command, path2script] + args
x = subprocess.check_output(cmd, universal_newlines=True)
### temporary test to load r script return value into database ###
engine = sqlalchemy.create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
Internal = sessionmaker(bind=engine)
internal = Internal()
try:
	sql_statement = """insert into tmp_score values (%i)""" % int(x)
	internal.execute(sql_statement)
	internal.commit()
	errorLog("insert done")
except:
	errorLog("insert failed")
finally:
	internal.close()
'''
##### END TEMPORARY TEST #######
		
# create bugs file by combining two excel tabs and making database call to get related crosswalk fields - single bugs dataframe
# dump bugs dataframe to timestamped csv
# create station file by getting subsetted fields from database - single stations dataframe
# dump stations dataframe to timestamped csv

# run csci script with new bugs/stations csv files
# outpute csci reports so user can download
import subprocess
command = 'Rscript'
path2script = '/var/www/smc/sync/csci.R'
args = [TIMESTAMP,bugs_filename,stations_filename]
cmd = [command, path2script] + args
#cmd = [command, path2script]
x = subprocess.check_output(cmd, universal_newlines=True)
# NEED TO ADD CODE TO CHECK IF x = true
# IF x = true then all output files process properly
#summary_results_link = 'http://checker.sccwrp.org/smc/logs/%s.core.csv' % TIMESTAMP
#summary_results_link = TIMESTAMP


### IMPORTANT LOAD ONE CSCI FIELD FROM CSV FILE AND MAP IT TO EXISTING BUGS/STATIONS DATAFRAME THEN OUTPUT TO CSV LOAD FILE FOR IMPORT
### AT STAGING INTO DATABASES

# get filenames from fileupload routine
#message = "Start csci checks: %s" % x	
#message = "Start csci checks:"
#errorLog(message)
#state = 0
#except ValueError:
#message = "Critical Error: Failed to run csci checks"	
#errorLog(message)
