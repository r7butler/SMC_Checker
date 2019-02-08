import os
import time, datetime
import pymssql
import pandas as pd
import random
import sys
from datetime import datetime
from sqlalchemy import create_engine
from pandas import DataFrame

'''
# first connect to sccwrp stations and get lastupdatedate
sccwrp_engine = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
sccwrp_sql = sccwrp_engine.execute("select max(origin_lastupdatedate) from lu_station where record_origin = 'SWAMP'")
sccwrp_lastupdatedate = sccwrp_sql.fetchall()
print sccwrp_lastupdatedate

# call swamp stationdetaillookup and get the lastupdatedate
swamp_engine = create_engine('mssql+pymssql://ReadOnlySwamp:ReadOnlySwamp1@165.235.37.226:1433/DW_Full')
swamp_sql = swamp_engine.execute("SELECT MAX(lastupdatedate) from StationDetailLookUp")
swamp_lastupdatedate = swamp_sql.fetchall()
print swamp_lastupdatedate

# if sccwrp lastupdatedate doesnt match swamp lastupdatedate get new records
if sccwrp_lastupdatedate != swamp_lastupdatedate:
	swamp_sql = swamp_engine.execute("SELECT stationcode,stationname,targetlatitude,targetlongitude,lastupdatedate from StationDetailLookUp")
	swamp_records = swamp_sql.fetchall()

	# load new records into database - may want to load one at a time
	# notify sccwrp
'''

# go to swamp database and get the taxonomy records for a specific station
inengine = create_engine('mssql+pymssql://ReadOnlySwamp:ReadOnlySwamp1@165.235.37.226:1433/DW_Full')
# working - sql = "SELECT StationDetailLookUp.StationCode, [Location].LocationCode, DWE_Sample.SampleDate, DWE_Sample.AgencyCode, DWE_Sample.SampleComments, BenthicCollection.CollectionMethodCode, BenthicCollection.Replicate, BenthicCollection.SampleID, BenthicCollection.BenthicCollectionComments, BenthicCollectionDetail.GrabSize, BenthicLabEffort.PercentSampleCounted, BenthicLabEffort.TotalGrids, BenthicLabEffort.GridsAnalyzed, BenthicLabEffort.GridsVolumeAnalyzed, BenthicLabEffort.TargetOrganismCount, BenthicLabEffort.ActualOrganismCount, BenthicLabEffort.ExtraOrganismCount, BenthicLabEffort.QCOrganismCount, BenthicLabEffort.DiscardedOrganismCount, BenthicLabEffort.EffortQACode, BenthicLabEffort.BenthicLabEffortComments, BenthicResult.FinalID, BenthicResult.LifeStageCode, BenthicResult.[Distinct] as DistinctCode, BenthicResult.BAResult, BenthicResult.ResQualCode, BenthicResult.QACode, BenthicResult.TaxonomicQualifier, BenthicLabEffort.PersonnelCode AS PersonnelCode_LabEffort, BenthicResult.PersonnelCode AS PersonnelCode_Results, BenthicResult.LabSampleID,BenthicResult.LastUpdateDate FROM ((((Location INNER JOIN (DWE_Sample INNER JOIN StationDetailLookUp ON DWE_Sample.StationCode = StationDetailLookUp.StationCode) ON Location.SampleRowID = DWE_Sample.SampleRowID) INNER JOIN BenthicCollection ON Location.LocationRowID = BenthicCollection.LocationRowID) INNER JOIN BenthicResult ON BenthicCollection.BenthicCollectionRowID = BenthicResult.BenthicCollectionRowID) INNER JOIN BenthicLabEffort ON BenthicCollection.BenthicCollectionRowID = BenthicLabEffort.BenthicCollectionRowID) INNER JOIN BenthicCollectionDetail ON BenthicCollection.BenthicCollectionRowID = BenthicCollectionDetail.BenthicCollectionRowID WHERE (((BenthicCollection.CollectionMethodCode) Like '%s') and StationDetailLookup.StationCode = '%s')" % ('%bmi%',sys.argv[1])
sql = "SELECT StationDetailLookUp.StationCode, [Location].LocationCode, DWE_Sample.SampleDate, DWE_Sample.AgencyCode, DWE_Sample.SampleComments, BenthicCollection.CollectionMethodCode, BenthicCollection.Replicate, BenthicCollection.SampleID, BenthicCollection.BenthicCollectionComments, BenthicCollectionDetail.GrabSize, BenthicLabEffort.PercentSampleCounted, BenthicLabEffort.TotalGrids, BenthicLabEffort.GridsAnalyzed, BenthicLabEffort.GridsVolumeAnalyzed, BenthicLabEffort.TargetOrganismCount, BenthicLabEffort.ActualOrganismCount, BenthicLabEffort.ExtraOrganismCount, BenthicLabEffort.QCOrganismCount, BenthicLabEffort.DiscardedOrganismCount, BenthicLabEffort.EffortQACode, BenthicLabEffort.BenthicLabEffortComments, BenthicResult.FinalID, BenthicResult.LifeStageCode, BenthicResult.[Distinct] as DistinctCode, BenthicResult.BAResult, BenthicResult.ResQualCode, BenthicResult.QACode, BenthicResult.TaxonomicQualifier, BenthicLabEffort.PersonnelCode AS PersonnelCode_LabEffort, BenthicResult.PersonnelCode AS PersonnelCode_Results, BenthicResult.LabSampleID,BenthicResult.LastUpdateDate FROM ((((Location INNER JOIN (DWE_Sample INNER JOIN StationDetailLookUp ON DWE_Sample.StationCode = StationDetailLookUp.StationCode) ON Location.SampleRowID = DWE_Sample.SampleRowID) INNER JOIN BenthicCollection ON Location.LocationRowID = BenthicCollection.LocationRowID) INNER JOIN BenthicResult ON BenthicCollection.BenthicCollectionRowID = BenthicResult.BenthicCollectionRowID) INNER JOIN BenthicLabEffort ON BenthicCollection.BenthicCollectionRowID = BenthicLabEffort.BenthicCollectionRowID) INNER JOIN BenthicCollectionDetail ON BenthicCollection.BenthicCollectionRowID = BenthicCollectionDetail.BenthicCollectionRowID WHERE (((BenthicCollection.CollectionMethodCode) Like '%s'))" % ('%bmi%')
print sql
swamp_sql = inengine.execute(sql)
swamp = DataFrame(swamp_sql.fetchall())
swamp.columns = swamp_sql.keys()
swamp.columns = [x.lower() for x in swamp.columns]
inengine.dispose()

# create objectid row
gettime = int(time.time())
TIMESTAMP = str(gettime)
def getRandomTimeStamp(row):
	row['objectid'] = int(TIMESTAMP) + int(row.name)
	return row
swamp = swamp.apply(getRandomTimeStamp, axis=1)

# adjust record date and and create new record of origin column
swamp.rename(columns={'lastupdatedate': 'origin_lastupdatedate'}, inplace=True)
swamp['record_origin'] = "SWAMP"
print list(swamp)

# append the record to the taxonomy table if there are records
swamp_count = len(swamp.index)
if swamp_count > 0:
	eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
	status = swamp.to_sql('taxonomy', eng, if_exists='append', index=False)

# go to smc database and get the taxonomy records for a specific station
smc_engine = create_engine('mssql+pymssql://smc_inquire:$HarP*@192.168.1.8:1433/SMCPHab')

# working - smc_sql = smc_engine.execute("SELECT tblTaxonomySampleInfo.StationCode, tblTaxonomySampleInfo.SampleDate, tblTaxonomySampleInfo.CollectionMethodCode, tblTaxonomySampleInfo.AgencyCode_LabEffort AS AgencyCode, tblTaxonomySampleInfo.FieldReplicate AS Replicate, tblTaxonomySampleInfo.FieldSampleID AS SampleID, tblTaxonomySampleInfo.BenthicCollectionComments, tblTaxonomySampleInfo.GrabSize, tblTaxonomySampleInfo.PercentSampleCounted, tblTaxonomySampleInfo.TotalGrids, tblTaxonomySampleInfo.GridsAnalyzed, tblTaxonomySampleInfo.GridsVolumeAnalyzed, tblTaxonomySampleInfo.TargetOrganismCount, tblTaxonomySampleInfo.ActualOrganismCount, tblTaxonomySampleInfo.ExtraOrganismCount, tblTaxonomySampleInfo.QCOrganismCount, tblTaxonomySampleInfo.DiscardedOrganismCount, tblTaxonomySampleInfo.BenthicLabEffortComments, tblTaxonomyResults.FinalID, tblTaxonomyResults.LifeStageCode, tblTaxonomyResults.DistinctCode, tblTaxonomyResults.BAResult, tblTaxonomyResults.ResQualCode, tblTaxonomyResults.QACode, tblTaxonomyResults.TaxonomicQualifier, tblTaxonomyResults.PersonnelCode_LabEffort, tblTaxonomyResults.PersonnelCode_Results, tblTaxonomyResults.LabSampleID FROM tblTaxonomyResults INNER JOIN tblTaxonomySampleInfo ON (tblTaxonomySampleInfo.FieldReplicate = tblTaxonomyResults.FieldReplicate) AND (tblTaxonomyResults.SampleDate = tblTaxonomySampleInfo.SampleDate) AND (tblTaxonomyResults.StationCode = tblTaxonomySampleInfo.StationCode) WHERE tblTaxonomySampleInfo.StationCode = '%s' GROUP BY tblTaxonomySampleInfo.StationCode, tblTaxonomySampleInfo.SampleDate, tblTaxonomySampleInfo.CollectionMethodCode, tblTaxonomySampleInfo.AgencyCode_LabEffort, tblTaxonomySampleInfo.FieldReplicate, tblTaxonomySampleInfo.FieldSampleID, tblTaxonomySampleInfo.BenthicCollectionComments, tblTaxonomySampleInfo.GrabSize, tblTaxonomySampleInfo.PercentSampleCounted, tblTaxonomySampleInfo.TotalGrids, tblTaxonomySampleInfo.GridsAnalyzed, tblTaxonomySampleInfo.GridsVolumeAnalyzed, tblTaxonomySampleInfo.TargetOrganismCount, tblTaxonomySampleInfo.ActualOrganismCount, tblTaxonomySampleInfo.ExtraOrganismCount, tblTaxonomySampleInfo.QCOrganismCount, tblTaxonomySampleInfo.DiscardedOrganismCount, tblTaxonomySampleInfo.BenthicLabEffortComments, tblTaxonomyResults.FinalID, tblTaxonomyResults.LifeStageCode, tblTaxonomyResults.DistinctCode, tblTaxonomyResults.BAResult, tblTaxonomyResults.ResQualCode, tblTaxonomyResults.QACode, tblTaxonomyResults.TaxonomicQualifier, tblTaxonomyResults.PersonnelCode_LabEffort, tblTaxonomyResults.PersonnelCode_Results, tblTaxonomyResults.LabSampleID" % (sys.argv[2]))

smc_sql = smc_engine.execute("SELECT tblTaxonomySampleInfo.StationCode, tblTaxonomySampleInfo.SampleDate, tblTaxonomySampleInfo.CollectionMethodCode, tblTaxonomySampleInfo.AgencyCode_LabEffort AS AgencyCode, tblTaxonomySampleInfo.FieldReplicate AS Replicate, tblTaxonomySampleInfo.FieldSampleID AS SampleID, tblTaxonomySampleInfo.BenthicCollectionComments, tblTaxonomySampleInfo.GrabSize, tblTaxonomySampleInfo.PercentSampleCounted, tblTaxonomySampleInfo.TotalGrids, tblTaxonomySampleInfo.GridsAnalyzed, tblTaxonomySampleInfo.GridsVolumeAnalyzed, tblTaxonomySampleInfo.TargetOrganismCount, tblTaxonomySampleInfo.ActualOrganismCount, tblTaxonomySampleInfo.ExtraOrganismCount, tblTaxonomySampleInfo.QCOrganismCount, tblTaxonomySampleInfo.DiscardedOrganismCount, tblTaxonomySampleInfo.BenthicLabEffortComments, tblTaxonomyResults.FinalID, tblTaxonomyResults.LifeStageCode, tblTaxonomyResults.DistinctCode, tblTaxonomyResults.BAResult, tblTaxonomyResults.ResQualCode, tblTaxonomyResults.QACode, tblTaxonomyResults.TaxonomicQualifier, tblTaxonomyResults.PersonnelCode_LabEffort, tblTaxonomyResults.PersonnelCode_Results, tblTaxonomyResults.LabSampleID FROM tblTaxonomyResults INNER JOIN tblTaxonomySampleInfo ON (tblTaxonomySampleInfo.FieldReplicate = tblTaxonomyResults.FieldReplicate) AND (tblTaxonomyResults.SampleDate = tblTaxonomySampleInfo.SampleDate) AND (tblTaxonomyResults.StationCode = tblTaxonomySampleInfo.StationCode) GROUP BY tblTaxonomySampleInfo.StationCode, tblTaxonomySampleInfo.SampleDate, tblTaxonomySampleInfo.CollectionMethodCode, tblTaxonomySampleInfo.AgencyCode_LabEffort, tblTaxonomySampleInfo.FieldReplicate, tblTaxonomySampleInfo.FieldSampleID, tblTaxonomySampleInfo.BenthicCollectionComments, tblTaxonomySampleInfo.GrabSize, tblTaxonomySampleInfo.PercentSampleCounted, tblTaxonomySampleInfo.TotalGrids, tblTaxonomySampleInfo.GridsAnalyzed, tblTaxonomySampleInfo.GridsVolumeAnalyzed, tblTaxonomySampleInfo.TargetOrganismCount, tblTaxonomySampleInfo.ActualOrganismCount, tblTaxonomySampleInfo.ExtraOrganismCount, tblTaxonomySampleInfo.QCOrganismCount, tblTaxonomySampleInfo.DiscardedOrganismCount, tblTaxonomySampleInfo.BenthicLabEffortComments, tblTaxonomyResults.FinalID, tblTaxonomyResults.LifeStageCode, tblTaxonomyResults.DistinctCode, tblTaxonomyResults.BAResult, tblTaxonomyResults.ResQualCode, tblTaxonomyResults.QACode, tblTaxonomyResults.TaxonomicQualifier, tblTaxonomyResults.PersonnelCode_LabEffort, tblTaxonomyResults.PersonnelCode_Results, tblTaxonomyResults.LabSampleID")
smc = DataFrame(smc_sql.fetchall())
smc.columns = smc_sql.keys()
smc.columns = [x.lower() for x in smc.columns]
smc_engine.dispose()

# get new timestamp so its different from swamp above
gettime = int(time.time()) + 500
TIMESTAMP = str(gettime)
def getRandomTimeStamp(row):
	print row
	row['objectid'] = int(TIMESTAMP) + int(row.name)
	return row
smc = smc.apply(getRandomTimeStamp, axis=1)

# adjust record date and and create new record of origin column
smc['record_origin'] = "SMC"

# append the record to the taxonomy table if there are records
smc_count = len(smc.index)
if smc_count > 0:
	status = smc.to_sql('taxonomy', eng, if_exists='append', index=False)

# in the future we will need to check to make sure these arent duplicates
bugs = swamp.append(smc)

# get a unique list of stations
list_of_unique_stations = pd.unique(bugs['stationcode'])
unique_stations = ','.join("'" + s + "'" for s in list_of_unique_stations)

# create sampleid field
#bugs["sampleid"] = bugs["stationcode"] + "_" + bugs["sampledate"].dt.strftime('%m%d%Y').map(str) + "_" + bugs["collectionmethodcode"] + "_" + bugs["replicate"]
# first get adjusted date
bugs["samplerealdate"] = bugs["sampledate"].dt.strftime('%m%d%Y').map(str)
# merge two
bugs["codeanddate"] = bugs.stationcode.astype(str).str.cat(bugs['samplerealdate'], sep='_')
# merge two
bugs["collectionandreplicate"] = bugs.collectionmethodcode.astype(str).str.cat(bugs['replicate'].astype(str), sep='_')
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

# bug fields
#StationCode,SampleDate,FieldReplicate,FinalID,LifeStageCode,Distinct,BAResult,CollectionMethodCode,SampleID,DatabaseCode
#list(bugs)

# drop unnecessary columns
bugs.drop(bugs[['actualorganismcount','agencycode','benthiccollectioncomments','benthiclabeffortcomments','discardedorganismcount','effortqacode','extraorganismcount','grabsize','gridsanalyzed','gridsvolumeanalyzed','labsampleid','locationcode','objectid','percentsamplecounted','personnelcode_labeffort','personnelcode_results','qacode','qcorganismcount','resqualcode','samplecomments','targetorganismcount','taxonomicqualifier','totalgrids']], axis=1, inplace=True)
# if row exists drop row, errors, and lookup_error
if 'row' in bugs.columns:
	bugs.drop(bugs[['row','errors']], axis=1, inplace=True)
if 'lookup_error' in bugs.columns:
	bugs.drop(bugs[['lookup_error']], axis=1, inplace=True)
stations.drop(stations[['objectid','gdb_geomattr_data','shape']], axis=1, inplace=True)
stations.drop(stations[['stationcode']], axis=1, inplace=True)
# rename field
bugs = bugs.rename(columns={'stationcode': 'StationCode', 'sampledate': 'SampleDate', 'replicate': 'FieldReplicate', 'collectionmethodcode': 'CollectionMethodCode', 'finalid': 'FinalID', 'lifestagecode': 'LifeStageCode', 'baresult': 'BAResult', 'databasecode': 'DatabaseCode', 'sampleid': 'SampleID','distinctcode': 'Distinct','distinct': 'Distinct','fieldreplicate': 'FieldReplicate'})

# drop station duplicates
stations.drop_duplicates(inplace=True)

bugs.drop(bugs[['StationCode']], axis=1, inplace=True)
bugs.rename(columns={'giscode': 'StationCode'}, inplace=True)

stations.rename(columns={'giscode': 'stationcode'}, inplace=True)

# dump new bugs and stations dataframe to timestamp csv file location 
# timestamp.bugs.csv
bugs_filename = '/var/www/smc/sync/' + TIMESTAMP + '.bugs.csv'
stations_filename = '/var/www/smc/sync/' + TIMESTAMP + '.stations.csv'
bugs.to_csv(bugs_filename, sep=',', encoding='utf-8', index=False)
stations.to_csv(stations_filename, sep=',', encoding='utf-8', index=False)

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
# if csci score was successful load output of core csci into tmp
if x:
	core = pd.read_csv(x)	
	core.columns = [x.lower() for x in core.columns]
        core['processed_by'] = "machine"

	gettime = int(time.time())
	TIMESTAMP = str(gettime)
	core = core.apply(getRandomTimeStamp, axis=1)
	# drop index
	core.drop(core[['unnamed: 0']], axis=1, inplace=True)
	status = core.to_sql('tmp_cscicore', eng, if_exists='append', index=False)
