#!/usr/bin/python
#### One Time SWAMP CSCI Processing of Existing Records ####
import os, time, datetime
import pymssql
import numpy as np
import pandas as pd
import random
import sys
import smtplib
#from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from pandas import DataFrame
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate
from os.path import basename

### get date and time
gettime = int(time.time())
timestamp_date = datetime.datetime.fromtimestamp(gettime)

### Initializes Email Message
msgs = ['TAXONOMY SUMMARY:']

eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')

# get swamp demo station - already has a processed sampleid
#swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where sampleid = '801CYC398_06092016_BMI_RWB_1' OR sampleid = '517PLC130_08082016_BMI_RWB_2' OR sampleid = '526PS0321_06282016_BMI_RWB_1'" - original small test dataset
# BMI_CSBP_Trans - total:  , unique stations: , unique_sampleid: , csci: 1340
# BMI_CSBP_Comp - total: 5561 , unique stations: 154, unique_sampleid: 243, csci: 132
# BMI_RWB - total: 197266, unique stations: , unique_sampleid: , csci: 6796
# BMI_RWB1_SFEel - total: 210, unique stations: 1, unique_sampleid: 4, csci: 4 - one test record should fail
# BMI_RWB_MCM - total: 5106, unique stations: , unique_sampleid: , csci: 575
# BMI_TRC - total: 30913, unique stations: 612, unique_sampleid: 759, csci: 1164
# BMI_SNARL - total: 38037, unique stations: 113, unique_sampleid: 194, csci: 193
#swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where collectionmethodcode = 'BMI_RWB1_SFEel'" 
#swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where collectionmethodcode = 'BMI_CSBP_Comp'" 
#swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where collectionmethodcode = 'BMI_RWB_MCM'" 
#swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where collectionmethodcode = 'BMI_RWB'" 
#swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where collectionmethodcode = 'BMI_SNARL'" 
swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where collectionmethodcode = 'BMI_TRC'" 

swamp_sql = eng.execute(swamp_sql_statement)
swamp = DataFrame(swamp_sql.fetchall())
swamp.columns = swamp_sql.keys()
swamp.columns = [x.lower() for x in swamp.columns]

# convert baresult to integer - set as string to align with ceden - and possible na's
swamp.baresult = swamp.baresult.astype(int)

# get samplemonth, sampleday, sampleyear for later use
swamp["samplemonth"] = swamp.sampledate.dt.month
swamp["sampleday"] = swamp.sampledate.dt.day
swamp["sampleyear"] = swamp.sampledate.dt.year

# in the future we will need to check to make sure these arent duplicates
#bugs = swamp.append(smc)
bugs = swamp

# original submitted stations
list_of_original_unique_stations = pd.unique(bugs['stationcode'])
print "list_of_original_unique_stations:"
print list_of_original_unique_stations
unique_original_stations = ','.join("'" + s + "'" for s in list_of_original_unique_stations)

# get a unique list of stations
#list_of_unique_stations = pd.unique(bugs['stationcode'])
#unique_stations = ','.join("'" + s + "'" for s in list_of_unique_stations)
#print "unique_stations: %s" % str(unique_stations) 

# get a unique list of sampleid
#list_of_unique_sampleid = pd.unique(bugs['sampleid'])
#unique_sampleid = ','.join("'" + s + "'" for s in list_of_unique_sampleid)
#print "list_of_unique_sampleid: %s" % str(unique_sampleid)

#### BUGS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISSTATIONCODEXWALK
# BUT STATIONCODE SHOULD ACTUALLY BE GISCODE NOT STATIONCODE
# ResultsTable:StationCode links to Crosswalk:StationCode, which links to GISMetrics:GISCode
# call gisxwalk table using unique stationcodes and get databasecode and giscode
sqlwalk = 'select stationcode,databasecode,giscode from lu_newgisstationcodexwalk where stationcode in (%s)' % unique_original_stations
gisxwalk = pd.read_sql_query(sqlwalk,eng)

bugs = pd.merge(bugs,gisxwalk[['stationcode','giscode','databasecode']], on = ['stationcode'], how='inner')

# only process stations that have associated gismetric data
missing_bugs_xwalk = set(list_of_original_unique_stations)-set(bugs.stationcode.tolist())

# send email if stations missing GIS Metric data.
if missing_bugs_xwalk:
    bad_stations = '\n'.join(str(x) for x in missing_bugs_xwalk)
    msgs.append('The following stations are missing GISXWalk data:\n')
    msgs.append(bad_stations)
    print msgs

# original stations translated to smc stations using giscode
list_of_unique_stations = pd.unique(bugs['giscode'])
print "list_of_unique_stations:"
print list_of_unique_stations
unique_stations = ','.join("'" + s + "'" for s in list_of_unique_stations)

# get a unique list of sampleid
list_of_unique_sampleid = pd.unique(bugs['sampleid'])
unique_sampleid = ','.join("'" + s + "'" for s in list_of_unique_sampleid)
print "list_of_unique_sampleid: %s" % str(unique_sampleid)
 
#### STATIONS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISMETRICS
sqlmetrics = 'select * from tbl_newgismetrics'
gismetrics = pd.read_sql_query(sqlmetrics,eng)
test_stations = pd.unique(bugs['stationcode'])
# merge gismetrics and gisxwalk to get giscode into dataframe
# merge bugs/stationcode and gismetrics/giscode
# old - stations = pd.merge(gismetrics,bugs[['giscode']], left_on = ['stationcode'], right_on = ['giscode'], how='inner')
bugs['original_stationcode'] = bugs['stationcode']
stations = pd.merge(gismetrics,bugs[['giscode','original_stationcode']], left_on = ['stationcode'], right_on = ['giscode'], how='inner')
# drop gismetrics stationcode
stations.drop(['stationcode'],axis=1,inplace=True)
stations.rename(columns={'original_stationcode': 'stationcode'}, inplace=True)
# check stations
test2_stations = pd.unique(stations['stationcode'])
print test2_stations

#### Only process stations that have associated gismetric data.
missing_bugs_stations = set(list_of_unique_stations)-set(bugs.giscode.tolist())
missing_stations_stations = set(list_of_unique_stations)-set(stations.giscode.tolist())

# send email if stations missing GIS Metric data
if missing_bugs_stations|missing_stations_stations:
    bad_stations = '\n'.join(str(x) for x in missing_bugs_stations.union(missing_stations_stations))
    msgs.append('The following stations are missing GIS Metric data:\n')
    msgs.append(bad_stations)
    print msgs

# drop unnecessary columns
bugs.drop(bugs[['actualorganismcount','agencycode','benthiccollectioncomments','benthiclabeffortcomments','discardedorganismcount','effortqacode','extraorganismcount','grabsize','gridsanalyzed','gridsvolumeanalyzed','labsampleid','locationcode','percentsamplecounted','personnelcode_labeffort','personnelcode_results','qacode','qcorganismcount','resqualcode','samplecomments','targetorganismcount','taxonomicqualifier','totalgrids']], axis=1, inplace=True)
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


###################################################################################################################
#                               BEGIN NEW METHOD FOR CLEANDATA AND CSCI PROCESSING                                #
###################################################################################################################

import rpy2
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
import rpy2.robjects.packages as rpackages
from rpy2.robjects.packages import importr
import rpy2.rinterface as rinterface

# shortens notation for accessing robjects
r = robjects.r

# import R package: CSCI
CSCI = importr('CSCI')

# convert  cleanData() and CSCI() functions from CSCI package to python
cd = CSCI.cleanData
csci = CSCI.CSCI

# collect errors and error counts for each group
error_count = {'clean data':0, 'CSCI': 0}
cd_group_errors = []
csci_group_errors = []


# process cleanData and CSCI for each Sample
bugs_grouped = bugs.groupby(['SampleID'])

# open log file for printing status
TIMESTAMP = str(int(round(time.time()*1000)))
logfile = '/var/www/smc/testfiles/' + TIMESTAMP + '.log'

# create objectid row
TIMESTAMP = int(round(time.time()*1000)) # NOTE: timestamp not used for this - broken Paul
def getRandomTimeStamp(row):
	while True:
		obj_id = int(random.random()*10e6)
		if obj_id not in swamp.objectid.tolist():
			row = obj_id
			return row

pandas2ri.activate() 
start_time = int(time.time())
for name, group in bugs_grouped:
    print "group name: %s" % (name)
    bug_sample_id = name

    # get existing csci objectid - you must run this code inside the for loop in order to get the correct last_id
    last_csci_id = 0
    csci_sql = "SELECT objectid FROM new_csci_core WHERE objectid IN (SELECT MAX(objectid) FROM new_csci_core)";
    csci_result = eng.execute(csci_sql)
    for c in csci_result:
        last_csci_id = c[0]
        

    last_s1mmi_id = 0
    s1mmi_sql = "SELECT objectid FROM new_csci_suppl1_mmi WHERE objectid IN (SELECT MAX(objectid) FROM new_csci_suppl1_mmi)";
    s1mmi_result = eng.execute(s1mmi_sql)
    for c in s1mmi_result:
        last_s1mmi_id = c[0]

    last_s2mmi_id = 0
    s2mmi_sql = "SELECT objectid FROM new_csci_suppl2_mmi WHERE objectid IN (SELECT MAX(objectid) FROM new_csci_suppl2_mmi)";
    s2mmi_result = eng.execute(s2mmi_sql)
    for c in s2mmi_result:
        last_s2mmi_id = c[0]
        

    last_s1grps_id = 0
    s1grps_sql = "SELECT objectid FROM new_csci_suppl1_grps WHERE objectid IN (SELECT MAX(objectid) FROM new_csci_suppl1_grps)";
    s1grps_result = eng.execute(s1grps_sql)
    for c in s1grps_result:
        last_s1grps_id = c[0]
        

    last_s1oe_id = 0
    s1oe_sql = "SELECT objectid FROM new_csci_suppl1_oe WHERE objectid IN (SELECT MAX(objectid) FROM new_csci_suppl1_oe)";
    print s1oe_sql
    s1oe_result = eng.execute(s1oe_sql)
    for c in s1oe_result:
        last_s1oe_id = c[0]
        

    last_s2oe_id = 0
    s2oe_sql = "SELECT objectid FROM new_csci_suppl2_oe WHERE objectid IN (SELECT MAX(objectid) FROM new_csci_suppl2_oe)";
    s2oe_result = eng.execute(s2oe_sql)
    for c in s2oe_result:
        last_s2oe_id = c[0]
        
    
    # group stationcode to get just one
    single_station = group.StationCode.unique()
    # to do - check to make sure there is only one but there should only be one
    print "stations_grouped: %s" % single_station[0]
    
    # find stationcode that matches between the bug record and what is in stations
    station = stations.loc[stations['stationcode'] == single_station[0]]
    # convert station to r dataframe
    station = pandas2ri.py2ri(station)

    # copy of group
    group_copy = group

    # make pandas dataframe to r dataframe
    group = pandas2ri.py2ri(group)
    #r.str(group)
    cd_list = cd(group,msgs=True) # msgs=True produces warning messages as strings
    group = cd_list[0]
    #print group
    warn_msg = cd_list[1]
    if warn_msg[0] != 'Data already clean':
         print warn_msg
         msgs.append(warn_msg[0]+'\n')
         error_count['clean data'] += 1
         cd_group_errors.append((name,warn_msg[0]))
    else:
         # Continue to process CSCI if data is clean
         try:
             print "data is clean process csci"
             report = csci(group,station)

             try:
                 print "load csci reports to database"
                 # assign csci elements to proper tables
                 print "assign elements to specific tables"
                 core = pandas2ri.ri2py(report[0])
                 s1mmi = pandas2ri.ri2py(report[1])
                 s1grps = pandas2ri.ri2py(report[2])
                 s1oe = pandas2ri.ri2py(report[3])
                 s2oe = pandas2ri.ri2py(report[4])
                 s2mmi = pandas2ri.ri2py(report[5])

                 # return previously attached fields to the dataframe that will get loaded to database
                 group_copy.columns = [x.lower() for x in group_copy.columns]

                 # fields that need to be filled
                 #core['objectid'] = 0
                 #core.objectid = core.objectid.apply(getRandomTimeStamp)
                 # code below produces extra records
                 print "first - csci"
                 core.columns = [x.lower() for x in core.columns]
                 core['objectid'] = last_csci_id + core.reset_index().index + 1
                 core['processed_by'] = "machine"
                 core['cleaned'] = "Yes"
                 core['scorenotes'] = "Distinct set to NA"
                 core['rand'] = 2
                 core['scoredate'] = timestamp_date
                 core = pd.merge(core,group_copy[['sampleid','sampledate','samplemonth','sampleday','sampleyear','collectionmethodcode','fieldreplicate','record_origin','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
                 core = core.drop_duplicates()
                 core_status = core.to_sql('new_csci_core', eng, if_exists='append', index=False)

                 print "second - s1mmi"
                 s1mmi.columns = [x.lower() for x in s1mmi.columns]
                 s1mmi['objectid'] = last_s1mmi_id + s1mmi.reset_index().index + 1
                 s1mmi['processed_by'] = "machine"
                 s1mmi.rename(columns={'coleoptera_percenttaxa_predicted': 'coleoptera_percenttaxa_predict'}, inplace=True)
                 s1mmi = pd.merge(s1mmi,group_copy[['sampleid','record_origin','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
                 s1mmi = s1mmi.drop_duplicates()
                 s1mmi_status = s1mmi.to_sql('new_csci_suppl1_mmi', eng, if_exists='append', index=False)

                 print "third- s2mmi"
                 s2mmi.columns = [x.lower() for x in s2mmi.columns]
                 s2mmi['objectid'] = last_s2mmi_id + s2mmi.reset_index().index + 1
                 s2mmi['processed_by'] = "machine"
                 s2mmi = pd.merge(s2mmi,group_copy[['sampleid','record_origin','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
                 s2mmi = s2mmi.drop_duplicates()
                 s2mmi_status = s2mmi.to_sql('new_csci_suppl2_mmi', eng, if_exists='append', index=False)

                 print "third - s1grps"
                 s1grps.columns = [x.lower() for x in s1grps.columns]
                 last_s1grps_id = last_s1grps_id + 1
                 s1grps['objectid'] = last_s1grps_id
                 s1grps['processed_by'] = "machine"
                 #missing sampleid - problem
                 #s1grps = pd.merge(s1grps,group_copy[['sampleid','record_origin','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
                 s1grps = s1grps.drop_duplicates()
                 # export to csv
                 s1grps.to_csv('s1grps.csv', mode='a', sep=',', encoding='utf-8', index=False, header=False)
                 #s1grps_status = s1grps.to_sql('csci_suppl1_grps', eng, if_exists='append', index=False)

                 print "fourth - s1oe"
                 s1oe.columns = [x.lower() for x in s1oe.columns]
                 s1oe['objectid'] = last_s1oe_id + s1oe.reset_index().index + 1
                 #print s1oe
                 #s1oe['objectid'] = s1oe.apply(lambda x: int(x.objectid) + x.index, axis=1)
                 s1oe['processed_by'] = "machine"
                 s1oe = pd.merge(s1oe,group_copy[['sampleid','record_origin','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
                 s1oe = s1oe.drop_duplicates()
                 s1oe_status = s1oe.to_sql('new_csci_suppl1_oe', eng, if_exists='append', index=False)
                
                 print "fifth - s2oe"
                 s2oe.columns = [x.lower() for x in s2oe.columns]
                 # fill na with -88
                 #s2oe.fillna(-88, inplace=True)
                 s2oe['captureprob'].replace(['NA'], -88, inplace=True)
                 s2oe['objectid'] = last_s2oe_id + s2oe.reset_index().index + 1
                 s2oe['processed_by'] = "machine"
                 s2oe = pd.merge(s2oe,group_copy[['sampleid','record_origin','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
                 s2oe = s2oe.drop_duplicates()
                 s2oe_status = s2oe.to_sql('new_csci_suppl2_oe', eng, if_exists='append', index=False)

             except Exception as e:
                print "failed to load to database"
                print e
         except Exception as e:
            bad_group = '\nCSCI could not be processed for sample %s for the following reason:\n' % bug_sample_id
            print bad_group
            msgs.append(bad_group)
            msgs.append(e[0]+'\n')
        
# declare sender and recipients of email
sender = 'test@checker.sccwrp.org'
me = 'pauls@sccwrp.org'
#Paul = 'pauls@sccwrp.org'
#IT_help = 'it-help@sccwrp.org'

# Structures and sends email message
'''
email_msg = ''.join(msgs)
print email_msg
msg = MIMEText(email_msg,'plain')
msg['From'] = sender
msg['To'] = Paul # replace Paul with whoever you want to send email to.
msg['Subject'] = 'CSCI Processing Summary'

# sends email
s = smtplib.SMTP('localhost')
s.sendmail(sender,Paul,msg.as_string()) # replace Paul with whoever you want to send email to.
s.quit()
'''
email_attachment = '\n'.join(msgs)
lf = open(logfile, 'w')
lf.write(email_attachment)
lf.close()
print email_attachment

email_body = "The CSCI Processing Summary is attached"

def send_mail(send_from, send_to, subject, text, files=None, server="localhost"):
    
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, 'rb') as fil:
            part = MIMEApplication(fil.read(), Name = basename(f))

        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

send_mail(sender, [me], "CSCI Processing Summary", email_body, [logfile], "localhost")
end_time = int(time.time())
elapsed_time = int((end_time - start_time)/60)
print "unique station count: %s" % len(list_of_unique_stations)
print "unique sampleid count: %s" % len(list_of_unique_sampleid)
print 'Elapsed Time: %s Minutes' % elapsed_time
