import os, time, datetime
import smtplib
import random
import pandas as pd
import numpy as np
import rpy2
import rpy2.robjects as robjects
import rpy2.robjects.packages as rpackages
import rpy2.rinterface as rinterfacei
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, MetaData, select, func
from sqlalchemy.exc import IntegrityError
from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate
from os.path import basename

'''
Description: This script has two primary functions. First, it clears the
             existing data from the 6 CSCI report tables in the smc database.
             Second, it re-processes the CSCI reports from the most recent 
             taxonomy data residing in the unified taxonomy table. This 
             script will run every weekend via a cron job.
'''

#####################
# INITIALIZE REPORT #
#####################
def errorLog(x):
    print(x)

gettime = int(time.time())
timestamp_date = datetime.fromtimestamp(gettime)

msgs = ['CSCI FLUSH AND REPROCESSING REPORT:\n\n']


###############################
# CLEAR EXISTING CSCI REPORTS #
###############################

# Connect to smc database
smc_eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
metadata = MetaData(smc_eng)

# View and Delete record counts of 6 CSCI report tables to be deleted
print("---------- Records deleted from CSCI report tables\n")
msgs.append("---------- Records deleted from CSCI report tables\n")
csci_core = Table('tmp_csci_core',metadata, autoload=True, autoload_with=smc_eng)
count = select([func.count()]).select_from(csci_core).scalar() 
delete_core = csci_core.delete()
delete_core.execute()
print("Records deleted from csci_core: %s" %count)
msgs.append("\nRecords deleted from csci_core: %s" %count)

csci_suppl1_grps = Table('tmp_csci_suppl1_grps',metadata, autoload=True, autoload_with=smc_eng)
count = select([func.count()]).select_from(csci_suppl1_grps).scalar() 
delete_suppl1_grps = csci_suppl1_grps.delete()
delete_suppl1_grps.execute()
print("Records deleted from csci_suppl1_grps: %s" %count)
msgs.append("\nRecords deleted from csci_suppl1_grps: %s" %count)

csci_suppl1_mmi = Table('tmp_csci_suppl1_mmi',metadata, autoload=True, autoload_with=smc_eng)
count = select([func.count()]).select_from(csci_suppl1_mmi).scalar() 
delete_suppl1_mmi = csci_suppl1_mmi.delete()
delete_suppl1_mmi.execute()
print("Records deleted from csci_suppl1_mmi: %s" %count)
msgs.append("\nRecords deleted from csci_suppl1_mmi: %s" %count)

csci_suppl1_oe = Table('tmp_csci_suppl1_oe',metadata, autoload=True, autoload_with=smc_eng)
count = select([func.count()]).select_from(csci_suppl1_oe).scalar() 
delete_suppl1_oe = csci_suppl1_oe.delete()
delete_suppl1_oe.execute()
print("Records deleted from csci_suppl1_oe: %s" %count)
msgs.append("\nRecords deleted from csci_suppl1_oe: %s" %count)

csci_suppl2_mmi = Table('tmp_csci_suppl2_mmi',metadata, autoload=True, autoload_with=smc_eng)
count = select([func.count()]).select_from(csci_suppl2_mmi).scalar() 
delete_suppl2_mmi = csci_suppl2_mmi.delete()
delete_suppl2_mmi.execute()
print("Records deleted from csci_suppl2_mmi: %s" %count)
msgs.append("\nRecords deleted from csci_suppl2_mmi: %s" %count)

csci_suppl2_oe = Table('tmp_csci_suppl2_oe',metadata, autoload=True, autoload_with=smc_eng)
count = select([func.count()]).select_from(csci_suppl2_oe).scalar() 
delete_suppl2_oe = csci_suppl2_oe.delete()
delete_suppl2_oe.execute()
print("Records deleted from csci_suppl2_oe: %s\n\n" %count)
msgs.append("\nRecords deleted from csci_suppl2_oe: %s\n\n" %count)






###############################################
# RE-PROCESS CSCI REPORTS FROM TAXONOMY TABLE #
###############################################

# Create Bugs and Stations Dataframes
# Open log file for printing status
TIMESTAMP = str(int(round(time.time()*1000)))
logfile = '/var/www/smc/testfiles/' + TIMESTAMP + '.log'

# Get Taxonomy Records
taxonomy_query = "SELECT stationcode, sampledate, collectionmethodcode, agencycode, replicate, bmisampleid AS sampleid, benthiccollectioncomments, grabsize, percentsamplecounted, totalgrids, gridsanalyzed, gridsvolumeanalyzed, targetorganismcount, actualorganismcount, extraorganismcount, qcorganismcount, discardedorganismcount, benthiclabeffortcomments, origin_lastupdatedate, finalid, lifestagecode, distinctcode, baresult, resqualcode, qacode, taxonomicqualifier, personnelcode_labeffort, personnelcode_results, labsampleid, record_origin, record_publish FROM taxonomy;"
tax = pd.read_sql_query(taxonomy_query, smc_eng)

# get samplemonth, sampleday, sampleyear for later use
tax["samplemonth"] = tax.sampledate.dt.month
tax["sampleday"] = tax.sampledate.dt.day
tax["sampleyear"] = tax.sampledate.dt.year

# Query CSCI Core Report and build dataframe
core_query = "select sampleid from csci_core"
core = pd.read_sql_query(core_query,smc_eng)

# Query CSCI Suppl1 Reports and build dataframes
suppl1_grps_query = "select stationcode from csci_suppl1_grps"
suppl1_grps = pd.read_sql_query(suppl1_grps_query, smc_eng)

suppl1_mmi_query = "select sampleid from csci_suppl1_mmi"
suppl1_mmi = pd.read_sql_query(suppl1_mmi_query, smc_eng)

suppl1_oe_query = "select sampleid from csci_suppl1_oe"
suppl1_oe = pd.read_sql_query(suppl1_oe_query, smc_eng)

# Query CSCI Suppl2 Reports and Build Dataframes
suppl2_mmi_query = "select sampleid from csci_suppl2_mmi"
suppl2_mmi = pd.read_sql_query(suppl2_mmi_query, smc_eng)

suppl2_oe_query = "select sampleid from csci_suppl2_oe"
suppl2_oe = pd.read_sql_query(suppl2_oe_query, smc_eng)

# Create Sets of Unique SampleIDs (or StationCodes) from Taxonomy and CSCI Report Dataframes
taxonomy_sampleids = set(tax.sampleid.tolist())
taxonomy_stationcodes = set(tax.stationcode.tolist())

core_sampleids = set(core.sampleid.tolist())
s1_grps_stationcodes = set(suppl1_grps.stationcode.tolist())
s1_mmi_sampleids = set(suppl1_mmi.sampleid.tolist())
s1_oe_sampleids = set(suppl1_oe.sampleid.tolist())
s2_mmi_sampleids = set(suppl2_mmi.sampleid.tolist())
s2_oe_sampleids = set(suppl2_oe.sampleid.tolist())

#set_list = [taxonomy_sampleids, core_sampleids, s1_mmi_sampleids, s1_oe_sampleids, s2_mmi_sampleids, s2_oe_sampleids]

# Compare Taxonomy SampleIds and StationCodes to Records in CSCI Reports
errorLog("---------- SampleIDs found in Unified Taxonomy, but not in CSCI Reports")
msgs.append("---------- SampleIDs found in Unified Taxonomy, but not in CSCI Reports\n")

missing_core = taxonomy_sampleids - core_sampleids
errorLog("\nnumber of unified taxonomy records missing core:")
errorLog(len(missing_core))
msgs.append("\nnumber of unified taxonomy records missing core: %s" %len(missing_core))

missing_s1grps = taxonomy_stationcodes - s1_grps_stationcodes
errorLog("\nnumber of unified taxonomy records missing s1grps:")
errorLog(len(missing_s1grps))
msgs.append("\nnumber of unified taxonomy records missing s1grps: %s" %len(missing_s1grps))

missing_s1mmi = taxonomy_sampleids - s1_mmi_sampleids
errorLog("\nnumber of unified taxonomy records missing s1mmi:")
errorLog(len(missing_s1mmi))
msgs.append("\nnumber of unified taxonomy records missing s1mmi: %s" %len(missing_s1mmi))

missing_s1oe = taxonomy_sampleids - s1_oe_sampleids
errorLog("\nnumber of unified taxonomy records missing s1oe:")
errorLog(len(missing_s1oe))
msgs.append("\nnumber of unified taxonomy records missing s1oe: %s" %len(missing_s1oe))

missing_s2mmi = taxonomy_sampleids - s2_mmi_sampleids
errorLog("\nnumber of unified taxonomy records missing s2mmi:")
errorLog(len(missing_s2mmi))
msgs.append("\nnumber of unified taxonomy records missing s2mmi: %s" %len(missing_s2mmi))

missing_s2oe = taxonomy_sampleids - s2_oe_sampleids
errorLog("\nnumber of unified taxonomy records missing s2oe:")
errorLog(len(missing_s2oe))
msgs.append("\nnumber of unified taxonomy records missing s2oe: %s" %len(missing_s2oe))

##Might be useful for later reporting, but for not it seems repetitive. -Jordan 5/2/2019
##missing_sampleids = list(set.union(*[missing_core, missing_s1mmi, missing_s1oe, missing_s2mmi, missing_s2oe]))
##errorLog("\nnumber of unified taxonomy records missing one of the csci reports (not including s1grps):")
##errorLog(len(missing_sampleids))
##msgs.append("\nnumber of unified taxonomy records missing one of the csci reports (not including s1grps): %s" %len(missing_sampleids))

# define bugs dataframe
bugs = tax

# bugs BAResult needs to be integer type
bugs = bugs[~bugs.baresult.isnull()]
bugs.baresult = bugs.baresult.astype(int)

# original submitted stations
list_of_original_unique_stations = pd.unique(bugs['stationcode'])
print "list_of_original_unique_stations:"
print list_of_original_unique_stations
unique_original_stations = ','.join("'" + s + "'" for s in list_of_original_unique_stations)

#### BUGS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISSTATIONCODEXWALK
# BUT STATIONCODE SHOULD ACTUALLY BE GISCODE NOT STATIONCODE
# ResultsTable:StationCode links to Crosswalk:StationCode, which links to GISMetrics:GISCode
# call gisxwalk table using unique stationcodes and get databasecode and giscode
sqlwalk = 'select stationcode,databasecode,giscode from lu_newgisstationcodexwalk where stationcode in (%s)' % unique_original_stations
gisxwalk = pd.read_sql_query(sqlwalk,smc_eng)
bugs = pd.merge(bugs,gisxwalk[['stationcode','giscode','databasecode']], on = ['stationcode'], how='inner')

# only process stations that have associated gismetric data
missing_bugs_xwalk = set(list_of_original_unique_stations)-set(bugs.stationcode.tolist())

# send email if stations missing GIS Metric data
if missing_bugs_xwalk:
    bad_stations = '\n'.join(str(x) for x in missing_bugs_xwalk)
    msgs.append('\n\n---------- The following stations are missing GISXWalk data:\n')
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
sqlmetrics = 'select * from tblgismetrics'
gismetrics = pd.read_sql_query(sqlmetrics,smc_eng)
test_stations = pd.unique(bugs['stationcode'])
# merge gismetrics and gisxwalk to get giscode into dataframe
# merge bugs/stationcode and gismetrics/giscode
bugs['original_stationcode'] = bugs['stationcode']
stations = pd.merge(gismetrics,bugs[['giscode']], left_on = ['stationcode'], right_on = ['giscode'], how='inner')
# drop gismetrics stationcode
stations.drop(['stationcode'],axis=1,inplace=True)
# check stations
#test2_stations = pd.unique(stations['stationcode'])
#print test2_stations

# Only process stations that have associated gismetric data
missing_bugs_stations = set(list_of_unique_stations) - set(bugs.giscode.tolist())
missing_stations_stations = set(list_of_unique_stations) - set(stations.giscode.tolist())

# send email if stations missing GIS Metric data
if missing_bugs_stations|missing_stations_stations:
    bad_stations = '\n'.join(str(x) for x in missing_bugs_stations.union(missing_stations_stations))
    msgs.append('\n\n---------- The following stations are missing GIS Metric data:\n\n')
    msgs.append(bad_stations)
    

# drop unnecessary columns (The values dropped has changed since SWAMP discontinued. removing effortqacode, locationcode, samplecomments - Jordan 2/27/19)
bugs.drop(bugs[['actualorganismcount','agencycode','benthiccollectioncomments','benthiclabeffortcomments','discardedorganismcount','extraorganismcount','grabsize','gridsanalyzed','gridsvolumeanalyzed','labsampleid','percentsamplecounted','personnelcode_labeffort','personnelcode_results','qacode','qcorganismcount','resqualcode','targetorganismcount','taxonomicqualifier','totalgrids']], axis=1, inplace=True)


# rename field                                  
bugs = bugs.rename(columns={'stationcode': 'StationCode', 'sampledate': 'SampleDate', 'replicate': 'FieldReplicate', 'collectionmethodcode': 'CollectionMethodCode', 'finalid': 'FinalID', 'lifestagecode': 'LifeStageCode', 'baresult': 'BAResult', 'databasecode': 'DatabaseCode', 'sampleid': 'SampleID','distinctcode': 'Distinct','distinct': 'Distinct','fieldreplicate': 'FieldReplicate'})

# drop station duplicates
stations.drop_duplicates(inplace=True)
bugs.drop(bugs[['StationCode']], axis=1, inplace=True)
bugs.rename(columns={'giscode': 'StationCode'}, inplace=True)
stations.rename(columns={'giscode': 'stationcode'}, inplace=True)





# shortens notation for accessing robjects
r = robjects.r
# import R packages: base, utils, CSCI, and BMIMetrics
base = importr('base')
utils = importr('utils')
CSCI = importr('CSCI')
BMIMetrics = importr('BMIMetrics')
# convert CSCI() functions from CSCI package to python
csci = CSCI.CSCI
cleandata = CSCI.cleanData
# convert loadMetaData function from BMIMetrics package to python
loadMetaData = BMIMetrics.loadMetaData
# collect errors and error counts for each group
error_count = {'CSCI': 0}
csci_group_errors = []
# process cleanData and CSCI for each Sample
bugs_grouped = bugs.groupby(['SampleID'])
# need to activate pandas2ri in order to function properly
pandas2ri.activate()



msgs.append('\n\n---------- CSCI Processing Errors\n\n')
for name, group in bugs_grouped:
    print "group name: %s" % (name)
    bug_sample_id = name
    # get existing csci objectid - you must run this code inside the for loop in order to get the correct last_id
    last_csci_id = 0
    csci_sql = "SELECT objectid FROM tmp_csci_core WHERE objectid IN (SELECT MAX(objectid) FROM tmp_csci_core)";
    csci_result = smc_eng.execute(csci_sql)
    for c in csci_result:
        last_csci_id = c[0]

    last_s1mmi_id = 0
    s1mmi_sql = "SELECT objectid FROM tmp_csci_suppl1_mmi WHERE objectid IN (SELECT MAX(objectid) FROM tmp_csci_suppl1_mmi)";
    s1mmi_result = smc_eng.execute(s1mmi_sql)
    for c in s1mmi_result:
        last_s1mmi_id = c[0]

    last_s2mmi_id = 0
    s2mmi_sql = "SELECT objectid FROM tmp_csci_suppl2_mmi WHERE objectid IN (SELECT MAX(objectid) FROM tmp_csci_suppl2_mmi)";
    s2mmi_result = smc_eng.execute(s2mmi_sql)
    for c in s2mmi_result:
        last_s2mmi_id = c[0]
    
    last_s1grps_id = 0
    s1grps_sql = "SELECT objectid FROM tmp_csci_suppl1_grps WHERE objectid IN (SELECT MAX(objectid) FROM tmp_csci_suppl1_grps)";
    s1grps_result = smc_eng.execute(s1grps_sql)
    for c in s1grps_result:
        last_s1grps_id = c[0]
    
    last_s1oe_id = 0
    s1oe_sql = "SELECT objectid FROM tmp_csci_suppl1_oe WHERE objectid IN (SELECT MAX(objectid) FROM tmp_csci_suppl1_oe)";
    s1oe_result = smc_eng.execute(s1oe_sql)
    for c in s1oe_result:
        last_s1oe_id = c[0]
    
    last_s2oe_id = 0
    s2oe_sql = "SELECT objectid FROM tmp_csci_suppl2_oe WHERE objectid IN (SELECT MAX(objectid) FROM tmp_csci_suppl2_oe)";
    s2oe_result = smc_eng.execute(s2oe_sql)
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
    
    
    # Only run cleandata if every FinalID in group is found in master list (metadata.rdata in BMIMetrics/inst). -Jordan 5/6/2019
    finalids = set(pandas2ri.ri2py(loadMetaData()).FinalID.tolist())
    group_ids = set(group_copy.FinalID.tolist())
    unknown_ids = group_ids - finalids
    if unknown_ids:
        errorLog("Sample %s has the following unknown FinalIDs: %s" %(bug_sample_id,list(unknown_ids)))
        msgs.append("Sample % has the following unknown FinalIDs:\n")
        msgs.append('\n'.join(str(x) for x in unknown_ids))
    else:
        errorLog("All FinalIDs in sample %s are valid." %bug_sample_id)
        cd_list = cleandata(group,msgs=True) # msgs=True produces warning messages as strings
        group = cd_list[0]
        warn_msg = cd_list[1]
        if warn_msg[0] != 'Data already clean':
            errorLog(warn_msg[0])
            msgs.append("cleanData failed on sample %s for the following reasons:\n" %bug_sample_id)
            msgs.append(warn_msg[0]+'\n')
         
    # Continue to process CSCI 
    try:
        print "Attempting to process CSCI..."
        #NOTE: To guarantee that the previously loaded CSCI data stays consistent we must set the seed before generating the reports.
        base.set_seed(1234)  
        report = csci(group,station)

        try:
            print "Processed Successfully. Attempting to load csci reports to database..."
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

            # lastupdatedate should be set to what it was in the initial database - this shouldnt apply to cscsi-core but all the other tables
            group_copy['lastupdatedate'] = group_copy.origin_lastupdatedate 
            # set origin_lastupdatedate to the processing date
            group_copy['origin_lastupdatedate'] = timestamp_date

            # fields that need to be filled 
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
            print core
            try:
                core_status = core.to_sql('tmp_csci_core', smc_eng, if_exists='append', index=False)
            except IntegrityError:
                msgs.append("Duplicate core data for sample %s. Skip loading to database.\n" %bug_sample_id)
                pass

            print "second - s1mmi"
            s1mmi.columns = [x.lower() for x in s1mmi.columns] 
            s1mmi['objectid'] = last_s1mmi_id + s1mmi.reset_index().index + 1
            s1mmi['processed_by'] = "machine"
            s1mmi.rename(columns={'coleoptera_percenttaxa_predicted': 'coleoptera_percenttaxa_predict'}, inplace=True)
            s1mmi = pd.merge(s1mmi,group_copy[['sampleid','record_origin','lastupdatedate','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
            s1mmi = s1mmi.drop_duplicates()
            print s1mmi
            try:
                s1mmi_status = s1mmi.to_sql('tmp_csci_suppl1_mmi', smc_eng, if_exists='append', index=False)
            except IntegrityError:
                msgs.append("Duplicate suppl1_mmi data for sample %s. Skip loading to database.\n" %bug_sample_id)
                pass

             
            print "third- s2mmi"
            s2mmi.columns = [x.lower() for x in s2mmi.columns]
            s2mmi['objectid'] = last_s2mmi_id + s2mmi.reset_index().index + 1
            s2mmi['processed_by'] = "machine"
            s2mmi = pd.merge(s2mmi,group_copy[['sampleid','record_origin','lastupdatedate','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
            s2mmi = s2mmi.drop_duplicates()
            print s2mmi
            try:
                s2mmi_status = s2mmi.to_sql('tmp_csci_suppl2_mmi', smc_eng, if_exists='append', index=False)
            except IntegrityError:
                msgs.append("Duplicate suppl2_mmi data for sample %s. Skip loading to database.\n" %bug_sample_id)
                pass

            print "third - s1oe"
            s1oe.columns = [x.lower() for x in s1oe.columns]
            s1oe['objectid'] = last_s1oe_id + s1oe.reset_index().index + 1
            s1oe['processed_by'] = "machine"
            s1oe = pd.merge(s1oe,group_copy[['sampleid','record_origin','lastupdatedate','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
            s1oe = s1oe.drop_duplicates()
            print s1oe
            try:
                s1oe_status = s1oe.to_sql('tmp_csci_suppl1_oe', smc_eng, if_exists='append', index=False)
            except IntegrityError:
                msgs.append("Duplicate suppl1_oe data for sample %s. Skip loading to database.\n" %bug_sample_id)
                pass

            print "fourth - s2oe"
            s2oe.columns = [x.lower() for x in s2oe.columns]
            # fill na with -88
            s2oe['captureprob'].replace(['NA'], -88, inplace=True)
            s2oe['objectid'] = last_s2oe_id + s2oe.reset_index().index + 1
            s2oe['processed_by'] = "machine"
            s2oe = pd.merge(s2oe,group_copy[['sampleid','record_origin','lastupdatedate','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
            s2oe = s2oe.drop_duplicates()
            print s2oe
            try:
                s2oe_status = s2oe.to_sql('tmp_csci_suppl2_oe', smc_eng, if_exists='append', index=False)
            except IntegrityError:
                msgs.append("Duplicate suppl2_oe data for sample %s. Skip loading to database.\n" %bug_sample_id)
                pass

            print "last - s1grps"
            s1grps.columns = [x.lower() for x in s1grps.columns]
            s1grps['objectid'] = last_s1grps_id + s1grps.reset_index().index + 1
            s1grps['processed_by'] = "machine"
            #missing sampleid - problem -> there is no sampleid for csci_suppl1_grps table
            #s1grps = pd.merge(s1grps,group_copy[['sampleid','record_origin','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
            # normally record_origin comes from above but since we cant merge we will have to reassign
            s1grps['record_origin'] = "SMC"
            s1grps['lastupdatedate'] = timestamp_date
            s1grps = s1grps.drop_duplicates()
            print s1grps
            try:
                s1grps_status = s1grps.to_sql('tmp_csci_suppl1_grps', smc_eng, if_exists='append', index=False)
            except IntegrityError:
                msgs.append("Duplicate suppl1_grps data for sample %s. Skip loading to database.\n" %bug_sample_id)
                pass


        except Exception as e:
            print "failed to load to database"
            msgs.append(e[0] + '\n')
            print e[0]

    except Exception as e:
        bad_group = '\nCSCI could not be processed for sample %s for the following reason:\n' % bug_sample_id
        print bad_group
        msgs.append(bad_group)
        msgs.append(e[0]+'\n')
        error_count['CSCI'] += 1



# 4TH - STRUCTURE AND SEND EMAIL WITH CSCI Reprocessing SUMMARY                        
#
# ACTION: Report any errors in MERGE or CSCI REPORTS
#
# DESCRIPTION:
#     Due to the inconsistency in the sync and merge of data, the email summaries vary. If no new records are found for either
#     smc or swamp, then the email will notify sccwrp of that fact. If new records are found, it will notify sccwrp of the 
#     difference in dates of the two databases in addition to the number of records it found. If records are found, the email
#     message may also notify sccwrp if the CSCI score could not be processed. 



# declare sender and recipients of email
sender = 'test@checker.sccwrp.org'
me = 'jordang@sccwrp.org'
Paul = 'pauls@sccwrp.org'


# Structures and sends email message
#email_attachment = '\n'.join(msgs)
lf = open(logfile, 'a')
for m in msgs:
    lf.write(m)
#lf.write(email_attachment)
lf.close()
#print email_attachment

email_body = "Please see the attached Swamp & SMC Sync Log."

def send_mail(send_from, send_to, subject, text, files=None, server="localhost"):
    msg = MIMEMultipart()
    
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    
    msg.attach(MIMEText(text))
    
    f = file(files)
    attachment = MIMEText(f.read())
    attachment.add_header('Content-Disposition', 'attachment', filename = os.path.basename(files))
    msg.attach(attachment)
    
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

send_mail(sender, [me, Paul], "CSCI Reprocessing Summary", email_body, logfile, "localhost")














