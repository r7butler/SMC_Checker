#!/usr/bin/python
import os, time, datetime
import pymssql
import numpy as np
import pandas as pd
import random
import sys
import smtplib
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from pandas import DataFrame
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate
from os.path import basename

### get date and time
#gettime = int(time.time())
#timestamp_date = datetime.datetime.fromtimestamp(gettime)

#========================================================================================================================#
#       PURPOSE:       
#
#           To unify existing taxonomic data into a single table "taxonomy" from SMC and CEDEN programs.                      
#           Also, to process the CSCI index for each sampleid (stationcode, sampledate, collectionmethodcode, replicate).
#
#
#
#
#       RESOURCES:
#
#           Existing OLDSMC
#           Server: 192.168.1.8 (portal.sccwrp.org)
#           Database: SMCPHab
#           Taxonomic Tables: tblTaxonomySampleInfo and tblTaxonomyResult
#
#           Existing SWAMP
#           Database:
#           Server: 
#           Taxonomic Tables:
#
#           Future UNIFIEDSMC
#           Database: smc
#           Server: 192.168.1.17 (smcchecker.sccwrp.org)
#           Taxonomic Tables: taxonomy (unified) and swamp_taxonomy
#
#========================================================================================================================#





# 1ST - INITIALIZE EMAIL MESSAGE AND COLLECT DATA FROM SMC
#
# ACTION: Get new data from OLDSMC destined for UNIFIEDSMC: 
#     1. Based on difference (new records) between smc.taxonomy.origin_lastupdatedate and SMCPHab.tblToxicityResults.LastUpdateDate.
#     2. Modify the record and add new field record_publish set to true if record is in Southern Califoria region and is 2016 or older store in unified taxonomy table.
#
#
# DESCRIPTION:
#     this code looks at the unified taxonomy table in the new SMC database and checks the date of the most recent SMC records. It then compares that date to the date of the most recent
#     records in the old SMCPHab database. If it finds newer records in the SMCPHab database, it will merge those records into the smc database with new appended fields record_origin 
#     and record_publish.



# Initializes Email Message
msgs = ['SMC & SWAMP SYNC SUMMARY:\n']

## START SMC MERGE ##
# first connect to sccwrp taxonomy and get lastupdatedate for SMC Records
sccwrp_engine = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
sccwrp_smc_sql = sccwrp_engine.execute("select max(origin_lastupdatedate) from taxonomy where record_origin = 'SMC'")
sccwrp_lastupdatedate1 = sccwrp_smc_sql.fetchall()
msgs.append("SMC Pre-processing origin_lastupdatedate: %s\n" %sccwrp_lastupdatedate1[0][0].strftime("%b %d %Y"))

# connect to smc tblTaxonomySampleInfo and tblTaxonomyResults using the following query
smc_engine = create_engine('mssql+pymssql://smc_inquire:$HarP*@192.168.1.8:1433/SMCPHab')
smc_query = text("SELECT tblTaxonomySampleInfo.StationCode, tblTaxonomySampleInfo.SampleDate, tblTaxonomySampleInfo.CollectionMethodCode, tblTaxonomySampleInfo.AgencyCode_LabEffort AS AgencyCode, tblTaxonomySampleInfo.FieldReplicate AS Replicate, tblTaxonomySampleInfo.FieldSampleID AS SampleID, tblTaxonomySampleInfo.BenthicCollectionComments, tblTaxonomySampleInfo.GrabSize, tblTaxonomySampleInfo.PercentSampleCounted, tblTaxonomySampleInfo.TotalGrids, tblTaxonomySampleInfo.GridsAnalyzed, tblTaxonomySampleInfo.GridsVolumeAnalyzed, tblTaxonomySampleInfo.TargetOrganismCount, tblTaxonomySampleInfo.ActualOrganismCount, tblTaxonomySampleInfo.ExtraOrganismCount, tblTaxonomySampleInfo.QCOrganismCount, tblTaxonomySampleInfo.DiscardedOrganismCount, tblTaxonomySampleInfo.BenthicLabEffortComments, tblTaxonomySampleInfo.LastChangeDate AS origin_lastupdatedate, tblTaxonomyResults.FinalID, tblTaxonomyResults.LifeStageCode, tblTaxonomyResults.DistinctCode, tblTaxonomyResults.BAResult, tblTaxonomyResults.ResQualCode, tblTaxonomyResults.QACode, tblTaxonomyResults.TaxonomicQualifier, tblTaxonomyResults.PersonnelCode_LabEffort, tblTaxonomyResults.PersonnelCode_Results, tblTaxonomyResults.LabSampleID "
                                         "FROM tblTaxonomyResults "
                                         "INNER JOIN tblTaxonomySampleInfo "
                                         "ON (tblTaxonomySampleInfo.FieldReplicate = tblTaxonomyResults.FieldReplicate) "
                                         "AND (tblTaxonomyResults.SampleDate = tblTaxonomySampleInfo.SampleDate) "
                                         "AND (tblTaxonomyResults.StationCode = tblTaxonomySampleInfo.StationCode) "
                                         "WHERE tblTaxonomySampleInfo.LastChangeDate > '%s' "
                                         "GROUP BY tblTaxonomySampleInfo.StationCode, tblTaxonomySampleInfo.SampleDate, tblTaxonomySampleInfo.CollectionMethodCode, tblTaxonomySampleInfo.AgencyCode_LabEffort, tblTaxonomySampleInfo.FieldReplicate, tblTaxonomySampleInfo.FieldSampleID, tblTaxonomySampleInfo.BenthicCollectionComments, tblTaxonomySampleInfo.GrabSize, tblTaxonomySampleInfo.PercentSampleCounted, tblTaxonomySampleInfo.TotalGrids, tblTaxonomySampleInfo.GridsAnalyzed, tblTaxonomySampleInfo.GridsVolumeAnalyzed, tblTaxonomySampleInfo.TargetOrganismCount, tblTaxonomySampleInfo.ActualOrganismCount, tblTaxonomySampleInfo.ExtraOrganismCount, tblTaxonomySampleInfo.QCOrganismCount, tblTaxonomySampleInfo.DiscardedOrganismCount, tblTaxonomySampleInfo.BenthicLabEffortComments, tblTaxonomySampleInfo.LastChangeDate, tblTaxonomyResults.FinalID, tblTaxonomyResults.LifeStageCode, tblTaxonomyResults.DistinctCode, tblTaxonomyResults.BAResult, tblTaxonomyResults.ResQualCode, tblTaxonomyResults.QACode, tblTaxonomyResults.TaxonomicQualifier, tblTaxonomyResults.PersonnelCode_LabEffort, tblTaxonomyResults.PersonnelCode_Results, tblTaxonomyResults.LabSampleID " %((sccwrp_lastupdatedate1[0][0]+timedelta(days=1)).strftime("%Y-%m-%d")))
#smc_query = text("select tbl_taxonomysampleinfo.stationcode, tbl_taxonomysampleinfo.sampledate, tbl_taxonomysampleinfo.collectionmethodcode, tbl_taxonomysampleinfo.agencycode_labeffort as agencycode, tbl_taxonomysampleinfo.fieldreplicate as replicate, tbl_taxonomysampleinfo.fieldsampleid as sampleid, tbl_taxonomysampleinfo.benthiccollectioncomments, tbl_taxonomysampleinfo.grabsize, tbl_taxonomysampleinfo.percentsamplecounted, tbl_taxonomysampleinfo.totalgrids, tbl_taxonomysampleinfo.gridsanalyzed, tbl_taxonomysampleinfo.gridsvolumeanalyzed, tbl_taxonomysampleinfo.targetorganismcount, tbl_taxonomysampleinfo.actualorganismcount, tbl_taxonomysampleinfo.extraorganismcount, tbl_taxonomysampleinfo.qcorganismcount, tbl_taxonomysampleinfo.discardedorganismcount, tbl_taxonomysampleinfo.benthiclabeffortcomments, tbl_taxonomysampleinfo.last_edited_date as origin_lastupdatedate, tbl_taxonomyresults.finalid, tbl_taxonomyresults.lifestagecode, tbl_taxonomyresults.distinctcode, tbl_taxonomyresults.baresult, tbl_taxonomyresults.resqualcode, tbl_taxonomyresults.qacode, tbl_taxonomyresults.taxonomicqualifier, tbl_taxonomyresults.personnelcode_labeffort, tbl_taxonomyresults.personnelcode_results,tbl_taxonomyresults.labsampleid from tbl_taxonomyresults inner join tbl_taxonomysampleinfo on (tbl_taxonomysampleinfo.fieldreplicate = tbl_taxonomyresults.fieldreplicate) and (tbl_taxonomyresults.sampledate = tbl_taxonomysampleinfo.sampledate) and (tbl_taxonomyresults.stationcode = tbl_taxonomysampleinfo.stationcode) where tbl_taxonomysampleinfo.last_edited_date > '%s' group by tbl_taxonomysampleinfo.stationcode, tbl_taxonomysampleinfo.sampledate, tbl_taxonomysampleinfo.collectionmethodcode, tbl_taxonomysampleinfo.agencycode_labeffort, tbl_taxonomysampleinfo.fieldreplicate, tbl_taxonomysampleinfo.fieldsampleid, tbl_taxonomysampleinfo.benthiccollectioncomments, tbl_taxonomysampleinfo.grabsize, tbl_taxonomysampleinfo.percentsamplecounted, tbl_taxonomysampleinfo.totalgrids, tbl_taxonomysampleinfo.gridsanalyzed, tbl_taxonomysampleinfo.gridsvolumeanalyzed, tbl_taxonomysampleinfo.targetorganismcount, tbl_taxonomysampleinfo.actualorganismcount, tbl_taxonomysampleinfo.extraorganismcount, tbl_taxonomysampleinfo.qcorganismcount, tbl_taxonomysampleinfo.discardedorganismcount, tbl_taxonomysampleinfo.benthiclabeffortcomments, tbl_taxonomysampleinfo.last_edited_date, tbl_taxonomyresults.finalid, tbl_taxonomyresults.lifestagecode, tbl_taxonomyresults.distinctcode, tbl_taxonomyresults.baresult, tbl_taxonomyresults.resqualcode, tbl_taxonomyresults.qacode, tbl_taxonomyresults.taxonomicqualifier, tbl_taxonomyresults.personnelcode_labeffort, tbl_taxonomyresults.personnelcode_results, tbl_taxonomyresults.labsampleid " %((sccwrp_lastupdatedate1[0][0]+timedelta(days=1)).strftime("%Y-%m-%d")))

# create a dataframe from all records newer than the origin_lastupdatedate
smc_sql = smc_engine.execute(smc_query)
smc = DataFrame(smc_sql.fetchall())

# if new records present, prepare the data to be inserted in sccwrp taxonomy
if len(smc.index) > 0:
    smc.columns = smc_sql.keys()
    smc.columns = [x.lower() for x in smc.columns]
    #smc_engine.dispose()
    
    # new field objectid
    smc_tax_sql = "SELECT MAX(objectid) from taxonomy"
    last_smc_objid = sccwrp_engine.execute(smc_tax_sql).fetchall()[0][0]
    smc['objectid'] = smc.index + last_smc_objid + 1

    # new field record_origin
    smc['record_origin'] = pd.Series("SMC", index=np.arange(len(smc)))

    # locationcode comes from swamp not in smc so set it to x
    smc['locationcode'] = 'X'
    
    # new field record_publish
    # code that sets record_publish to true only if the sampledate is before 2017-01-01 and record is in Southern California region
    exprdate = pd.Timestamp(datetime(2017,1,1))
    smc['record_publish'] = smc.apply(lambda x: 'true' if (x.stationcode[0] in [4,8,9])&(x.sampledate < exprdate) else 'false', axis = 1)

    # turn off database submission temporarily - 
    eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
    #status = smc.to_sql('taxonomy', eng, if_exists='append', index=False)
    msgs.append("SMC Post-processing origin_lastupdatedate: %s\n" % smc.origin_lastupdatedate.max().strftime("%b %d %Y %X"))
    msgs.append("A total of %s OLDSMC records have been added.\n" % len(smc))
    eng.dispose()
else:
    msgs.append("No new OLDSMC records added this week.\n")
## END SMC MERGE ##









'''
# 2ND - COLLECT DATA FROM SWAMP
#
# UPDATE: WE ARE NO LONGER COLLECTING DATA FROM SWAMP. THIS COLLECTION IS SUPSPENDED UNTIL FURTHER NOTICE. 
#
# ACTION: Get new data from SWAMP:
#     **** swamp_taxonomy is a duplicate copy of the original data pulled from SWAMP the only modified field is record_publish ****
#     1. Based on difference (new records) between swamp_taxonomy.origin_lastupdatedate and BenthicResult.LastUpdateDate.
#     2. Store a copy of the record in swamp_taxonomy and modify record_publish from 1/0 to true/false.
#     3. Store a second copy of the record in unified taxonomy table and modify record_publish based on following criteria: 
#           If BenthicResult.LastUpdateDate is set to true and if the record is in Southern Califoria region set to true
#
#
# DESCRIPTION: 
#     Similarly to the SMC code above, it looks at the date of the most recent SWAMP records in the swamp_taxonomy table. It then compares that date to 
#     the date of the most recent records in the SWAMP database located at the STATE BOARD. If it finds newer records in the SWAMP database, it will 
#     merge those records into the swamp_taxonomy table with a new appended field record_origin and a new record_publish field based on DWC_PublicRelease. 
#     Here is the criteria for whether to publish or not: "1 means that the data can be shared with the public. If the value is 0, then the data should 
#     not be shared. The decision to release the data should be at the project level." We need to adjust those values from 1/0 to true/false before they 
#     are stored in swamp_taxonomy table. The same data that gets stored in the swamp_taxonomy needs to get stored in the unified taxonomy table, but 
#     each records record_publish need to be adjusted. Rafi only wants southern california data to# published. So any stationcode not equal to starting 
#     with 4, 8, or 9 should be set to false.



## START SWAMP MERGE ##
# first connect to sccwrp swamp_taxonomy and get lastupdatedate for SWAMP Records
sccwrp_engine = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
sccwrp_swamp_sql = sccwrp_engine.execute("select max(origin_lastupdatedate) from swamp_taxonomy where record_origin = 'SWAMP'")
sccwrp_lastupdatedate2 = sccwrp_swamp_sql.fetchall()
msgs.append("SWAMP Pre-processing origin_lastupdatedate: %s\n" %sccwrp_lastupdatedate2[0][0].strftime("%b %d %Y"))

# connect to swamp database using the following query
swamp_engine = create_engine('mssql+pymssql://ReadOnlySwamp:ReadOnlySwamp1@165.235.37.226:1433/DW_Full')

# query retrieves new records based on LastUpdateDate - also uses DWC_PublicRelease to decide whether a record should be published for swamp_taxonomy
swamp_query = text("SELECT StationDetailLookUp.StationCode, [Location].LocationCode, DWE_Sample.SampleDate, DWE_Sample.AgencyCode, DWE_Sample.SampleComments, BenthicCollection.CollectionMethodCode, BenthicCollection.Replicate, BenthicCollection.SampleID, BenthicCollection.BenthicCollectionComments, BenthicCollectionDetail.GrabSize, BenthicLabEffort.PercentSampleCounted, BenthicLabEffort.TotalGrids, BenthicLabEffort.GridsAnalyzed, BenthicLabEffort.GridsVolumeAnalyzed, BenthicLabEffort.TargetOrganismCount, BenthicLabEffort.ActualOrganismCount, BenthicLabEffort.ExtraOrganismCount, BenthicLabEffort.QCOrganismCount, BenthicLabEffort.DiscardedOrganismCount, BenthicLabEffort.EffortQACode, BenthicLabEffort.BenthicLabEffortComments, BenthicResult.FinalID, BenthicResult.LifeStageCode, BenthicResult.[Distinct] as DistinctCode, BenthicResult.BAResult, BenthicResult.ResQualCode, BenthicResult.QACode, BenthicResult.TaxonomicQualifier, BenthicLabEffort.PersonnelCode AS PersonnelCode_LabEffort, BenthicResult.PersonnelCode AS PersonnelCode_Results, BenthicResult.LabSampleID, BenthicResult.LastUpdateDate AS origin_lastupdatedate, BenthicResult.DWC_PublicRelease as record_publish "
                   "FROM ((((Location "
                   
                   "INNER JOIN (DWE_Sample "
                   
                   "INNER JOIN StationDetailLookUp ON DWE_Sample.StationCode = StationDetailLookUp.StationCode) ON Location.SampleRowID = DWE_Sample.SampleRowID) "
                   
                   "INNER JOIN BenthicCollection ON Location.LocationRowID = BenthicCollection.LocationRowID) "
                   
                   "INNER JOIN BenthicResult ON BenthicCollection.BenthicCollectionRowID = BenthicResult.BenthicCollectionRowID) "
                   
                   "INNER JOIN BenthicLabEffort ON BenthicCollection.BenthicCollectionRowID = BenthicLabEffort.BenthicCollectionRowID) "
                   
                   "INNER JOIN BenthicCollectionDetail ON BenthicCollection.BenthicCollectionRowID = BenthicCollectionDetail.BenthicCollectionRowID "
                   
                   "WHERE BenthicCollection.CollectionMethodCode Like '%s' "
                   
                   "AND BenthicResult.LastUpdateDate>'%s'" %('%bmi%',str((sccwrp_lastupdatedate2[0][0]+timedelta(days=1)).strftime("%Y-%m-%d"))))

# create a data frame from all records newer than origin_lastupdatedate
swamp_sql = swamp_engine.execute(swamp_query)
swamp = DataFrame(swamp_sql.fetchall())
swamp_engine.dispose()

# if new records are found, prepare them to be entered into sccwrp swamp_taxonomy and taxonomy
if len(swamp.index) > 0:
    swamp.columns = swamp_sql.keys()
    swamp.columns = [x.lower() for x in swamp.columns]
    # convert baresult to integer - set as string to align with ceden - and possible na's
    swamp.baresult = swamp.baresult.astype(int)
    
    # new field record_origin
    swamp['record_origin'] = pd.Series("SWAMP",index=np.arange(len(swamp)))

    # swamp_taxonomy data merge: 
    # create objectid field for merge into swamp_taxonomy (must be adjusted later when merging into taxonomy table -Jordan)
    swamp_tax_sql = "SELECT MAX(objectid) from swamp_taxonomy;"
    last_swamp_objid = sccwrp_engine.execute(swamp_tax_sql).fetchall()[0][0]
    swamp['objectid'] = swamp.index + last_swamp_objid + 1
    
    # converts the provided record publish records from SWAMP (i.e. 1/0) to strings (i.e. true/false)
    # if actual SWAMP record is "false" we never publish the record - that takes precedence over anything else
    swamp['record_publish'] = swamp.record_publish.apply(lambda x: 'true' if x else 'false')
    
    # turn off database submission temporarily - 
    eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
    status = swamp.to_sql('swamp_taxonomy', eng, if_exists='append', index=False)

    # unified taxonomy data merge: 
    # create objectid field for merge into taxonomy table (adjusting objectid field from above -Jordan)
    tax_sql = "SELECT MAX(objectid) from taxonomy;"
    last_tax_objid = sccwrp_engine.execute(tax_sql).fetchall()[0][0]
    swamp['objectid'] = swamp.index + last_tax_objid + 1

    # adjust swamp dataframe and modify record_publish field
    # if swamp record_publish is set to false never publish the record
    # if swamp record_publish is set to true then to be published it must be a station in the socal area
    # stationcode must start with a 4,8,9 all other stations are set to false
    swamp['record_publish'] = swamp.apply(lambda x: 'true' if (x.record_publish == 'true')&(x.stationcode[0] in [4,8,9]) else 'false', axis = 1)
    
    # turn off database submission temporarily - 
    eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
    status = swamp.to_sql('taxonomy', eng, if_exists='append', index=False)
    msgs.append("SWAMP Post-processing origin_lastupdate: %s\n" % swamp.origin_lastupdatedate.max().strftime("%b %d %Y"))
    msgs.append("A total of %s SWAMP records have been added.\n" % len(swamp))
    eng.dispose()
else:
    msgs.append("No new SWAMP records added this week.\n")

print msgs
## END SWAMP MERGE ##
'''









# 3RD - PROCESS CSCI
#
# ACTION: Process CSCI scores:
#     1. Do we have related gis cross walk data?
#     2. Do we have related gis metrics data?
#     3. Did the rscript process correctly?
#     4. If we fail any of the above we notify SCCWRP, otherwise the processed records are stored in six csci tables.
#
#
# DESCRIPTION:
#     After merging both smc and swamp records into the taxonomy table in the smc database, we then move on to processing the CSCI reports for that data.
#     This code concatenates both smc and swamp records into a 'bugs' dataframe. We continue to build the bugs dataframe by merging the original bugs and
#     its associated GISSTATIONCODEXWALK data. After bugs is built with its associated GIS data, we then run the CSCI function and produce the many CSCI
#     reports needed for analysis.



## CREATE BUGS AND STATIONS DATAFRAMES ##
# open log file for printing status
TIMESTAMP = str(int(round(time.time()*1000)))
logfile = '/var/www/smc/testfiles/' + TIMESTAMP + '.log'

# We can only build these dataframes if new records exist - Jordan 9/10/18
#if len(swamp.index)>0 | len(smc.index)>0:
if len(smc.index) > 0:    
    # get samplemonth, sampleday, sampleyear for later use
    smc["samplemonth"] = smc.sampledate.dt.month
    smc["sampleday"] = smc.sampledate.dt.day
    smc["sampleyear"] = smc.sampledate.dt.year
    
    # re-establish connection to smc database
    eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
    
    # in the future we will need to check to make sure these arent duplicates
    bugs = smc

    # bugs BAResult needs to be integer type
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
    gisxwalk = pd.read_sql_query(sqlwalk,eng)
    bugs = pd.merge(bugs,gisxwalk[['stationcode','giscode','databasecode']], on = ['stationcode'], how='inner')
     
    # only process stations that have associated gismetric data
    missing_bugs_xwalk = set(list_of_original_unique_stations)-set(bugs.stationcode.tolist())

    # send email if stations missing GIS Metric data
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

    # create sampleid field
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
    
    # get a unique list of sampleid
    list_of_unique_sampleid = pd.unique(bugs['sampleid'])
    unique_sampleid = ','.join("'" + s + "'" for s in list_of_unique_sampleid)
    print "list_of_unique_sampleid: %s" % str(unique_sampleid)

    #### STATIONS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISMETRICS
    sqlmetrics = 'select * from tblgismetrics'
    gismetrics = pd.read_sql_query(sqlmetrics,eng)
    test_stations = pd.unique(bugs['stationcode'])
    # merge gismetrics and gisxwalk to get giscode into dataframe
    # merge bugs/stationcode and gismetrics/giscode
    bugs['original_stationcode'] = bugs['stationcode']
    stations = pd.merge(gismetrics,bugs[['giscode','original_stationcode']], left_on = ['stationcode'], right_on = ['giscode'], how='inner') 
    # drop gismetrics stationcode
    stations.drop(['stationcode'],axis=1,inplace=True)
    stations.rename(columns={'original_stationcode': 'stationcode'}, inplace=True)
    # check stations
    test2_stations = pd.unique(stations['stationcode'])
    print test2_stations

    # Only process stations that have associated gismetric data
    missing_bugs_stations = set(list_of_unique_stations) - set(bugs.giscode.tolist())
    missing_stations_stations = set(list_of_unique_stations) - set(stations.giscode.tolist())

    # send email if stations missing GIS Metric data
    if missing_bugs_stations|missing_stations_stations:
        bad_stations = '\n'.join(str(x) for x in missing_bugs_stations.union(missing_stations_stations))
        msgs.append('The following stations are missing GIS Metric data:\n')
        msgs.append(bad_stations)
        print msgs


    # drop unnecessary columns (Note: the values needing to be dropped has seemed to change since not syncing SWAMP. removing effortqacode, locationcode, samplecomments - Jordan 2/27/19
    bugs.drop(bugs[['actualorganismcount','agencycode','benthiccollectioncomments','benthiclabeffortcomments','discardedorganismcount','extraorganismcount','grabsize','gridsanalyzed','gridsvolumeanalyzed','labsampleid','objectid','percentsamplecounted','personnelcode_labeffort','personnelcode_results','qacode','qcorganismcount','resqualcode','targetorganismcount','taxonomicqualifier','totalgrids']], axis=1, inplace=True)
    #bugs.drop(bugs[['actualorganismcount','agencycode','benthiccollectioncomments','benthiclabeffortcomments','discardedorganismcount','effortqacode','extraorganismcount','grabsize','gridsanalyzed','gridsvolumeanalyzed','labsampleid','locationcode','objectid','percentsamplecounted','personnelcode_labeffort','personnelcode_results','qacode','qcorganismcount','resqualcode','samplecomments','targetorganismcount','taxonomicqualifier','totalgrids']], axis=1, inplace=True)
    
    # if row exists drop row, errors, and lookup_error
    if 'row' in bugs.columns:
            bugs.drop(bugs[['row','errors']], axis=1, inplace=True)
    if 'lookup_error' in bugs.columns:
            bugs.drop(bugs[['lookup_error']], axis=1, inplace=True)
    # adjust 12apr19 - check for each individually otherwise script will error
    if 'objectid' in stations.columns:
            stations.drop(stations[['objectid']], axis=1, inplace=True)
    if 'gdb_geomattr_data' in stations.columns:
            stations.drop(stations[['gdb_geomattr_data']], axis=1, inplace=True)
    if 'shape' in stations.columns:
            stations.drop(stations[['shape']], axis=1, inplace=True)
    #stations.drop(stations[['objectid','gdb_geomattr_data','shape']], axis=1, inplace=True)
    stations.drop(stations[['stationcode']], axis=1, inplace=True)
    # rename field
    bugs = bugs.rename(columns={'stationcode': 'StationCode', 'sampledate': 'SampleDate', 'replicate': 'FieldReplicate', 'collectionmethodcode': 'CollectionMethodCode', 'finalid': 'FinalID', 'lifestagecode': 'LifeStageCode', 'baresult': 'BAResult', 'databasecode': 'DatabaseCode', 'sampleid': 'SampleID','distinctcode': 'Distinct','distinct': 'Distinct','fieldreplicate': 'FieldReplicate'})

    # drop station duplicates
    stations.drop_duplicates(inplace=True)

    bugs.drop(bugs[['StationCode']], axis=1, inplace=True)
    bugs.rename(columns={'giscode': 'StationCode'}, inplace=True)

    stations.rename(columns={'giscode': 'stationcode'}, inplace=True)
    
    
    ## BEGIN NEW METHOD FOR CLEANDATA AND CSCI PROCESSING ##
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

    # convert CSCI() functions from CSCI package to python
    csci = CSCI.CSCI

    # collect errors and error counts for each group
    error_count = {'CSCI': 0}
    csci_group_errors = []

    # process cleanData and CSCI for each Sample
    bugs_grouped = bugs.groupby(['SampleID'])

    # need to activate pandas2ri in order to function properly
    pandas2ri.activate()
    for name, group in bugs_grouped:
        print "group name: %s" % (name)
        bug_sample_id = name

        # get existing csci objectid - you must run this code inside the for loop in order to get the correct last_id
        last_csci_id = 0
        csci_sql = "SELECT objectid FROM csci_core WHERE objectid IN (SELECT MAX(objectid) FROM csci_core)";
        csci_result = eng.execute(csci_sql)
        for c in csci_result:
            last_csci_id = c[0]


        last_s1mmi_id = 0
        s1mmi_sql = "SELECT objectid FROM csci_suppl1_mmi WHERE objectid IN (SELECT MAX(objectid) FROM csci_suppl1_mmi)";
        s1mmi_result = eng.execute(s1mmi_sql)
        for c in s1mmi_result:
            last_s1mmi_id = c[0]

        last_s2mmi_id = 0
        s2mmi_sql = "SELECT objectid FROM csci_suppl2_mmi WHERE objectid IN (SELECT MAX(objectid) FROM csci_suppl2_mmi)";
        s2mmi_result = eng.execute(s2mmi_sql)
        for c in s2mmi_result:
            last_s2mmi_id = c[0]

        
        last_s1grps_id = 0
        s1grps_sql = "SELECT objectid FROM csci_suppl1_grps WHERE objectid IN (SELECT MAX(objectid) FROM csci_suppl1_grps)";
        s1grps_result = eng.execute(s1grps_sql)
        for c in s1grps_result:
            last_s1grps_id = c[0]

        
        last_s1oe_id = 0
        s1oe_sql = "SELECT objectid FROM csci_suppl1_oe WHERE objectid IN (SELECT MAX(objectid) FROM csci_suppl1_oe)";
        print s1oe_sql
        s1oe_result = eng.execute(s1oe_sql)
        for c in s1oe_result:
            last_s1oe_id = c[0]

        
        last_s2oe_id = 0
        s2oe_sql = "SELECT objectid FROM csci_suppl2_oe WHERE objectid IN (SELECT MAX(objectid) FROM csci_suppl2_oe)";
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
        
        # NOTE: Here we removed cleanData. According to Rafi the data we are grabbing has already been processed by clean data. -Jordan

        # Process CSCI if data is clean
        try:
            print "Attempting to process CSCI..."
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
                core_status = core.to_sql('csci_core', eng, if_exists='append', index=False)
                #print core
                
                print "second - s1mmi"
                s1mmi.columns = [x.lower() for x in s1mmi.columns] 
                s1mmi['objectid'] = last_s1mmi_id + s1mmi.reset_index().index + 1
                s1mmi['processed_by'] = "machine"
                s1mmi.rename(columns={'coleoptera_percenttaxa_predicted': 'coleoptera_percenttaxa_predict'}, inplace=True)
                s1mmi = pd.merge(s1mmi,group_copy[['sampleid','record_origin','lastupdatedate','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
                s1mmi = s1mmi.drop_duplicates()
                s1mmi_status = s1mmi.to_sql('csci_suppl1_mmi', eng, if_exists='append', index=False)
                #print s1mmi

                 
                print "third- s2mmi"
                s2mmi.columns = [x.lower() for x in s2mmi.columns]
                s2mmi['objectid'] = last_s2mmi_id + s2mmi.reset_index().index + 1
                s2mmi['processed_by'] = "machine"
                s2mmi = pd.merge(s2mmi,group_copy[['sampleid','record_origin','lastupdatedate','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
                s2mmi = s2mmi.drop_duplicates()
                s2mmi_status = s2mmi.to_sql('csci_suppl2_mmi', eng, if_exists='append', index=False)
                #print s2mmi

                print "third - s1oe"
                s1oe.columns = [x.lower() for x in s1oe.columns]
                s1oe['objectid'] = last_s1oe_id + s1oe.reset_index().index + 1
                s1oe['processed_by'] = "machine"
                s1oe = pd.merge(s1oe,group_copy[['sampleid','record_origin','lastupdatedate','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
                s1oe = s1oe.drop_duplicates()
                s1oe_status = s1oe.to_sql('csci_suppl1_oe', eng, if_exists='append', index=False)
                #print s1oe

                print "fourth - s2oe"
                s2oe.columns = [x.lower() for x in s2oe.columns]
                # fill na with -88
                s2oe['captureprob'].replace(['NA'], -88, inplace=True)
                s2oe['objectid'] = last_s2oe_id + s2oe.reset_index().index + 1
                s2oe['processed_by'] = "machine"
                s2oe = pd.merge(s2oe,group_copy[['sampleid','record_origin','lastupdatedate','origin_lastupdatedate','record_publish']], on = ['sampleid'], how='left')
                s2oe = s2oe.drop_duplicates()
                s2oe_status = s2oe.to_sql('csci_suppl2_oe', eng, if_exists='append', index=False)
                #print s2oe

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
                s1grps_status = s1grps.to_sql('csci_suppl1_grps', eng, if_exists='append', index=False)
                #print s1grps

            except Exception as e:
                print "failed to load to database"
                msgs.append(e+'\n')
                print e

        except Exception as e:
            bad_group = '\nCSCI could not be processed for sample %s for the following reason:\n' % bug_sample_id
            print bad_group
            msgs.append(bad_group)
            msgs.append(e[0]+'\n')
            error_count['CSCI'] += 1










# 4TH - STRUCTURE AND SEND EMAIL WITH SYNC & CSCI SUMMARY                        
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

send_mail(sender, [me, Paul], "SMC & Swamp Sync Summary", email_body, logfile, "localhost")
