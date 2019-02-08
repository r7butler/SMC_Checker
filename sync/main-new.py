#!/usr/bin/python
import os
import time, datetime
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

### Initializes Email Message
msgs = ['TAXONOMY SUMMARY:']

eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')

# get swamp demo station - already has a processed sampleid
swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where sampleid = '801CYC398_06092016_BMI_RWB_1' OR sampleid = '517PLC130_08082016_BMI_RWB_2' OR sampleid = '526PS0321_06282016_BMI_RWB_1'"

swamp_sql = eng.execute(swamp_sql_statement)
swamp = DataFrame(swamp_sql.fetchall())
swamp.columns = swamp_sql.keys()
swamp.columns = [x.lower() for x in swamp.columns]

# convert baresult to integer may not be necessary
swamp.baresult = swamp.baresult.astype(int)

# in the future we will need to check to make sure these arent duplicates
#bugs = swamp.append(smc)
bugs = swamp

# get a unique list of stations
list_of_unique_stations = pd.unique(bugs['stationcode'])
unique_stations = ','.join("'" + s + "'" for s in list_of_unique_stations)

#### BUGS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISSTATIONCODEXWALK
# BUT STATIONCODE SHOULD ACTUALLY BE GISCODE NOT STATIONCODE
# ResultsTable:StationCode links to Crosswalk:StationCode, which links to GISMetrics:GISCode
# call gisxwalk table using unique stationcodes and get databasecode and giscode
sqlwalk = 'select stationcode,databasecode,giscode from lu_gisstationcodexwalk where stationcode in (%s)' % unique_stations
gisxwalk = pd.read_sql_query(sqlwalk,eng)
bugs = pd.merge(bugs,gisxwalk[['stationcode','giscode','databasecode']], on = ['stationcode'], how='inner')
 
 #### STATIONS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISMETRICS
 sqlmetrics = 'select * from tbl_gismetrics'
 gismetrics = pd.read_sql_query(sqlmetrics,eng)
 # merge gismetrics and gisxwalk to get giscode into dataframe
 # merge bugs/stationcode and gismetrics/giscode
 stations = pd.merge(gismetrics,bugs[['giscode']], left_on = ['stationcode'], right_on = ['giscode'], how='inner') 

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

                # open log file for printing status
                TIMESTAMP = str(int(round(time.time()*1000)))
                logfile = '/var/www/smc/testfiles' + TIMESTAMP + '.log'

                # collect errors and error counts for each group
                error_count = {'clean data':0, 'CSCI': 0}
                cd_group_errors = []
                csci_group_errors = []


                # process cleanData and CSCI for each Sample
                bugs_grouped = bugs.groupby(['SampleID'])


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
                                                                                    
                                                                                        # group stationcode to get just one
                                                                                            single_station = group.StationCode.unique()
                                                                                                # to do - check to make sure there is only one but there should only be one
                                                                                                    print "stations_grouped: %s" % single_station[0]
                                                                                                        
                                                                                                            # find stationcode that matches between the bug record and what is in stations
                                                                                                                station = stations.loc[stations['stationcode'] == single_station[0]]
                                                                                                                    # convert station to r dataframe
                                                                                                                        station = pandas2ri.py2ri(station)

                                                                                                                            # checks bugs with cleanData()

                                                                                                                                # copy of group
                                                                                                                                    group_copy = group

                                                                                                                                        # make pandas dataframe to r dataframe
                                                                                                                                            group = pandas2ri.py2ri(group)
                                                                                                                                                r.str(group)
                                                                                                                                                    cd_list = cd(group,msgs=True) # msgs=True produces warning messages as strings
                                                                                                                                                        group = cd_list[0]
                                                                                                                                                            print group
                                                                                                                                                                warn_msg = cd_list[1]
                                                                                                                                                                    if warn_msg[0] != 'Data already clean':
                                                                                                                                                                                 errorLog(warn_msg)    
                                                                                                                                                                                          error_count['clean data'] += 1
                                                                                                                                                                                                   cd_group_errors.append((name,warn_msg[0]))
                                                                                                                                                                                                       else:
                                                                                                                                                                                                                    # Continue to process CSCI if data is clean
                                                                                                                                                                                                                             report = csci(group,station)
                                                                                                                                                                                                                                      # make pandas dataframe to r dataframe
                                                                                                                                                                                                                                               #report = pandas2ri.ri2py(report)
                                                                                                                                                                                                                                                        core = pandas2ri.ri2py(report[0])
                                                                                                                                                                                                                                                                 core['processed_by'] = "machine"
                                                                                                                                                                                                                                                                     core['cleaned'] = "Yes"
                                                                                                                                                                                                                                                                         core['scorenotes'] = "Distinct set to NA"
                                                                                                                                                                                                                                                                             core['rand'] = 2
                                                                                                                                                                                                                                                                                 core['scoredate'] = "2018-08-14"
                                                                                                                                                                                                                                                                                     core['objectid'] = 0
                                                                                                                                                                                                                                                                                              core.objectid = core.objectid.apply(getRandomTimeStamp)
                                                                                                                                                                                                                                                                                                       # each of five or six dataframes needs to be exported into sql database
                                                                                                                                                                                                                                                                                                         core.columns = [x.lower() for x in core.columns]
                                                                                                                                                                                                                                                                                                             group_copy.columns = [x.lower() for x in group_copy.columns]
                                                                                                                                                                                                                                                                                                                 core = pd.merge(core,group_copy[['sampleid','sampledate','collectionmethodcode','fieldreplicate']], on = ['sampleid'], how='left')
                                                                                                                                                                                                                                                                                                                     # drop duplicates - we are only processing one record at a time so this should be ok
                                                                                                                                                                                                                                                                                                                         core = core.drop_duplicates()
                                                                                                                                                                                                                                                                                                                                  status = core.to_sql('csci_core', eng, if_exists='append', index=False)
                                                                                                                                                                                                                                                                                                                                           print(status)

                                                                                                                                                                                                                                                                                                                                           end_time = int(time.time())
                                                                                                                                                                                                                                                                                                                                           elapsed_time = int((end_time - start_time)/60)
                                                                                                                                                                                                                                                                                                                                           print 'Elapsed Time: %s Minutes' % elapsed_time

