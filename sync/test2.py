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
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate
from os.path import basename

### Initializes Email Message
msgs = []

eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
# get swamp demo station - already has a processed sampleid
swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where sampleid = '801CYC398_06092016_BMI_RWB_1' OR sampleid = '517PLC130_08082016_BMI_RWB_2'"

swamp_sql = eng.execute(swamp_sql_statement)
swamp = DataFrame(swamp_sql.fetchall())
swamp.columns = swamp_sql.keys()
swamp.columns = [x.lower() for x in swamp.columns]

# convert baresult to integer may not be necessary
swamp.baresult = swamp.baresult.astype(int)

# in the future we will need to check to make sure these arent duplicates
#bugs = swamp.append(smc)
bugs = swamp

eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
# get swamp demo station - already has a processed sampleid
swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where sampleid = '801CYC398_06092016_BMI_RWB_1' OR sampleid = '517PLC130_08082016_BMI_RWB_2'"

swamp_sql = eng.execute(swamp_sql_statement)
swamp = DataFrame(swamp_sql.fetchall())
swamp.columns = swamp_sql.keys()
swamp.columns = [x.lower() for x in swamp.columns]

# convert baresult to integer may not be necessary
swamp.baresult = swamp.baresult.astype(int)

# in the future we will need to check to make sure these arent duplicates
#bugs = swamp.append(smc)
bugs = swamp

eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')

# get swamp demo station - already has a processed sampleid
swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where sampleid = '801CYC398_06092016_BMI_RWB_1' OR sampleid = '517PLC130_08082016_BMI_RWB_2'"

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
sqlwalk = 'select stationcode,databasecode,giscode from tmp_lu_gisstationcodexwalk where stationcode in (%s)' % unique_stations
gisxwalk = pd.read_sql_query(sqlwalk,eng)
bugs = pd.merge(bugs,gisxwalk[['stationcode','giscode','databasecode']], on = ['stationcode'], how='inner')
 
#### STATIONS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISMETRICS
sqlmetrics = 'select * from tbl_gismetrics'
gismetrics = pd.read_sql_query(sqlmetrics,eng)
# merge gismetrics and gisxwalk to get giscode into dataframe
# merge bugs/stationcode and gismetrics/giscode
stations = pd.merge(gismetrics,bugs[['giscode']], left_on = ['stationcode'], right_on = ['giscode'], how='inner') 

# Only process stations that have associated gismetric data.
missing_bugs_stations = set(list_of_unique_stations)-set(bugs.stationcode.tolist())
missing_stations_stations = set(list_of_unique_stations)-set(stations.stationcode.tolist())

# send email if stations missing GIS Metric data
if missing_bugs_stations|missing_stations_stations:
    bad_stations = '\n'.join(str(x) for x in missing_bugs_stations.union(missing_stations_stations))
    msgs.append('The following stations are missing GIS Metric data:\n')
    msgs.append(bad_stations)
    print msgs


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

stations.stationcode[0] = 'hi'
###################################################################################################################
#                               BEGIN NEW METHOD FOR CLEANDATA AND CSCI PROCESSING                                #
###################################################################################################################

import rpy2
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
import rpy2.robjects.packages as rpackages
from rpy2.robjects.packages import importr
import rpy2.rinterface as rinterface

# function that creates objectid field
def getRandomTimeStamp(row):
    while True:
        obj_id = int(random.random()*10e7)
        if obj_id not in swamp.objectid.tolist():
            row = obj_id
            return row

# shortens notation for accessing robjects
r = robjects.r

# import R package: CSCI
CSCI = importr('CSCI')

# convert  cleanData() and CSCI() functions from CSCI package to python
cd = CSCI.cleanData
csci = CSCI.CSCI

# open log file for printing status
TIMESTAMP = str(int(round(time.time()*1000)))
logfile = '/var/www/smc/testfiles/' + TIMESTAMP + '.log'


# collect errors and error counts for each group
error_count = {'clean data':0, 'CSCI': 0}
cd_group_errors = []
csci_group_errors = []


# process cleanData and CSCI for each Sample
bugs_grouped = bugs.groupby(['SampleID'])

# Need this for R to Python DataFrame conversions
pandas2ri.activate()

start_time = int(time.time())
for name, group in bugs_grouped:
    print "group name: %s" % (name)
    bug_sample_id = name
                                                        
    #group stationcode to get just one
    single_station = group.StationCode.unique()
    # to do - check to make sure there is only one but there should only be one
    print "stations_grouped: %s" % single_station[0]
                                                                          
    # find stationcode that matches between the bug record and what is in stations
    station = stations.loc[stations['stationcode'] == single_station[0]]
    # convert station to r dataframe
    station = pandas2ri.py2ri(station)

    # checks bugs with cleanData()

    # make pandas dataframe to r dataframe
    group = pandas2ri.py2ri(group)
    cd_list = cd(group,msgs=True) # msgs=True produces warning messages as strings
    group = cd_list[0]
    
    warn_msg = cd_list[1]
    
    if warn_msg[0] != 'Data already clean':
        errorLog(warn_msg)
        msgs.append(warn_msg[0]+'\n')
        error_count['clean data'] += 1
        cd_group_errors.append((name,warn_msg[0]))
    else:
        # Continue to process CSCI if data is clean
        try:
            report = csci(group,station)
            # make pandas dataframe to r dataframe
            #report = pandas2ri.ri2py(report)
            core = pandas2ri.ri2py(report[0])
            core['processed_by'] = "machine"
            core['objectid'] = 0
            core['objectid'] = core['objectid'].apply(getRandomTimeStamp)
            # each of five or six dataframes needs to be exported into sql database
            core.columns = [x.lower() for x in core.columns]
            #status = core.to_sql('csci_core', eng, if_exists='append', index=False)
            #print(status)
        except Exception as e:
            bad_group = '\nCSCI could not be processed for sample %s for the following reason:\n' % bug_sample_id
            msgs.append(bad_group)
            msgs.append(e[0]+'\n')


end_time = int(time.time())
elapsed_time = int((end_time - start_time)/60)
print 'Elapsed Time: %s Minutes' % elapsed_time

# declare sender and recipients of email
sender = 'test@checker.sccwrp.org'
me = 'r7butler@yahoo.com'
Paul = 'pauls@sccwrp.org'
IT_help = 'it-help@sccwrp.org'

'''
# Structures and sends email message
email_msg = ''.join(msgs)
print email_msg
msg = MIMEText(email_msg,'plain')
msg['From'] = sender
msg['To'] = me # replace Paul with whoever you want to send email to.
msg['Subject'] = 'IT WORKS! CSCI Processing Summary'

# sends email
s = smtplib.SMTP('localhost')
s.sendmail(sender,me,msg.as_string()) # replace Paul with whoever you want to send email to.
s.quit()
'''

## copied code from stack overflow is below
## it works for sending emails with attachments
## url for the post: https://stackoverflow.com/questions/3362600/how-to-send-email-attachments

## I needed to import new packages to make it work.
## from os.path import basename
## from email.mime.application import MIMEApplication
## from email.MIME.multipart import MIMEMultipart
## from email.utils import COMMASPACE, formatdate

## Another thing is that the "send_to" argument and the "files" argument MUST be lists! Not sure exactly why


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
