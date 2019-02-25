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
from collections import OrderedDict


#========================================================================================================================#
#       PURPOSE:
#
#           To unify existing chemistry data into a single table "chemistry" from SMC and CEDEN programs.
#           Also, to build nutrient reports for each record (stationcode,sampledate,fieldreplicate,labreplicate,sampletypecode,fraction,matrixname).
#
#
#
#
#       RESOURCES:
#
#           Existing OLDSMC
#           Server: 192.168.1.8 (portal.sccwrp.org)
#           Database: SMCPHab
#           Chemistry Tables: tblChemistryResults
#
#           Existing SWAMP
#           Database:
#           Server:
#           Chemistry Tables:
#
#           Future UNIFIEDSMC
#           Database: smc
#           Server: 192.168.1.17 (smcchecker.sccwrp.org)
#           Chemistry Tables: chemistry (unified) and swamp_chemistry
#
#========================================================================================================================#



# 1ST - INITIALIZE EMAIL MESSAGE AND COLLECT DATA FROM SMC
#
# ACTION: Get new data from OLDSMC destined for UNIFIEDSMC:
#     1. Based on difference (new records) between smc.chemistry.origin_lastupdatedate and SMCPHab.tblChemistryResults.LastChangeDate.
#     2. Modify the record and add new field record_publish set to true if record is in Southern Califoria region and is 2016 or older store in unified chemistry table.
#
#
# DESCRIPTION:
#     this code looks at the unified chemistry table in the new SMC database and checks the date of the most recent SMC records. It then compares that date to the date of the most recent
#     records in the old SMCPHab database. If it finds newer records in the SMCPHab database, it will merge those records into the smc database with new appended fields record_origin
#     and record_publish.

# Initializes Email Message
msgs = ['CHEMISTRY & SWAMP SYNC SUMMARY:\n']

# Collect SMC Data for Chemistry Table:
# 1st - connect to sccwrp chemistry and get lastupdatedate for SMC Records
sccwrp_engine = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
sccwrp_smc_chemistry_sql = sccwrp_engine.execute("select max(origin_lastupdatedate) from chemistry where record_origin = 'SMC'")
sccwrp_chemistry_lastupdatedate1 = sccwrp_smc_chemistry_sql.fetchall()
msgs.append("SCCWRP SMC Chemistry Records Last Updated On: %s\n" %sccwrp_chemistry_lastupdatedate1[0][0].strftime("%b %d %Y"))

# 2nd - Connect to smcphab tblChemistryResults
smc_engine = create_engine('mssql+pymssql://smc_inquire:$HarP*@192.168.1.8:1433/SMCPHab')
smc_chemistry_query = text(

# Paul adjusted datetime below - 5feb19
"SELECT tblChemistryResults.StationCode, tblChemistryResults.SampleDate, tblChemistryResults.SampleTypeCode, tblChemistryResults.MatrixName, tblChemistryResults.FieldReplicate, tblChemistryResults.LabReplicate, tblChemistryResults.MethodName, tblChemistryResults.AnalyteName, tblChemistryResults.FractionName, tblChemistryResults.Unit, tblChemistryResults.Result, tblChemistryResults.ResQualCode, tblChemistryResults.MDL, tblChemistryResults.RL, tblChemistryResults.DilFactor AS DilutionFactor, tblChemistryResults.QACode, tblChemistryResults.LabResultComments, tblChemistryResults.LabAgencyCode, tblChemistryResults.LastChangeDate, tblChemistryResults.ProjectCode "
        
"FROM tblChemistryResults "

"WHERE (tblChemistryResults.SampleTypeCode='Grab' Or tblChemistryResults.SampleTypeCode='Integrated') "
                                        
"AND tblChemistryResults.LastChangeDate > '%s'" % (sccwrp_chemistry_lastupdatedate1[0][0]+timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S:%f')[:-3])

smc_chemistry_sql = smc_engine.execute(smc_chemistry_query)
smc_chemistry = DataFrame(smc_chemistry_sql.fetchall())


number_of_records_in_old_database = pd.read_sql('SELECT COUNT(*) FROM tblChemistryResults', smc_engine).iloc[0][0]
number_of_records_in_new_database = pd.read_sql('SELECT COUNT(*) FROM chemistry', sccwrp_engine).iloc[0][0]

msgs.append("\nNumber of records in old database: %s\n" % number_of_records_in_old_database)
msgs.append("Number of records in new database: %s\n\n" % number_of_records_in_new_database)


# if new records present, prepare the data to be inserted in sccwrp chemistry
if len(smc_chemistry.index) > 0:
    smc_chemistry.columns = smc_chemistry_sql.keys()
    smc_chemistry.columns = [x.lower() for x in smc_chemistry.columns]
    smc_engine.dispose()
    print smc_chemistry
    eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
    
    # new field objectid
    smc_chem_sql = "SELECT MAX(objectid) from chemistry"
    last_smc_objid = eng.execute(smc_chem_sql).fetchall()[0][0]
    smc_chemistry['objectid'] = smc_chemistry.index + last_smc_objid + 1

    # new field record_origin
    #smc_chemistry['record_origin'] = pd.Series("SMC", index=np.arange(len(smc)))
    smc_chemistry['record_origin'] = pd.Series("SMC", index=np.arange(len(smc_chemistry)))
    
    # new field record_publish
    # code that sets record_publish to true only if the sampledate is before 2017-01-01 and record is in Southern California region
    exprdate = pd.Timestamp(datetime(2017,1,1))
    smc_chemistry['record_publish'] = smc_chemistry.apply(lambda x: 'true' if (x.stationcode[0] in [4,8,9])&(x.sampledate < exprdate) else 'false', axis = 1)

    # rename lastchangedate - paul 5feb19
    smc_chemistry.rename(columns={'lastchangedate': 'origin_lastupdatedate'}, inplace=True)

    # Temporarily disabled database submission
    #status = smc.to_sql('chemistry', eng, if_exists='append', index=False)
    msgs.append("SMC chemistry records last updated on: %s\n" % smc_chemistry.origin_lastupdatedate.max().strftime("%b %d %Y %X"))
    msgs.append("A total of %s new SMC chemistry records have been added.\n" % len(smc_chemistry))
else:
    msgs.append("There are no new chemistry records in SMC Database this week.\n")




# 2ND - MERGE DATA FROM SWAMP
# This code has been moved to the code archive, since SWAMP no longer allows us access to their database




# 3RD - PROCESS THE CHEMISTRY NUTRIENT REPORT

# ACTION: Process the Chemistry nutrient reports for Nitrogen and Phosphorus
#     1. Query the newly created table called "chemistry" which is in the smc database
#     2. Calculate Total Nitrogen
#     3. Calculate Total Phosphorus


# DESCRIPTION:
#     This script pulls from the chemistry table, only from SMC records, and calculates Total Nitrogen and Phosphorus at certain given stations and dates
#     records are supposed to be uniquely determined by stationcode, sampledate, fieldreplicate, labreplicate, sampletypecode, fractionname, matrixname
#     
#       The end result, called 'finaltable' will have the fields mentioned above, as well as field names as
#           1) total_n_mgl
#           2) total_n_mgl_rl
#           3) total_n_mgl_mdl
#           4) total_n_mgl_method
#           5) total_p_mgl
#           6) total_p_mgl_rl
#           7) total_p_mgl_mdl
#           8) total_p_mgl_method
#     
#     In some cases it will not be possible to calculate the Total Nitrogen or Phosphorus, so there will be certain conditions when we will apply calculation
#     methods or approximations to get the Total Nitrogen or Phosphorus. The 'method' column of the table will indicate whether the Total column reflects the
#     actual total amount, a calculated amount, or a partial amount. More details on the calculation methods and the conditions where we will use these methods
#     outlined in the subsections of this block of code. The subsections will be titled 'NITROGEN' and 'PHOSPHORUS'







## NITROGEN ##


# Each record is uniquely determined by the stationcode, sampledate, fieldreplicate, labreplicate, sampletypecode, fractionname and the matrixname

# We only want analytenames that are in ('Ammonia as N', 'Ammonia as N ', 'Nitrate + Nitrite as N', 'Nitrate as N', 'Nitrate as NO3', 'Nitrite as N', 
# 'Nitrogen, Total Kjeldahl', 'Nitrogen,Total', 'Nitrogen, Total', 'Nitrogen-Organic', 'Total Nitrogen')

# If the analytename has "Total Nitrogen" or "Nitrogen, Total" then the 'result' column is the total_n_reported.

# If that analytename is not there for that record, then we calculate total nitrogen based on other reported data

# Total Nitrogen may be calculated as:
# "Nitrogen, Total Kjeldahl" + "Nitrate + Nitrite as N"
# OR
# "Nitrogen, Total Kjeldahl" + "Nitrate as N" + "Nitrite as N"

# If the fields that are required to calculate Total Nitrogen are NOT there, then you can calculate total_n_partial

# Total N Partial would be calculated if:
# 1) "Nitrogen, Total Kjeldahl" is missing
# 2) "Nitrate + Nitrite as N" AND either "Nitrate as N" or "Nitrite as N" are missing



sccwrp_engine = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
sql_statement = text(
            "SELECT stationcode, sampledate, fieldreplicate, labreplicate, sampletypecode, fractionname, matrixname, analytename, unit, result, resqualcode, mdl, rl, qacode " 
                                
            "FROM chemistry WHERE LOWER(sampletypecode) IN ('integrated', 'grab') "
                    
            "AND record_origin = 'SMC'"
                                        
            "AND matrixname = 'samplewater' " 
                                            
            "AND analytename IN ('Ammonia as N', 'Ammonia as N ', 'Nitrate + Nitrite as N', 'Nitrate as N', 'Nitrate as NO3', 'Nitrite as N', 'Nitrogen, Total Kjeldahl', 'Nitrogen,Total', 'Nitrogen, Total', 'Nitrogen-Organic', 'Total Nitrogen')")


chemtable_sql = sccwrp_engine.execute(sql_statement)
chemtable = DataFrame(chemtable_sql.fetchall())
chemtable.columns = chemtable_sql.keys()

#Convert all negative values to zero
chemtable['result'] = chemtable.result.apply(lambda x: 0 if float(x) < 0 else float(x))
chemtable['rl'] = chemtable.rl.apply(lambda x: 0 if float(x) < 0 else float(x))
chemtable['mdl'] = chemtable.mdl.apply(lambda x: 0 if float(x) < 0 else float(x))

#Sometimes I had to group based on the fields mentioned above, and sometimes I had to group by all those fields including the analytename field
key = ['stationcode', 'sampledate', 'fieldreplicate', 'labreplicate', 'fractionname', 'sampletypecode', 'matrixname', 'analytename']
key2 = ['stationcode', 'sampledate', 'fieldreplicate', 'labreplicate', 'fractionname', 'sampletypecode', 'matrixname']

#Now these next three lines was a mechanism to deal with duplicate records, to add result values if more than one was given for the same stationcode, etc and analytename
#these lines may be able to be removed if there are no duplicates, but even if there are duplicates, these lines of code will not affect performance
results = chemtable.groupby(key)['result'].apply(sum)
rl = chemtable.groupby(key)['rl'].apply(sum)
mdl = chemtable.groupby(key)['mdl'].apply(sum)

chemtable2 = pd.concat([results, rl, mdl], axis = 1)
chemtable2 = chemtable2.reset_index()

# Since calculations had to be based on which analytenames were present, we have to associate result, rl, and mdl values with their corresponding analyte name
# "analytes" represents the analyte names, and results, rl, and mdl are the results, rl, and mdl values
# I took these lists and zipped tham into tuples which would later be converted to lists of dictionaries
analytes = chemtable2.groupby(key2)['analytename'].apply(list)
results = chemtable2.groupby(key2)['result'].apply(list)
rl = chemtable2.groupby(key2)['rl'].apply(list)
mdl = chemtable2.groupby(key2)['mdl'].apply(list)

results = zip(analytes, results)
rl = zip(analytes, rl)
mdl = zip(analytes, mdl)

for k in range(len(results)):
    results[k] = OrderedDict(zip(results[k][0], results[k][1]))

for k in range(len(results)):
    rl[k] = OrderedDict(zip(rl[k][0], rl[k][1]))

for k in range(len(results)):
    mdl[k] = OrderedDict(zip(mdl[k][0], mdl[k][1]))


# We now take these lists of dictionaries and put them into a dataframe
results = pd.Series(results)
rl = pd.Series(rl)
mdl = pd.Series(mdl)
df = DataFrame({"results": results.tolist(), "rl": rl.tolist(), "mdl": mdl.tolist() })

# ndf stands for 'nitrogen dataframe'
analytes = analytes.reset_index()
ndf = pd.concat([analytes, df], axis=1)

# initialize the final columns
ndf['total_n_mgl'] = ['--'] * len(ndf)
ndf['total_n_mgl_rl'] = ['--'] * len(ndf)
ndf['total_n_mgl_mdl'] = ['--'] * len(ndf)
ndf['total_n_mgl_method'] = ['--'] * len(ndf)


# a function that applies the appropriate calculation method to the data: partial, calculate, reported
def method(x):
    if ('Nitrogen,Total' in x['results'].keys()) | ('Nitrogen, Total' in x['results'].keys()) | ('Total Nitrogen' in x['results'].keys()):
        x['total_n_mgl_method'] = 'reported'
        x['total_n_mgl'] = x['results']['Nitrogen,Total']
        x['total_n_mgl_rl'] = x['rl']['Nitrogen,Total']
        x['total_n_mgl_mdl'] = x['mdl']['Nitrogen,Total']
    elif 'Nitrogen, Total Kjeldahl' in x['results'].keys():
        if "Nitrate + Nitrite as N" in x['results'].keys():
            x['total_n_mgl_method'] = 'calculated'
            x['total_n_mgl'] = x['results']['Nitrogen, Total Kjeldahl'] + x['results']['Nitrate + Nitrite as N']
            x['total_n_mgl_rl'] = x['rl']['Nitrogen, Total Kjeldahl'] + x['rl']['Nitrate + Nitrite as N']
            x['total_n_mgl_mdl'] = x['mdl']['Nitrogen, Total Kjeldahl'] + x['mdl']['Nitrate + Nitrite as N']
        elif ("Nitrate as N" in x['results'].keys()) and ("Nitrite as N" in x['results'].keys()):
            x['total_n_mgl_method'] = 'calculated'
            x['total_n_mgl'] = x['results']['Nitrogen, Total Kjeldahl'] + x['results']['Nitrate as N'] + x['results']['Nitrite as N']
            x['total_n_mgl_rl'] = x['rl']['Nitrogen, Total Kjeldahl'] + x['rl']['Nitrate as N'] + x['rl']['Nitrite as N']
            x['total_n_mgl_mdl'] = x['mdl']['Nitrogen, Total Kjeldahl'] + x['mdl']['Nitrate as N'] + x['mdl']['Nitrite as N']
        else:
            x['total_n_mgl_method'] = 'partial'
            x['total_n_mgl'] = sum(x['results'].values())
            x['total_n_mgl_rl'] = sum(x['rl'].values())
            x['total_n_mgl_mdl'] = sum(x['mdl'].values())

    else:
        x['total_n_mgl_method'] = 'partial'
        x['total_n_mgl'] = sum(x['results'].values())
        x['total_n_mgl_rl'] = sum(x['rl'].values())
        x['total_n_mgl_mdl'] = sum(x['mdl'].values())

    return pd.Series([x['total_n_mgl'], x['total_n_mgl_rl'], x['total_n_mgl_mdl'], x['total_n_mgl_method']])

ndf[['total_n_mgl', 'total_n_mgl_rl', 'total_n_mgl_mdl', 'total_n_mgl_method']] = ndf.apply(lambda x: method(x), axis = 1)

# drop unnecessary columns
ndf.drop(['analytename', 'results', 'rl', 'mdl'], axis = 1, inplace = True)

# This ndf table will later be merged with the pdf table
# pdf in this context will stand for 'Phosphorus DataFrame'



##    PHOSPHORUS    ##

#Each record is uniquely determined by the stationcode, sampledate, fieldreplicate, labreplicate, sampletypecode, fractionname and the matrixname

#We only want analytenames that are in 'OrthoPhosphate as P', 'Phosphate as P', 'Phosphorus as P', 'Phosphorus as PO4'

# If the analytename has "Phosphorus as P" then the 'result' column is the total_p_reported.

# If that analytename is not there for that record, then we get the partial Phosphorus measurement based on other reported data

# Partial phosphorus is the max of Phosphate as P and OrthoPhosphate as P



sql_statement2 = text(
    "SELECT stationcode, sampledate, fieldreplicate, labreplicate, sampletypecode, fractionname, matrixname, analytename, unit, result, resqualcode, mdl, rl, qacode " 
                                
    "FROM chemistry WHERE LOWER(sampletypecode) IN ('integrated', 'grab') "
                    
    "AND record_origin = 'SMC'"
                                        
    "AND matrixname = 'samplewater' " 
                                            
    "AND LOWER(analytename) IN ('orthophosphate as p', 'phosphate as p', 'phosphorus as p', 'phosphorus as po4')")


phostable_sql = sccwrp_engine.execute(sql_statement2)
phostable = DataFrame(phostable_sql.fetchall())
phostable.columns = phostable_sql.keys()

# convert negative values to zero
phostable['result'] = phostable.result.apply(lambda x: 0 if float(x) < 0 else float(x))
phostable['rl'] = phostable.rl.apply(lambda x: 0 if float(x) < 0 else float(x))
phostable['mdl'] = phostable.mdl.apply(lambda x: 0 if float(x) < 0 else float(x))

# this was a mechanism to deal with duplicates
results = phostable.groupby(key)['result'].apply(sum)
rl = phostable.groupby(key)['rl'].apply(sum)
mdl = phostable.groupby(key)['mdl'].apply(sum)


phostable2 = pd.concat([results, rl, mdl], axis = 1)
phostable2 = phostable2.reset_index()

# make lists of analytenames and results
# each result, rl, and mdl value must be associated with a given analytename
# we will zip these lists together to pair the analytename with the associated result value (also with the rl and mdl values)
# we will convert the zipped lists of tuples to lists of ordered distionaries
analytes = phostable2.groupby(key2)['analytename'].apply(list)
results = phostable2.groupby(key2)['result'].apply(list)
rl = phostable2.groupby(key2)['rl'].apply(list)
mdl = phostable2.groupby(key2)['mdl'].apply(list)

results = zip(analytes, results)
rl = zip(analytes, rl)
mdl = zip(analytes, mdl)

for k in range(len(results)):
    results[k] = OrderedDict(zip(results[k][0], results[k][1]))
            
for k in range(len(results)):
    rl[k] = OrderedDict(zip(rl[k][0], rl[k][1]))

for k in range(len(results)):
    mdl[k] = OrderedDict(zip(mdl[k][0], mdl[k][1]))


results = pd.Series(results)
rl = pd.Series(rl)
mdl = pd.Series(mdl)
df = DataFrame({"results": results.tolist(), "rl": rl.tolist(), "mdl": mdl.tolist() })

analytes = analytes.reset_index()
pdf = pd.concat([analytes, df], axis=1)

# initialize the final columns
pdf['total_p_mgl'] = ['--'] * len(pdf)
pdf['total_p_mgl_rl'] = ['--'] * len(pdf)
pdf['total_p_mgl_mdl'] = ['--'] * len(pdf)
pdf['total_p_mgl_method'] = ['--'] * len(pdf)

    
# function that will be applied to all rows to calculate the desired fields
def phos_method(x):
    # applies necessary calculations to the phosphorus table
    if 'Phosphorus as P' in x['analytename']:
        x['total_p_mgl'] = x['results']['Phosphorus as P']
        x['total_p_mgl_rl'] = x['rl']['Phosphorus as P']
        x['total_p_mgl_mdl'] = x['mdl']['Phosphorus as P']
        x['total_p_mgl_method'] = 'reported'
    else:
        x['total_p_mgl'] = max(x['results'].values())
        x['total_p_mgl_rl'] = max(x['rl'].values())
        x['total_p_mgl_mdl'] = max(x['mdl'].values())
        x['total_p_mgl_method'] = 'partial'
    return pd.Series([x['total_p_mgl'], x['total_p_mgl_rl'], x['total_p_mgl_mdl'], x['total_p_mgl_method']])


# apply appropriate calcuation method to each row (or record) in the Dataframe
pdf[['total_p_mgl', 'total_p_mgl_rl', 'total_p_mgl_mdl', 'total_p_mgl_method']] = pdf.apply(lambda x: phos_method(x), axis = 1)


# drop unnecessary fields
pdf.drop(['analytename','mdl', 'results', 'rl'], axis = 1, inplace = True)


# outer join nitrogen and phosphorus tables so that no records get deleted
finaltable = pd.merge(ndf, pdf, how = 'outer', on = key2)

# Here we will load the nutrient report into the new database in the nutrients_analyzed table
eng = create_engine("postgresql://sde:dinkum@192.168.1.17:5432/smc")
finaltable.to_sql('nutrients_analyzed', eng, if_exists='append', index=False) 

##  END CHEMISTRY NUTRIENT REPORT  ##





'''
######################################################################################################
#                      STRUCTURE AND SEND EMAIL WITH SYNC & NURIENT REPORTING                        #
######################################################################################################

# Description: Due to the inconsistency in the sync and merge of data, the email summaries vary. If no new records are found for either
#              smc or swamp, then the email will notify sccwrp of that fact. If new records are found, it will notify sccwrp of the 
#              difference in dates of the two databases in addition to the number of records it found. If records are found, the email
#              message may also notify sccwrp if the CSCI score could not be processed. 

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

#send_mail(sender, [me, Paul], "SMC & Swamp Sync Summary", email_body, logfile, "localhost")
'''






#########################################################################
# ---------                     CODE ARCHIVE                 -----------#
#########################################################################

# Removed this code from up above since SWAMP no longer allows us access to their database.
'''
# 2ND - COLLECT DATA FROM SWAMP
#
# ACTION: Get new data from SWAMP:
#     **** swamp_chemistry is a duplicate copy of the original data pulled from SWAMP the only modified field is record_publish ****
#     1. Based on difference (new records) between swamp_chemistry.origin_lastupdatedate and BenthicResult.LastUpdateDate.
#     2. Store a copy of the record in swamp_chemistry and modify record_publish from 1/0 to true/false.
#     3. Store a second copy of the record in unified chemistry table and modify record_publish based on following criteria:
#           If BenthicResult.LastUpdateDate is set to true and if the record is in Southern Califoria region set to true
#
#
# DESCRIPTION:
#     Similarly to the SMC code above, it looks at the date of the most recent SWAMP records in the swamp_taxonomy table. It then compares that date to the date of the most recent records
#     in the SWAMP database located at the STATE BOARD. If it finds newer records in the SWAMP database, it will merge those records into the swamp_taxonomy table with a new appended
#     field record_origin and a new record_publish field based on DWC_PublicRelease. Here is the criteria for whether to publish or not: "1 means that the data can be shared with the
#     public. If the value is 0, then the data should not be shared. The decision to release the data should be at the project level." We need to adjust those values from 1/0 to
#     true/false before they are stored in swamp_taxonomy table. The same data that gets stored in the swamp_taxonomy needs to get stored in the unified taxonomy table, but each records
#     record_publish need to be adjusted. Rafi only wants southern california data to# published. So any stationcode not equal to starting with 4, 8, or 9 should be set to false.
#              in the SWAMP database. If it finds newer records in the SWAMP database, it will merge those records into the smc database with a new appended field record_origin.

# first connect to sccwrp swamp_chemistry and get lastupdatedate for SWAMP Records
sccwrp_engine = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
sccwrp_swamp_sql = sccwrp_engine.execute("select max(origin_lastupdatedate) from swamp_chemistry where record_origin = 'SWAMP'")
sccwrp_lastupdatedate2 = sccwrp_swamp_sql.fetchall()
msgs.append("SWAMP Pre-processing origin_lastupdatedate: %s\n" %sccwrp_lastupdatedate2[0][0].strftime("%b %d %Y"))

# connect to swamp database using the following query
swamp_engine = create_engine('mssql+pymssql://ReadOnlySwamp:ReadOnlySwamp1@165.235.37.226:1433/DW_Full')
swamp_query = text(
"SELECT StationLookUp.StationCode, Sample.SampleDate, LabCollection.SampleTypeCode, LabResult.MatrixName, LabCollection.Replicate AS FieldReplicate, LabResult.LabReplicate, LabResult.MethodName, LabResult.AnalyteName, LabResult.FractionName, LabResult.UnitName AS Unit, LabResult.Result, LabResult.ResQualCode, LabResult.MDL, LabResult.RL, LabResult.DilutionFactor, LabResult.QACode, LabResult.LabResultComments, LabBatch.AgencyCode AS LabAgencyCode, LabResult.LastUpdateDate as origin_lastupdatedate, Sample.ProjectCode, LabResult.DWC_PublicRelease AS record_publish "

    "FROM LabBatch "

    "INNER JOIN (((Location "

    "INNER JOIN (Sample "

    "INNER JOIN StationLookUp ON Sample.StationCode = StationLookUp.StationCode) ON Location.SampleRowID = Sample.SampleRowID) "

    "INNER JOIN LabCollection ON Location.LocationRowID = LabCollection.LocationRowID) "

    "INNER JOIN LabResult ON LabCollection.LabCollectionRowID = LabResult.LabCollectionRowID) ON LabBatch.LabBatch = LabResult.LabBatch "

    "WHERE (((Sample.EventCode)='BA')) "

    "AND Sample.SampleDate>'%s' "

    "GROUP BY StationLookUp.StationCode, Sample.SampleDate, LabCollection.SampleTypeCode, LabResult.MatrixName, LabCollection.Replicate, LabResult.LabReplicate, LabResult.MethodName, LabResult.AnalyteName,LabResult.FractionName, LabResult.UnitName, LabResult.Result, LabResult.ResQualCode, LabResult.MDL, LabResult.RL, LabResult.DilutionFactor, LabResult.QACode, LabResult.LabResultComments, LabBatch.AgencyCode, LabResult.LastUpdateDate, Sample.ProjectCode, LabResult.DWC_PublicRelease "

    "HAVING ((LabCollection.SampleTypeCode)='Grab' Or (LabCollection.SampleTypeCode)='Integrated')"  %(str((sccwrp_lastupdatedate2[0][0]- timedelta(days=400)).strftime("%Y-%m-%d"))))   



# HUGE NOTE: Please look at the above substitution. You will see a subtraction of 400 days. This is for testing. It should be + timedelta(days=1)

# create a data frame from all records newer than origin_lastupdatedate
swamp_sql = swamp_engine.execute(swamp_query)
swamp = DataFrame(swamp_sql.fetchall())
swamp_engine.dispose()

# if new records are found, prepare them to be entered into sccwrp swamp_chemistry
if len(swamp.index) > 0:
    swamp.columns = swamp_sql.keys()
    swamp.columns = [x.lower() for x in swamp.columns]
    
    # convert empty result field values to -88
    swamp['result'].fillna('-88', inplace = True)

    # convert result to integer - set as string to align with ceden - and possible na's
    swamp.result = swamp.result.astype(float)
    
    # get samplemonth, sampleday, sampleyear for later use
    swamp["samplemonth"] = swamp.sampledate.dt.month
    swamp["sampleday"] = swamp.sampledate.dt.day
    swamp["sampleyear"] = swamp.sampledate.dt.year
    
    # new field record_origin
    swamp['record_origin'] = pd.Series("SWAMP",index=np.arange(len(swamp)))
    
    # SWAMP_CHEMISTRY DATA MERGE:
    # create objectid field for swamp_chemistry (must be adjusted later when merging into chemistry table - Jordan)
    swamp_chem_sql = "SELECT MAX(objectid) from swamp_chemistry;"
    last_swamp_objid = sccwrp_engine.execute(swamp_chem_sql).fetchall()[0][0]
    swamp['objectid'] = swamp.index + last_swamp_objid + 1

    # converts the provided record publish records from SWAMP (i.e. 1/0) to strings (i.e. true/false)
    # if actual SWAMP record is "false" we never publish the record - that take precedence over anything else
    swamp['record_publish'] = swamp.record_publish.apply(lambda x: 'true' if x else 'false')

    # turn off database submission temporarily -
    # eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
    # status = swamp.to_sql('swamp_chemistry', eng, if_exists='append', index=False)
    
    # UNIFIED CHEMISTRY DATA MERGE:
    # create objectid field for merge into chemistry table (adjusting objectid field from above -Jordan)
    chem_sql = "SELECT MAX(objectid) FROM chemistry;"
    last_chem_objid = sccwrp_engine.execute(chem_sql).fetchall()[0][0]
    swamp['objectid'] = swamp.index + last_chem_objid + 1
    
    # ONLY FOR TESTING:
    #swamp['objectid'] = swamp.index + len(smc) + last_chem_objid + 1

    # adjust swamp dataframe and modify record_publish field
    # if swamp record_publish is set to false never publish the record
    # if swamp record_publish is set to true then to be published it must be a station in the socal area
    # stationcode must start with a 4,8,9 all other stations are set to false
    swamp['record_publish'] = swamp.apply(lambda x: 'true' if (x.record_publish == 'true')&(x.stationcode[0] in [4,8,9]) else 'false', axis = 1)

    # turn off database submission temporarily -
    # status = swamp.to_sql('chemistry', eng, if_exists='append', index=False)
    msgs.append("SWAMP Post-processing origin_lastupdate: %s\n" % swamp.origin_lastupdatedate.max().strftime("%b %d %Y"))
    msgs.append("A total of %s SWAMP records have been added.\n" % len(swamp))
    # eng.dispose()

else:
    msgs.append("There are no new records in Swamp Database this week.\n")
'''





