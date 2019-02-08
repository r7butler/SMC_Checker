import os, time, datetime, sys, random
import pymssql
import numpy as np
import pandas as pd
import smtplib
from datetime import datetime
from datetime import timedelta
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text
from pandas import DataFrame
from email.mime.text import MIMEText


# mimics error logging
def errorLog(x):
        print(x)


################################################################################
#                       OBTAIN DATA FROM SWAMP DATABASE                        #
################################################################################

eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
# get swamp demo station - already has a processed sampleid
swamp_sql_statement = "select objectid, swamp_taxonomy.stationcode, swamp_taxonomy.locationcode, swamp_taxonomy.sampledate, swamp_taxonomy.agencycode, swamp_taxonomy.samplecomments, swamp_taxonomy.collectionmethodcode, swamp_taxonomy.replicate, swamp_taxonomy.sampleid, swamp_taxonomy.benthiccollectioncomments, swamp_taxonomy.grabsize, swamp_taxonomy.percentsamplecounted, swamp_taxonomy.totalgrids, swamp_taxonomy.gridsanalyzed, swamp_taxonomy.gridsvolumeanalyzed, swamp_taxonomy.targetorganismcount, swamp_taxonomy.actualorganismcount, swamp_taxonomy.extraorganismcount, swamp_taxonomy.qcorganismcount, swamp_taxonomy.discardedorganismcount, swamp_taxonomy.effortqacode, swamp_taxonomy.benthiclabeffortcomments, swamp_taxonomy.finalid, swamp_taxonomy.lifestagecode, swamp_taxonomy.distinctcode, swamp_taxonomy.baresult, swamp_taxonomy.resqualcode, swamp_taxonomy.qacode, swamp_taxonomy.taxonomicqualifier, swamp_taxonomy.personnelcode_labeffort, swamp_taxonomy.personnelcode_results, swamp_taxonomy.labsampleid,swamp_taxonomy.origin_lastupdatedate, swamp_taxonomy.record_publish, swamp_taxonomy.record_origin from swamp_taxonomy where collectionmethodcode = 'BMI_RWB_MCM'"
swamp_sql = eng.execute(swamp_sql_statement)
swamp = DataFrame(swamp_sql.fetchall())
swamp.columns = swamp_sql.keys()
swamp.columns = [x.lower() for x in swamp.columns]

# create objectid row
gettime = int(round(time.time()*1000))
TIMESTAMP = str(gettime)
def getRandomTimeStamp(row):
        while True:
                obj_id = int(random.random()*10e10)
                if obj_id not in swamp.objectid.tolist():
                        row.objectid = obj_id
                        return row
                
swamp = swamp.apply(getRandomTimeStamp, axis=1)
      










################################################################################
#                       CREATE BUGS AND STATIONS DATAFRAMES                    #
################################################################################

bugs = swamp

# get a unique list of stations
list_of_unique_stations = pd.unique(bugs['stationcode'])
unique_stations = ','.join("'" + s + "'" for s in list_of_unique_stations)

# create sampleid field
bugs["samplerealdate"] = bugs["sampledate"].dt.strftime('%m%d%Y').map(str)
bugs["codeanddate"] = bugs.stationcode.astype(str).str.cat(bugs['samplerealdate'], sep='_')
bugs["collectionandreplicate"] = bugs.collectionmethodcode.astype(str).str.cat(bugs['replicate'].astype(str), sep='_')
bugs["sampleid"] = bugs.codeanddate.str.cat(bugs.collectionandreplicate, sep='_')
bugs.drop(['samplerealdate','codeanddate','collectionandreplicate'],axis=1,inplace=True)

# BUGS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISSTATIONCODEXWALK BUT STATIONCODE SHOULD ACTUALLY BE GISCODE NOT STATIONCODE (ResultsTable:StationCode links to Crosswalk:StationCode, which links to GISMetrics:GISCode)
# call gisxwalk table using unique stationcodes and get databasecode and giscode
sqlwalk = 'select stationcode,databasecode,giscode from tmp_lu_gisstationcodexwalk where stationcode in (%s)' % unique_stations
gisxwalk = pd.read_sql_query(sqlwalk,eng)
bugs = pd.merge(bugs,gisxwalk[['stationcode','giscode','databasecode']], on = ['stationcode'], how='inner')

# STATIONS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISMETRICS
sqlmetrics = 'select * from tbl_gismetrics'
gismetrics = pd.read_sql_query(sqlmetrics,eng)
# merge gismetrics and gisxwalk to get giscode into dataframe
# merge bugs/stationcode and gismetrics/giscode
stations = pd.merge(gismetrics,bugs[['giscode']], left_on = ['stationcode'], right_on = ['giscode'], how='inner') 


# drop unnecessary columns in both bugs and stations
bugs.drop(bugs[['actualorganismcount','agencycode','benthiccollectioncomments','benthiclabeffortcomments','discardedorganismcount','effortqacode','extraorganismcount','grabsize','gridsanalyzed','gridsvolumeanalyzed','labsampleid','locationcode','objectid','percentsamplecounted','personnelcode_labeffort','personnelcode_results','qacode','qcorganismcount','resqualcode','samplecomments','targetorganismcount','taxonomicqualifier','totalgrids']], axis=1, inplace=True)
# if row exists drop row, errors, and lookup_error
if 'row' in bugs.columns:
	bugs.drop(bugs[['row','errors']], axis=1, inplace=True)
if 'lookup_error' in bugs.columns:
	bugs.drop(bugs[['lookup_error']], axis=1, inplace=True)

stations.drop(stations[['objectid','gdb_geomattr_data','shape']], axis=1, inplace=True)
stations.drop(stations[['stationcode']], axis=1, inplace=True)

# rename fields in bugs
bugs = bugs.rename(columns={'stationcode': 'StationCode', 'sampledate': 'SampleDate', 'replicate': 'FieldReplicate', 'collectionmethodcode': 'CollectionMethodCode', 'finalid': 'FinalID', 'lifestagecode': 'LifeStageCode', 'baresult': 'BAResult', 'databasecode': 'DatabaseCode', 'sampleid': 'SampleID','distinctcode': 'Distinct','distinct': 'Distinct','fieldreplicate': 'FieldReplicate'})

# Change BAResult Field from strings of integers to integers -Jordan 7/27/2018
bugs.BAResult = bugs.BAResult.map(int)

# drop station duplicates
stations.drop_duplicates(inplace=True)
bugs.drop(bugs[['StationCode']], axis=1, inplace=True)
bugs.rename(columns={'giscode': 'StationCode'}, inplace=True)
stations.rename(columns={'giscode': 'stationcode'}, inplace=True)

# create csv for bugs and stations
bugs_filename = '/var/www/smc/testfiles/' + TIMESTAMP + '.bugs.csv'
stations_filename = '/var/www/smc/testfiles/' + TIMESTAMP + '.stations.csv'
#bugs.to_csv(bugs_filename, sep=',', encoding='utf-8', index=False)
#stations.to_csv(stations_filename, sep=',', encoding='utf-8', index=False)




################################################################################
#               NEW METHOD OF CLEANING DATA AND CSCI PROCESSING                #
################################################################################

import rpy2
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
import rpy2.robjects.packages as rpackages
from rpy2.robjects.packages import importr
import rpy2.rinterface as rinterface

### must be activated in order to be used below
pandas2ri.activate()

# shortens notation for accesing robjects
r = robjects.r

# import base, devtools, CSCI, and pbapply
utils = importr('utils')
base = importr('base')
CSCI = importr('CSCI')
#pbapply = importr('pbapply')

# get the cleanData() and CSCI() functions
cd = CSCI.cleanData
csci = CSCI.CSCI

# open log file for printing status
logfile = '/var/www/smc/testfiles' + TIMESTAMP + '.log'
flog = open(logfile, "a")

# group stations by sampleid - this will allow us to create a bugs file based in unique sampleid and all the bugs records related to it and a station file
#bugs = bugs[['StationCode','SampleID','FinalID','BAResult','LifeStageCode','Distinct']]

# collects errors and errors counts for each group in both clean data and CSCI
error_count = {'clean data': 0, 'CSCI': 0}
cd_group_errors = []
csci_group_errors = []


### FOR TESTING ###
#bugs = pd.DataFrame.from_csv('/var/www/smc/testfiles/WrongBugs/MissingColumns_Bugs.csv')
errorLog(bugs)

bugs_grouped = bugs.groupby(['SampleID'])
start_time = int(time.time())
for name, group in bugs_grouped:
        print "group name: %s" % (name)
        bug_sample_id = name
	
	# group file
	#group_filename = '/var/www/smc/testfiles/' + TIMESTAMP + '.group.csv'        
	#group.to_csv(group_filename, sep=',', encoding='utf-8', index=False)

	
	# group stationcode to get just one
        single_station = group.StationCode.unique()
        # to do - check to make sure there is only one but there should only be one
        print "stations_grouped: %s" % single_station[0]

        # find stationcode that matches between the bug record and what is in stations - we only want one record
        station = stations.loc[stations['stationcode'] == single_station[0]]
        specific_station = station.stationcode.item()
        s = open(stations_filename,'w')
        station_header = 'wgtcode,database,new_lat,new_long,regionalboardnumber,psa6c,psa9c,eco_iii_1987,eco_iii_2010,eco_ii_1987,eco_ii_2010,flowstatus,ag_2000_1k,ag_2000_5k,ag_2000_ws,code_21_2000_1k,code_21_2000_5k,code_21_2000_ws,urban_2000_1k,urban_2000_5k,urban_2000_ws,roaddens_1k,roaddens_5k,roaddens_ws,paved_int_1k,paved_int_5k,paved_int_ws,permanmade_ws,invdamdist,mines_5k,gravelminedensl_r5k,elev_range,max_elev,n_mean,p_mean,pct_cenoz,pct_nosed,pct_quart,pct_sedim,pct_volcnc,ppt_00_09,temp_00_09,nhd_so,maflowu,nhdslope,ftype,nhdflow,sampled,bpj_nonref,active,area_sqkm,site_elev,cao_mean,mgo_mean,s_mean,ucs_mean,lprem_mean,atmca,atmmg,atmso4,minp_ws,meanp_ws,sumave_p,tmax_ws,xwd_ws,maxwd_ws,lst32ave,bdh_ave,kfct_ave,prmh_ave,condqr01,condqr05,condqr25,condqr50,condqr75,condqr95,condqr99,lastupdatedate,comid,sitestatus,ag_2006_1k,ag_2006_5k,ag_2006_ws,code_21_2006_1k,code_21_2006_5k,code_21_2006_ws,urban_2006_1k,urban_2006_5k,urban_2006_ws,ag_2011_1k,ag_2011_5k,ag_2011_ws,code_21_2011_1k,code_21_2011_5k,code_21_2011_ws,urban_2011_1k,urban_2011_5k,urban_2011_ws,psa10c,created_user,created_date,last_edited_user,last_edited_date,stationcode\n'
        station_record = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (station.wgtcode.item(),station.database.item(),station.new_lat.item(),station.new_long.item(),station.regionalboardnumber.item(),station.psa6c.item(),station.psa9c.item(),station.eco_iii_1987.item(),station.eco_iii_2010.item(),station.eco_ii_1987.item(),station.eco_ii_2010.item(),station.flowstatus.item(),station.ag_2000_1k.item(),station.ag_2000_5k.item(),station.ag_2000_ws.item(),station.code_21_2000_1k.item(),station.code_21_2000_5k.item(),station.code_21_2000_ws.item(),station.urban_2000_1k.item(),station.urban_2000_5k.item(),station.urban_2000_ws.item(),station.roaddens_1k.item(),station.roaddens_5k.item(),station.roaddens_ws.item(),station.paved_int_1k.item(),station.paved_int_5k.item(),station.paved_int_ws.item(),station.permanmade_ws.item(),station.invdamdist.item(),station.mines_5k.item(),station.gravelminedensl_r5k.item(),station.elev_range.item(),station.max_elev.item(),station.n_mean.item(),station.p_mean.item(),station.pct_cenoz.item(),station.pct_nosed.item(),station.pct_quart.item(),station.pct_sedim.item(),station.pct_volcnc.item(),station.ppt_00_09.item(),station.temp_00_09.item(),station.nhd_so.item(),station.maflowu.item(),station.nhdslope.item(),station.ftype.item(),station.nhdflow.item(),station.sampled.item(),station.bpj_nonref.item(),station.active.item(),station.area_sqkm.item(),station.site_elev.item(),station.cao_mean.item(),station.mgo_mean.item(),station.s_mean.item(),station.ucs_mean.item(),station.lprem_mean.item(),station.atmca.item(),station.atmmg.item(),station.atmso4.item(),station.minp_ws.item(),station.meanp_ws.item(),station.sumave_p.item(),station.tmax_ws.item(),station.xwd_ws.item(),station.maxwd_ws.item(),station.lst32ave.item(),station.bdh_ave.item(),station.kfct_ave.item(),station.prmh_ave.item(),station.condqr01.item(),station.condqr05.item(),station.condqr25.item(),station.condqr50.item(),station.condqr75.item(),station.condqr95.item(),station.condqr99.item(),station.lastupdatedate.item(),station.comid.item(),station.sitestatus.item(),station.ag_2006_1k.item(),station.ag_2006_5k.item(),station.ag_2006_ws.item(),station.code_21_2006_1k.item(),station.code_21_2006_5k.item(),station.code_21_2006_ws.item(),station.urban_2006_1k.item(),station.urban_2006_5k.item(),station.urban_2006_ws.item(),station.ag_2011_1k.item(),station.ag_2011_5k.item(),station.ag_2011_ws.item(),station.code_21_2011_1k.item(),station.code_21_2011_5k.item(),station.code_21_2011_ws.item(),station.urban_2011_1k.item(),station.urban_2011_5k.item(),station.urban_2011_ws.item(),station.psa10c.item(),station.created_user.item(),station.created_date.item(),station.last_edited_user.item(),station.last_edited_date.item(),station.stationcode.item())
        s.writelines(station_header)
        s.writelines(station_record)
        s.close()
	
	#station = station[['stationcode','area_sqkm','new_lat','new_long','site_elev','ppt_00_09','temp_00_09','sumave_p','kfct_ave','bdh_ave','p_mean','elev_range']]
        #rinterface.globalenv['station'] = pandas2ri.py2ri(station)
	
	# checks bugs with cleanData()
	cd_list = cd(group,msgs=True) # msgs=True produces warning messages as strings
	group = cd_list[0]
        warn_msg = cd_list[1]
        if warn_msg[0] != 'Data already clean':
            errorLog(warn_msg)    
            error_count['clean data'] += 1
            cd_group_errors.append((name,warn_msg[0]))
	
        # Continue to process CSCI if data is clean
        report = csci(group,station)

end_time = int(time.time())
elapsed_time = int((end_time - start_time)/60)
print 'Elapsed Time: %s Minutes' % elapsed_time
