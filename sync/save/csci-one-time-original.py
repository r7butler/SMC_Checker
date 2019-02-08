import os
import time, datetime
import pymssql
import pandas as pd
import random
import sys
from datetime import datetime
from sqlalchemy import create_engine
from pandas import DataFrame

gettime = int(time.time())
TIMESTAMP = str(gettime)

# get all bug records for a specific subset of data
eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
#sql = eng.execute("SELECT objectid, gdb_geomattr_data, shape, stationcode, sampledate,agencycode, replicate, sampleid, benthiccollectioncomments, grabsize,percentsamplecounted, totalgrids, gridsanalyzed, gridsvolumeanalyzed,targetorganismcount, actualorganismcount, extraorganismcount,qcorganismcount, discardedorganismcount, benthiclabeffortcomments,finalid, lifestagecode, distinctcode, baresult, resqualcode,qacode, taxonomicqualifier, personnelcode_labeffort, personnelcode_results,labsampleid, locationcode, samplecomments, collectionmethodcode,effortqacode, record_origin, origin_lastupdatedate FROM taxonomy where collectionmethodcode = 'BMI_RWB'")
sql = eng.execute("SELECT objectid, gdb_geomattr_data, shape, stationcode, sampledate,agencycode, replicate, sampleid, benthiccollectioncomments, grabsize,percentsamplecounted, totalgrids, gridsanalyzed, gridsvolumeanalyzed,targetorganismcount, actualorganismcount, extraorganismcount,qcorganismcount, discardedorganismcount, benthiclabeffortcomments,finalid, lifestagecode, distinctcode, baresult, resqualcode,qacode, taxonomicqualifier, personnelcode_labeffort, personnelcode_results,labsampleid, locationcode, samplecomments, collectionmethodcode,effortqacode, record_origin, origin_lastupdatedate FROM taxonomy where collectionmethodcode = 'BMI_CSBP_Trans'")
bugs = DataFrame(sql.fetchall())
bugs.columns = sql.keys()
bugs.columns = [x.lower() for x in bugs.columns]
eng.dispose()

def getRandomTimeStamp(row):
        print row
        row['objectid'] = int(TIMESTAMP) + int(row.name)
        return row
bugs = bugs.apply(getRandomTimeStamp, axis=1)

# get the related stations for our subset of bug records
list_of_unique_stations = pd.unique(bugs['stationcode'])
unique_stations = ','.join("'" + s + "'" for s in list_of_unique_stations)

# create sampleid field
# old way memory intensive - bugs["sampleid"] = bugs["stationcode"] + "_" + bugs["sampledate"].dt.strftime('%m%d%Y').map(str) + "_" + bugs["collectionmethodcode"] + "_" + bugs["replicate"]
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

#### we are checking to make sure that we dont have any dangling stations
# issue = bugs['giscode'][~bugs.giscode.isin(stations.stationcode.values)] 
# issue.unique()
# if there any giscode with \n remove them
bugs['giscode'] = bugs['giscode'].str.replace('\n', '')

#### STATIONS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISMETRICS
sqlmetrics = 'select * from tbl_gismetrics'
gismetrics = pd.read_sql_query(sqlmetrics,eng)
# merge gismetrics and gisxwalk to get giscode into dataframe
# merge bugs/stationcode and gismetrics/giscode
stations = pd.merge(gismetrics,bugs[['giscode']], left_on = ['stationcode'], right_on = ['giscode'], how='inner')

# drop unnecessary columns
bugs.drop(bugs[['actualorganismcount','agencycode','benthiccollectioncomments','benthiclabeffortcomments','discardedorganismcount','effortqacode','extraorganismcount','grabsize','gridsanalyzed','gridsvolumeanalyzed','labsampleid','locationcode','objectid','percentsamplecounted','personnelcode_labeffort','personnelcode_results','qacode','qcorganismcount','resqualcode','samplecomments','targetorganismcount','taxonomicqualifier','totalgrids']], axis=1, inplace=True)
# if row exists drop row, errors, and lookup_error
if 'row' in bugs.columns:
	bugs.drop(bugs[['row','errors']], axis=1, inplace=True)
if 'lookup_error' in bugs.columns:
	bugs.drop(bugs[['lookup_error']], axis=1, inplace=True)
stations.drop(stations[['objectid','gdb_geomattr_data','shape']], axis=1, inplace=True)
stations.drop(stations[['stationcode']], axis=1, inplace=True)

# rename bug fields
bugs = bugs.rename(columns={'stationcode': 'StationCode', 'sampledate': 'SampleDate', 'replicate': 'FieldReplicate', 'collectionmethodcode': 'CollectionMethodCode', 'finalid': 'FinalID', 'lifestagecode': 'LifeStageCode', 'baresult': 'BAResult', 'databasecode': 'DatabaseCode', 'sampleid': 'SampleID','distinctcode': 'Distinct','distinct': 'Distinct','fieldreplicate': 'FieldReplicate'})

# drop station duplicates
stations.drop_duplicates(inplace=True)

bugs.drop(bugs[['StationCode']], axis=1, inplace=True)
bugs.rename(columns={'giscode': 'StationCode'}, inplace=True)

stations.rename(columns={'giscode': 'stationcode'}, inplace=True)

stations_filename = '/var/www/smc/sync/' + TIMESTAMP + '.stations.csv'
# need to limit stations down to one station below - stations.to_csv(stations_filename, sep=',', encoding='utf-8', index=False)
bugs_filename = '/var/www/smc/sync/' + TIMESTAMP + '.bugs.csv'

# open log file for printing status
logfile = TIMESTAMP + '.log'
flog = open(logfile, "a")
# group stations by sampleid - this will allow us to create a bugs file based in unique sampleid and all the bugs records related to it and a station file
bugs_grouped = bugs.groupby(['SampleID'])
for name, group in bugs_grouped:
	print "group name: %s" % (name)
	#print "get station: %s" % (group.StationCode)

	group.to_csv(bugs_filename, sep=',', encoding='utf-8', index=False)

	bug_sample_id = name

	# group stationcode to get just one
	single_station = group.StationCode.unique()
	# to do - check to make sure there is only one but there should only be one
	print "stations_grouped: %s" % single_station[0]
	
	# write record to bug file
	#f = open(bugs_filename,'w')
	#bug_header = 'gdb_geomattr_data,shape,SampleDate,FieldReplicate,SampleID,FinalID,LifeStageCode,Distinct,BAResult,CollectionMethodCode,record_origin,origin_lastupdatedate,StationCode,DatabaseCode\n'
	#bug_record = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (group.gdb_geomattr_data,group.shape,group.SampleDate,group.FieldReplicate,group.SampleID,group.FinalID,group.LifeStageCode,group.Distinct,group.BAResult,group.CollectionMethodCode,group.record_origin,group.origin_lastupdatedate,group.StationCode,group.DatabaseCode)
	#f.writelines(bug_header)
	#f.writelines(bug_record)
	#f.close()
	# find stationcode that matches between the bug record and what is in stations - we only want one record
	#print "this is the station: %s" % group.StationCode

	station = stations.loc[stations['stationcode'] == single_station[0]]
	specific_station = station.stationcode.item()
	s = open(stations_filename,'w')
	station_header = 'wgtcode,database,new_lat,new_long,regionalboardnumber,psa6c,psa9c,eco_iii_1987,eco_iii_2010,eco_ii_1987,eco_ii_2010,flowstatus,ag_2000_1k,ag_2000_5k,ag_2000_ws,code_21_2000_1k,code_21_2000_5k,code_21_2000_ws,urban_2000_1k,urban_2000_5k,urban_2000_ws,roaddens_1k,roaddens_5k,roaddens_ws,paved_int_1k,paved_int_5k,paved_int_ws,permanmade_ws,invdamdist,mines_5k,gravelminedensl_r5k,elev_range,max_elev,n_mean,p_mean,pct_cenoz,pct_nosed,pct_quart,pct_sedim,pct_volcnc,ppt_00_09,temp_00_09,nhd_so,maflowu,nhdslope,ftype,nhdflow,sampled,bpj_nonref,active,area_sqkm,site_elev,cao_mean,mgo_mean,s_mean,ucs_mean,lprem_mean,atmca,atmmg,atmso4,minp_ws,meanp_ws,sumave_p,tmax_ws,xwd_ws,maxwd_ws,lst32ave,bdh_ave,kfct_ave,prmh_ave,condqr01,condqr05,condqr25,condqr50,condqr75,condqr95,condqr99,lastupdatedate,comid,sitestatus,ag_2006_1k,ag_2006_5k,ag_2006_ws,code_21_2006_1k,code_21_2006_5k,code_21_2006_ws,urban_2006_1k,urban_2006_5k,urban_2006_ws,ag_2011_1k,ag_2011_5k,ag_2011_ws,code_21_2011_1k,code_21_2011_5k,code_21_2011_ws,urban_2011_1k,urban_2011_5k,urban_2011_ws,psa10c,created_user,created_date,last_edited_user,last_edited_date,stationcode\n'
	station_record = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (station.wgtcode.item(),station.database.item(),station.new_lat.item(),station.new_long.item(),station.regionalboardnumber.item(),station.psa6c.item(),station.psa9c.item(),station.eco_iii_1987.item(),station.eco_iii_2010.item(),station.eco_ii_1987.item(),station.eco_ii_2010.item(),station.flowstatus.item(),station.ag_2000_1k.item(),station.ag_2000_5k.item(),station.ag_2000_ws.item(),station.code_21_2000_1k.item(),station.code_21_2000_5k.item(),station.code_21_2000_ws.item(),station.urban_2000_1k.item(),station.urban_2000_5k.item(),station.urban_2000_ws.item(),station.roaddens_1k.item(),station.roaddens_5k.item(),station.roaddens_ws.item(),station.paved_int_1k.item(),station.paved_int_5k.item(),station.paved_int_ws.item(),station.permanmade_ws.item(),station.invdamdist.item(),station.mines_5k.item(),station.gravelminedensl_r5k.item(),station.elev_range.item(),station.max_elev.item(),station.n_mean.item(),station.p_mean.item(),station.pct_cenoz.item(),station.pct_nosed.item(),station.pct_quart.item(),station.pct_sedim.item(),station.pct_volcnc.item(),station.ppt_00_09.item(),station.temp_00_09.item(),station.nhd_so.item(),station.maflowu.item(),station.nhdslope.item(),station.ftype.item(),station.nhdflow.item(),station.sampled.item(),station.bpj_nonref.item(),station.active.item(),station.area_sqkm.item(),station.site_elev.item(),station.cao_mean.item(),station.mgo_mean.item(),station.s_mean.item(),station.ucs_mean.item(),station.lprem_mean.item(),station.atmca.item(),station.atmmg.item(),station.atmso4.item(),station.minp_ws.item(),station.meanp_ws.item(),station.sumave_p.item(),station.tmax_ws.item(),station.xwd_ws.item(),station.maxwd_ws.item(),station.lst32ave.item(),station.bdh_ave.item(),station.kfct_ave.item(),station.prmh_ave.item(),station.condqr01.item(),station.condqr05.item(),station.condqr25.item(),station.condqr50.item(),station.condqr75.item(),station.condqr95.item(),station.condqr99.item(),station.lastupdatedate.item(),station.comid.item(),station.sitestatus.item(),station.ag_2006_1k.item(),station.ag_2006_5k.item(),station.ag_2006_ws.item(),station.code_21_2006_1k.item(),station.code_21_2006_5k.item(),station.code_21_2006_ws.item(),station.urban_2006_1k.item(),station.urban_2006_5k.item(),station.urban_2006_ws.item(),station.ag_2011_1k.item(),station.ag_2011_5k.item(),station.ag_2011_ws.item(),station.code_21_2011_1k.item(),station.code_21_2011_5k.item(),station.code_21_2011_ws.item(),station.urban_2011_1k.item(),station.urban_2011_5k.item(),station.urban_2011_ws.item(),station.psa10c.item(),station.created_user.item(),station.created_date.item(),station.last_edited_user.item(),station.last_edited_date.item(),station.stationcode.item())
	s.writelines(station_header)
	s.writelines(station_record)
	s.close()

	# run csci script with new bugs/stations csv files
	# outpute csci reports so user can download
	import subprocess
	command = 'Rscript'
	path2script = '/var/www/smc/sync/csci.R'
	args = [TIMESTAMP,bugs_filename,stations_filename]
	cmd = [command, path2script] + args

	try:
		x = subprocess.check_output(cmd, universal_newlines=True)

		core = pd.read_csv(x)
		core.columns = [x.lower() for x in core.columns]

		gettime = int(time.time())
		TIMESTAMP = str(gettime)
		core = core.apply(getRandomTimeStamp, axis=1)
		# drop index
		core.drop(core[['unnamed: 0']], axis=1, inplace=True)
		
		# try to load new csci record to the database
		try:
			status = core.to_sql('tmp_cscicore', eng, if_exists='append', index=False)
			message = "\nSuccessfully processed csci score for bug sampleid: %s and station: %s and csci score: %s" % (bug_sample_id,specific_station,core.csci.item())
			flog.writelines(message)
			print message
		except ValueError:
			message = "\nDatabase failed to load csci score for bug sampleid: %s and station: %s" % (bug_sample_id,specific_station)
			flog.writelines(message)
			print message
	except ValueError:
		message = "\nRscript failed to process csci score for bug sampleid: %s and station: %s" % (bug_sample_id,specific_station)
		flog.writelines(message)
		print message
flog.close()
