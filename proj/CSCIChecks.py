from flask import Blueprint, request, jsonify
import pandas as pd
import json
from sqlalchemy import create_engine, exc, Table, Column, Integer, Unicode, MetaData, String, Text, update, and_, select, func, types
from .ApplicationLog import *

csci_checks = Blueprint('csci_checks', __name__)

@csci_checks.route("/csci", methods=["POST"])
def csci():
	errorLog("Function - csci")
	message = "Custom CSCI: Start checks."
	statusLog("Starting CSCI Checks")
	TIMESTAMP = current_app.timestamp
	all_dataframes = current_app.all_dataframes
	# match tablenames to tabs
	for dataframe in all_dataframes.keys():
		df_sheet_and_table_name = dataframe.strip().split(" - ")
		table_name = str(df_sheet_and_table_name[2])
		if table_name == "tbl_taxonomysampleinfo":
			sampleinfo = all_dataframes[dataframe]
			sampleinfo['tmp_row'] = sampleinfo.index
		if table_name == "tbl_taxonomyresults":
			result = all_dataframes[dataframe]
			result['tmp_row'] = result.index
	# combine results and sampleinfo on stationcode we want to get collectionmethod field from sampleinfo
	bugs = pd.merge(result,sampleinfo[['stationcode','fieldsampleid','fieldreplicate','collectionmethodcode']], on=['stationcode','fieldsampleid','fieldreplicate'], how='left')
	list_of_unique_stations = pd.unique(bugs['stationcode'])
	unique_stations = ','.join("'" + s + "'" for s in list_of_unique_stations)

	# concatenate stationcode, sampledate, collectionmethod, fieldreplicate into one field called sampleid
	bugs["sampleid"] = bugs["stationcode"] + "_" + bugs["sampledate"].dt.strftime('%m%d%Y').map(str) + "_" + bugs["collectionmethodcode"] + "_" + bugs["fieldreplicate"]

	# call gisxwalk table using unique stationcodes and get databasecode
	eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
	#sql = "select stationcode,databasecode from tmp_lu_gisstationcodexwalk"
	sql = 'select stationcode,databasecode from tmp_lu_gisstationcodexwalk where stationcode in (%s)' % unique_stations
	df_sql = pd.read_sql_query(sql,eng)
	bugs = pd.merge(bugs,df_sql[['stationcode','databasecode']], on=['stationcode'], how='left') 
	# call gismetrics table to build out stations file
	sql_stations = 'select * from tbl_gismetrics where stationcode in (%s)' % unique_stations
	df_stations = pd.read_sql_query(sql_stations,eng)
	stations = pd.merge(df_stations,bugs[['stationcode']], on=['stationcode'], how='left')

	# drop unnecessary columns
	#summary.drop('result', axis=1, inplace=True)
	bugs.drop(bugs[['fieldsampleid','unit','excludedtaxa','personnelcode_labeffort','personnelcode_results','enterdate','taxonomicqualifier','qacode','resultqualifiercode','labsampleid','benthicresultscomments','agencycode_labeffort','tmp_row','result','row','errors','lookup_error']], axis=1, inplace=True)
	stations.drop(stations[['objectid','gdb_geomattr_data','shape']], axis=1, inplace=True)
	# rename field
	bugs = bugs.rename(columns={'stationcode': 'StationCode', 'sampledate': 'SampleDate', 'fieldreplicate': 'FieldReplicate', 'collectionmethodcode': 'CollectionMethodCode', 'finalid': 'FinalID', 'lifestagecode': 'LifeStageCode', 'baresult': 'BAResult', 'databasecode': 'DatabaseCode', 'sampleid': 'SampleID','distinctcode': 'Distinct'})
	# drop all duplicates
	stations.drop_duplicates(inplace=True)

	# dump new bugs and stations dataframe to timestamp csv file location 
	# timestamp.bugs.csv
	bugs_filename = '/var/www/smc/files/' + TIMESTAMP + '.bugs.csv'
	stations_filename = '/var/www/smc/files/' + TIMESTAMP + '.stations.csv'
	bugs.to_csv(bugs_filename, sep=',', encoding='utf-8', index=False)
	stations.to_csv(stations_filename, sep=',', encoding='utf-8', index=False)

	# first we need to find out what type of custom checks we are doing
	# toxicity requires three matching tabs - batch,result,wq
	# if there arent three then bounce
	# try: call ToxicityChecks
	# try: call FishChecks
	try:
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
		path2script = '/var/www/smc/proj/rscripts/csci.R'
		args = [TIMESTAMP,bugs_filename,stations_filename]
		cmd = [command, path2script] + args
		#cmd = [command, path2script]
		x = subprocess.check_output(cmd, universal_newlines=True)
		# NEED TO ADD CODE TO CHECK IF x = true
		# IF x = true then all output files process properly
		#summary_results_link = 'http://checker.sccwrp.org/smc/logs/%s.core.csv' % TIMESTAMP
		summary_results_link = TIMESTAMP


		### IMPORTANT LOAD ONE CSCI FIELD FROM CSV FILE AND MAP IT TO EXISTING BUGS/STATIONS DATAFRAME THEN OUTPUT TO CSV LOAD FILE FOR IMPORT
		### AT STAGING INTO DATABASES

		# get filenames from fileupload routine
		#message = "Start csci checks: %s" % x	
		message = "Start csci checks:"
		errorLog(message)
		state = 0
	except ValueError:
		message = "Critical Error: Failed to run csci checks"	
		errorLog(message)
		state = 1
	return jsonify(message=message,state=state,summary_file=summary_results_link)
