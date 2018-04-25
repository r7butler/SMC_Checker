from flask import Blueprint, request, jsonify, session
import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine, exc, Table, Column, Integer, Unicode, MetaData, String, Text, update, and_, select, func, types
from pandas import DataFrame
from .ApplicationLog import *

def addErrorToList(error_column, row, error_to_add,df):
	df.ix[int(row), 'row'] = str(row)
	if error_column in df.columns:
		# check if cell value is empty (nan) 
		if(pd.isnull(df.ix[int(row), error_column])):
			# no data exists in cell so add error
	      		df.ix[int(row), error_column] = error_to_add
			errorLog("addErrorToList New Row: %s, Error To Add: %s" % (int(row),error_to_add))
		else:
			# a previous error was recorded so append to it
			# even though there may be data lets check to make sure it is not empty
			if str(df.ix[int(row), error_column]):
				#print("There is already a previous error recorded: %s" % str(df.ix[int(row), error_column]))
				df.ix[int(row), error_column] = str(df.ix[int(row), error_column]) + "," + error_to_add
				errorLog("addErrorToList Existing Row: %s, Error To Add: %s" % (int(row),error_to_add))
			else:
				#print("No error is recorded: %s" % str(df.ix[int(row), error_column]))
	      			df.ix[int(row), error_column] = error_to_add
				errorLog("addErrorToList Row: %s, Error To Add: %s" % (int(row),error_to_add))
	else:
		df.ix[int(row), error_column] = error_to_add
		errorLog("addErrorToList Add New Column and New Row: %s, Error To Add: %s" % (int(row),error_to_add))
	return df

def getCustomErrors(df,name,warn_or_error):
	errorLog("start getCustomErrors")
	errorLog(name)
	# get name of dataframe
	tab_value = name.strip().split(" - ")
	tab = tab_value[0]
	# get tab number of dataframe batch = 0, result = 1, wq = 2
	#errorLog(tab[0])
	#errorLog(list(df))
	#  clear dataframe of rows that have no errors
	dfjson = df
        dfjson = dfjson[pd.notnull(dfjson[warn_or_error])]
	#errorLog(dfjson)
        #dfjson = dfjson[pd.notnull(dfjson['toxicity_errors'])]
        # must re-index dataframe - set to 0 after removing rows
	# not necessary with custom errors only regular
	#dfjson.reset_index(drop=True,inplace=True) 
	tmp_dict = {}
	count = 0
	# Critical for custom checks we look at row instead of tmp_row
	# something must be wrong with code row seems to work with toxicity and summary checks, but tmp_row fails to work properly with summary (duplicates)
	for index, row in dfjson.iterrows():
		# delete - errorLog("count: %s - row: %s value: %s" % (count,row['tmp_row'],row['custom_errors']))
		# delete - tmp_dict[tab] = '[{"count":"%s","row":"%s","value":[%s]}]' % (count,row['tmp_row'],row['custom_errors'])
		tabcount = tab + "-" + str(count)
		tmp_dict[tabcount] = '[{"row":"%s","value":[%s]}]' % (row['row'],row[warn_or_error])
		errorLog("row: %s, value: %s" % (row['row'],row[warn_or_error]))
		# delete - tmp_dict[tabcount] = '[{"row":"%s","value":[%s]}]' % (row['tmp_row'],row['custom_errors'])
		count = count + 1
	errorLog("end getCustomErrors")
	return tmp_dict

def getCustomRedundantErrors(df,name,check):
	errorLog("start getCustomRedundantErrors")
	#errorLog("check: %s" % check)
	# get name of dataframe
	tab_value = name.strip().split(" - ")
	# get tab number of dataframe batch = 0, result = 1, wq = 2
	tab = tab_value[0]
	tmp_dict = {}
	count = 0
	for error_message,group in df.groupby(check):
		# only return errors if there are more one (redundant)
		#errorLog("grouped rows count: %s" % len(group.row))
		if len(group.row) > 1:
			row_fix = []
			for r in group.row:
				row_fix.append(str(int(r) + 2))
			rows = ', '.join(row_fix)
			errorLog('[{"rows":"%s","value":[%s]}]' % (rows,error_message))
			tabcount = tab + "-" + str(count)
			tmp_dict[tabcount] = '[{"rows":"%s","value":[%s]}]' % (rows,error_message)
		count = count + 1
	errorLog("end getCustomRedundantErrors")
	return tmp_dict

def dcValueAgainstMultipleValues(eng,dbtable,dbfield,df,field):
        # codes_df: dataframe of valid codes according to database
        codes = eng.execute("select " + dbfield + " from " + dbtable +";")
        codes_df = pd.DataFrame(codes.fetchall())
        codes_df.columns = codes.keys()
        
        # subcodes: submitted codes for specified field
        subcodes = df[[field,'tmp_row']]
	
        # check submitted data for at least one code
        nan_rows = subcodes.loc[subcodes[field]==''].tmp_row.tolist()
	
        # check submitted data for invalid codes
	db_list = codes_df[dbfield].apply(lambda row: "".join(row.split()).lower()).tolist()
        subcodes['check'] = subcodes[field][subcodes[field] != ""].apply(lambda row: set("".join(row.split()).lower().split(',')).issubset(db_list))
        invalid_codes = df.loc[subcodes.check == False].tmp_row.tolist()
	return nan_rows, invalid_codes, subcodes

taxonomy_checks = Blueprint('taxonomy_checks', __name__)

@taxonomy_checks.route("/taxonomy", methods=["POST"])

def taxonomy(all_dataframes,sql_match_tables,errors_dict,project_code,login_info):
	errorLog("Function - taxonomy")
	message = "Custom Taxonomy: Start checks."
	statusLog("Starting Taxonomy Checks")
	errorLog(message)
	errorLog("project code: %s" % project_code)
	login_info = login_info.strip().split("-")
	login = str(login_info[0])
	agency = str(login_info[1])
	owner = str(login_info[2])
	year = str(login_info[3])
	project = str(login_info[4])

        assignment_table = ""
        custom_checks = ""
        summary_checks = ""
        summary_results_link = ""
        custom_redundant_checks = ""
        custom_errors = []
        custom_warnings = []
        custom_redundant_errors = []
        custom_redundant_warnings = []

	TIMESTAMP=str(session.get('key'))
	# add submitted table names to list
	tables = []
	# match tablenames to tabs
	errorLog(all_dataframes.keys())
	for dataframe in all_dataframes.keys():
		df_sheet_and_table_name = dataframe.strip().split(" - ")
		errorLog(df_sheet_and_table_name)
		table_name = str(df_sheet_and_table_name[2])
		errorLog(table_name)
		if table_name == "tbl_taxonomysampleinfo":
			tables.append("sampleinfo")
			sampleinfo = all_dataframes[dataframe]
			sampleinfo['tmp_row'] = sampleinfo.index
		if table_name == "tbl_taxonomyresults":
			tables.append("result")
			result = all_dataframes[dataframe]
			result['tmp_row'] = result.index

	try:
		## CHECKS ##
		def checkData(statement,column,warn_or_error,error_label,human_error,dataframe):
			errorLog("checkData warn_or_error: %s" % error_label)
			for item_number in statement:
				unique_error = '{"column": "%s", "error_type": "%s", "error": "%s"}' % (column,warn_or_error,human_error)
				if error_label == 'error':
					addErrorToList("custom_errors",item_number,unique_error,dataframe)
					errorsCount(errors_dict,'custom')
				if error_label == 'warning':
					addErrorToList("custom_errors",item_number,unique_error,dataframe)
					# do not count warnings as errors - submission allowed - errorsCount('custom')
		def checkLogic(statement,column,warn_or_error,error_label,human_error,dataframe):
			for item_number in statement:
				unique_error = '{"column": "%s", "error_type": "%s", "error": "%s"}' % (column,warn_or_error,human_error)
				addErrorToList("custom_errors",item_number,unique_error,dataframe)
				errorsCount(errors_dict,'custom')

		## LOGIC ##
		message = "Starting Taxonomy Logic Checks"
		errorLog(message)
		statusLog(message)
		# EACH SAMPLEINFO INFORMATION RECORD MUST HAVE A CORRESPONDING RESULT RECORD. RECORDS ARE MATCHED ON STATIONCODE, SAMPLEDATE, FIELDREPLICATE.
		errorLog("## EACH SAMPLEINFO INFORMATION RECORD MUST HAVE A CORRESPONDING RESULT RECORD. RECORDS ARE MATCHED ON STATIONCODE, SAMPLEDATE, FIELDREPLICATE ##") 
		errorLog(sampleinfo[~sampleinfo[['stationcode','sampledate','fieldreplicate']].isin(result[['stationcode','sampledate','fieldreplicate']].to_dict(orient='list')).all(axis=1)])
		checkLogic(sampleinfo[~sampleinfo[['stationcode','sampledate','fieldreplicate']].isin(result[['stationcode','sampledate','fieldreplicate']].to_dict(orient='l    ist')).all(axis=1)].index.tolist(),'StationCode/SampleDate/FieldReplicate','Logic Error','error','Each Taxonomy SampleInfo record must have a corresponding Taxonomy Result record. Records are matched on StationCode,SampleDate, and FieldReplicate.',sampleinfo)
		errorLog(result[~result[['stationcode','sampledate','fieldreplicate']].isin(sampleinfo[['stationcode','sampledate','fieldreplicate']].to_dict(orient='list')).all(axis=1)])
		checkLogic(result[~result[['stationcode','sampledate','fieldreplicate']].isin(sampleinfo[['stationcode','sampledate','fieldreplicate']].to_dict(orient='l    ist')).all(axis=1)].index.tolist(),'StationCode/SampleDate/FieldReplicate','Logic Error','error','Each Taxonomy Result record must have a corresponding Taxonomy SampleInfo record. Records are matched on StationCode,SampleDate, and FieldReplicate.',result)
		## END LOGIC CHECKS ##

		## CUSTOM CHECKS ##
		## Jordan - Taxonomicqualifier Multi Value Lookup List: check to make sure taxonomicqualifier field data is valid (multiple values may be accepted).
		errorLog(result['taxonomicqualifier'])
		errorLog("Taxonomicqualifier Multi Value Lookup List: check to make sure taxonomicqualifier field data is valid (multiple values may be accepted).")
		nan_rows, invalid_codes, subcodes  = dcValueAgainstMultipleValues(current_app.eng,'lu_taxonomicqualifier','taxonomicqualifiercode',result,'taxonomicqualifier')
		errorLog("Check submitted data for at least one code:")
		checkData(nan_rows,'taxonomicqualifier','custom error','error','At least one taxonomicqualifier code required please check the list: <a href=http://checker.sccwrp.org/smc/scraper?action=help&layer=lu_taxonomicqualifier target=_blank>qa list</a>.',result)
		errorLog("Check submitted data for invalid code (or code combination):")
		checkData(invalid_codes,'taxonomicqualifier','custom error','error','At least one Taxonomic Qualifier code is invalid please check the list: <a href=http://checker.sccwrp.org/smc/scraper?action=help&layer=lu_taxonomicqualifier target=_blank>qa list</a>',result)

		## Jordan -  Sample/Result SampleDate field - make sure user did not accidentally drag down date
                errorLog('Sample/Result SampleDate field - make sure user did not accidentally drag down date')
		# If every date submitted is consecutive from the first, it will error out every row. Otherwise, no error is thrown.
		if sampleinfo.sampledate.diff()[1:].sum() == pd.Timedelta('%s day' %(len(sampleinfo)-1)):
			checkData(sampleinfo.loc[sampleinfo.sampledate.diff() == pd.Timedelta('1 day')].tmp_row.tolist(),'SampleDate','Custom Error','Error','Consecutive Dates. Make sure you did not accidentally drag down the date',sampleinfo)

		if result.sampledate.diff()[1:].sum() == pd.Timedelta('%s day' %(len(result)-1)):
			checkData(result.loc[result.sampledate.diff() == pd.Timedelta('1 day')].tmp_row.tolist(),'SampleDate','Custom Error','Error','Consecutive Dates. Make sure you did not accidentally drag down the date',result)
		## END CUSTOM CHECKS ##
		## START MAP CHECK ##
		# get a unique list of stations from results file
		rlist_of_stations = pd.unique(result['stationcode'])
		result_unique_stations = ','.join("'" + s + "'" for s in rlist_of_stations)
		## END MAP CHECKS
		## END CHECKS ##

		## NEW FIELDS ##
		sampleinfo['project_code'] = project_code
		result['project_code'] = project_code
		## END NEW FIELDS ##

		## ONCE DATA PASSES CHECKS - BUILD CSCI ##
		## failure to run csci should not result in a failure to submit data - csci status should always = 0
		try:
			# dont run csci code if there are custom errors - data must be clean
			total_count = errors_dict['total']
			errorLog("total error count: %s" % total_count)
			errorLog("project code: %s" % project_code)
			if total_count == 0:
				### START CSCI Code - SHOULD NOT RUN UNTIL AFTER ALL CHECKS####
				errorLog("# START CSCI Code #")
				# combine results and sampleinfo on stationcode we want to get collectionmethod field from sampleinfo
				bugs = pd.merge(result,sampleinfo[['stationcode','fieldsampleid','fieldreplicate','collectionmethodcode']], on=['stationcode','fieldsampleid','fieldreplicate'], how='left')
				list_of_unique_stations = pd.unique(bugs['stationcode'])
				errorLog("list_of_unique_stations:")
				errorLog(list_of_unique_stations)
				unique_stations = ','.join("'" + s + "'" for s in list_of_unique_stations)
				errorLog("unique_stations:")
				errorLog(unique_stations)

				# concatenate stationcode, sampledate, collectionmethod, fieldreplicate into one field called sampleid
				#errorLog("create sampleid:")
				#bugs["sampleid"] = bugs["stationcode"] + "_" + bugs["sampledate"].dt.strftime('%m%d%Y').map(str) + "_" + bugs["collectionmethodcode"] + "_" + bugs["fieldreplicate"]
				# first get adjusted date
				bugs["samplerealdate"] = bugs["sampledate"].dt.strftime('%m%d%Y').map(str)
				# merge two
				bugs["codeanddate"] = bugs.stationcode.astype(str).str.cat(bugs['samplerealdate'], sep='_')
				# merge two
				bugs["collectionandreplicate"] = bugs.collectionmethodcode.astype(str).str.cat(bugs['fieldreplicate'].astype(str), sep='_')
				# merge both
				bugs["sampleid"] = bugs.codeanddate.str.cat(bugs.collectionandreplicate, sep='_')
				# drop temp columns
				bugs.drop(['samplerealdate','codeanddate','collectionandreplicate'],axis=1,inplace=True)

				#### BUGS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISSTATIONCODEXWALK
				# BUT STATIONCODE SHOULD ACTUALLY BE GISCODE NOT STATIONCODE
				# ResultsTable:StationCode links to Crosswalk:StationCode, which links to GISMetrics:GISCode
				# call gisxwalk table using unique stationcodes and get databasecode and giscode
				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
				sqlwalk = 'select stationcode,databasecode,giscode from tmp_lu_gisstationcodexwalk where stationcode in (%s)' % unique_stations
				gisxwalk = pd.read_sql_query(sqlwalk,eng)

				##### can the stations that the user submitted be found in the delineated table
				errorLog("can the stations that the user submitted be found in the delineated table:")
				delineated_stations_count = len(gisxwalk.index)
				errorLog(delineated_stations_count)	

				## dont run rest of code if we cant delineate station
				if delineated_stations_count:
					bugs = pd.merge(bugs,gisxwalk[['stationcode','giscode','databasecode']], on = ['stationcode'], how='inner')

					#### STATIONS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISMETRICS
					errorLog("building gismetrics...")
					sqlmetrics = 'select * from tbl_gismetrics'
					gismetrics = pd.read_sql_query(sqlmetrics,eng)
					# merge gismetrics and gisxwalk to get giscode into dataframe
					# merge bugs/stationcode and gismetrics/giscode
					stations = pd.merge(gismetrics,bugs[['giscode']], left_on = ['stationcode'], right_on = ['giscode'], how='inner')
					eng.dispose()

					# drop unnecessary columns
					#summary.drop('result', axis=1, inplace=True)
					bugs.drop(bugs[['fieldsampleid','unit','excludedtaxa','personnelcode_labeffort','personnelcode_results','enterdate','taxonomicqualifier','qacode','resqualcode','labsampleid','benthicresultscomments','agencycode_labeffort','tmp_row','result']], axis=1, inplace=True)
					# if row exists drop row, errors, and lookup_error
					if 'row' in bugs.columns:
						bugs.drop(bugs[['row','errors']], axis=1, inplace=True)
					if 'lookup_error' in bugs.columns:
						bugs.drop(bugs[['lookup_error']], axis=1, inplace=True)
					stations.drop(stations[['objectid','gdb_geomattr_data','shape']], axis=1, inplace=True)
					# rename field
					bugs = bugs.rename(columns={'stationcode': 'StationCode', 'sampledate': 'SampleDate', 'fieldreplicate': 'FieldReplicate', 'collectionmethodcode': 'CollectionMethodCode', 'finalid': 'FinalID', 'lifestagecode': 'LifeStageCode', 'baresult': 'BAResult', 'databasecode': 'DatabaseCode', 'sampleid': 'SampleID','distinctcode': 'Distinct'})
					errorLog(bugs)
					# drop all duplicates
					stations.drop_duplicates(inplace=True)
					errorLog(stations)
					bugs_count = len(bugs.index)
					stations_count = len(stations.index)
					errorLog("bugs_count:")	
					errorLog(bugs_count)	
					errorLog("stations_count:")	
					errorLog(stations_count)	

					# if bugs and stations arent empty then run otherwise must be a problem with gismetrics
					#if bugs_count and stations_count:

					# dump new bugs and stations dataframe to timestamp csv file location 
					# timestamp.bugs.csv
					errorLog("create bugs and stations file...")
					bugs_filename = '/var/www/smc/files/' + TIMESTAMP + '.bugs.csv'
					stations_filename = '/var/www/smc/files/' + TIMESTAMP + '.stations.csv'
					errorLog(bugs_filename)
					errorLog(stations_filename)
					bugs.to_csv(bugs_filename, sep=',', encoding='utf-8', index=False)
					stations.to_csv(stations_filename, sep=',', encoding='utf-8', index=False)

					# create bugs file by combining two excel tabs and making database call to get related crosswalk fields - single bugs dataframe
					# dump bugs dataframe to timestamped csv
					# create station file by getting subsetted fields from database - single stations dataframe
					# dump stations dataframe to timestamped csv
					# run csci script with new bugs/stations csv files
					# outpute csci reports so user can download
					errorLog("running csci tool...")
					import subprocess
					command = 'Rscript'
					path2script = '/var/www/smc/proj/rscripts/csci.R'
					args = [TIMESTAMP,bugs_filename,stations_filename]
					cmd = [command, path2script] + args
					#cmd = [command, path2script]
					errorLog(cmd)
					try:
						x = subprocess.check_output(cmd, universal_newlines=True)
						# NEED TO ADD CODE TO CHECK IF x = true
						# IF x = true then all output files process properly
						errorLog("x:")
						errorLog(x)
						file_to_get = "/var/www/smc/logs/%s.core.csv" % TIMESTAMP
						errorLog("file to get:")
						errorLog(file_to_get)
						all_dataframes["2 - core_csv - tmp_cscicore"] = pd.read_csv('/var/www/smc/logs/%s.core.csv' % TIMESTAMP)
						all_dataframes["2 - core_csv - tmp_cscicore"].columns = [x.lower() for x in all_dataframes["2 - core_csv - tmp_cscicore"].columns]

						## WHAT HAPPENS IF CSCI SCORE IS ALREADY IN DATABASE - MAY WANT TO CHECK ABOVE
						errorLog("print core_csv columns:")
						errorLog(list(all_dataframes["2 - core_csv - tmp_cscicore"]))
						errorLog("remove index:")
						all_dataframes["2 - core_csv - tmp_cscicore"].drop(['unnamed: 0'],axis=1, inplace=True)
						errorLog(list(all_dataframes["2 - core_csv - tmp_cscicore"]))
						errorLog(all_dataframes["2 - core_csv - tmp_cscicore"])
						
						summary_results_link = 'http://checker.sccwrp.org/smc/logs/%s.core.csv' % TIMESTAMP
						summary_results_link = TIMESTAMP

						### IMPORTANT LOAD ONE CSCI FIELD FROM CSV FILE AND MAP IT TO EXISTING BUGS/STATIONS DATAFRAME THEN OUTPUT TO CSV LOAD FILE FOR IMPORT
						### AT STAGING INTO DATABASES
						message = "Success CSCI"
						errorLog(message)
						state = 0
					except subprocess.CalledProcessError as e:
						# here is where we email sccwrp to let them know we couldnt get csci score for sampleid - we still need load the data and try to load other sampleids 
						# if there are different return codes we can adjust below
						if e.returncode == 1:
						    errorLog("mail %s" % e.returncode)
						errorLog("failed...%s" % e)
						message = "Failed to run csci"
						errorLog(message)
						state = 0
				else:
					message = "Failed CSCI delineate"
					#mail_body = "The following user: %s with agency/lab: %s attempted to submit data for owner: %s, project: %s, sampled year: %s, but the csci portion of the checker failed to process the following un-delineated stations: %s" % (login,agency,owner,project,year,unique_stations)
					# let sccwrp know that a user is submitting data for a station that is not deliniated we will not be able process csci score
					#errorLog(mail_body)
					#status = internal_email("notify","checker@checker.sccwrp.org",["pauls@sccwrp.org"],message,mail_body)
					#if status == 1:
					#	errorLog("failed to email sccwrp")
					#else:
					#	errorLog("emailed sccwrp")
					state = 0
		except ValueError:
			message = "Failed CSCI routine"	
			errorLog(message)
			state = 0
		for dataframe in all_dataframes.keys():
			if 'custom_errors' in all_dataframes[dataframe]:
				custom_errors.append(getCustomErrors(all_dataframes[dataframe],dataframe,'custom_errors'))
				custom_redundant_errors.append(getCustomRedundantErrors(all_dataframes[dataframe],dataframe,"custom_errors"))
			if 'custom_warnings' in all_dataframes[dataframe]:
				errorLog("custom_warnings")
				custom_errors.append(getCustomErrors(all_dataframes[dataframe],dataframe,'custom_warnings'))
				errorLog(custom_warnings)
				custom_redundant_errors.append(getCustomRedundantErrors(all_dataframes[dataframe],dataframe,"custom_warnings"))
		custom_checks = json.dumps(custom_errors, ensure_ascii=True)
		custom_redundant_checks = json.dumps(custom_redundant_errors, ensure_ascii=True)
		## END RETRIEVE ERRORS ##
		# get filenames from fileupload routine
		errorLog(message)
		#assignment_table = result.groupby(['stationid','lab','analyteclass']).size().to_frame(name = 'count').reset_index()
		# lets reassign the analyteclass field name to species so the assignment query will run properly - check StagingUpload.py for details
		#assignment_table = assignment_table.rename(columns={'analyteclass': 'species'})
		return assignment_table, custom_checks, custom_redundant_checks, summary_checks, summary_results_link, message, result_unique_stations
	except ValueError:
		message = "Critical Error: Failed to run taxonomy checks"	
		errorLog(message)
		state = 1
		return jsonify(message=message,state=state)
