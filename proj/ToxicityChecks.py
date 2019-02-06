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
	subcodes[field].replace(r'^$',np.nan,regex=True,inplace = True)
        # check submitted data for at least one code
        #nan_rows = subcodes.loc[subcodes[field]==''].tmp_row.tolist()
	nan_rows = subcodes.loc[subcodes[field].isnull()].tmp_row.tolist()
	
        # check submitted data for invalid codes
	db_list = codes_df[dbfield].apply(lambda row: "".join(row.split()).lower()).tolist()
        #subcodes['check'] = subcodes[field][subcodes[field] != ""].apply(lambda row: set("".join(row.split()).lower().split(',')).issubset(db_list))
        subcodes['check'] = subcodes[field].dropna().apply(lambda row: set("".join(row.split()).lower().split(',')).issubset(db_list))
	invalid_codes = df.loc[subcodes.check == False].tmp_row.tolist()
	return nan_rows, invalid_codes, subcodes

toxicity_checks = Blueprint('toxicity_checks', __name__)

@toxicity_checks.route("/toxicity", methods=["POST"])

def toxicity(all_dataframes,sql_match_tables,errors_dict,project_code,login_info):
	errorLog("Function - toxicity")
	message = "Custom Toxicity: Start checks."
	statusLog("Starting Toxicity Checks")
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
		table_name = str(df_sheet_and_table_name[2])
		if table_name == "tbl_toxicitybatch":
			batch = all_dataframes[dataframe]
			batch['tmp_row'] = batch.index
		if table_name == "tbl_toxicityresults":
			result = all_dataframes[dataframe]
			result['tmp_row'] = result.index
		if table_name == "tbl_toxicitysummary":
			summary = all_dataframes[dataframe]
			summary['tmp_row'] = summary.index
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
		message = "Starting Toxicity Logic Checks"
		errorLog(message)
		statusLog(message)
		# 1 - All records for each table must have a corresponding record in the other tables due on submission. Join tables on Agency/LabCode and ToxBatch/QABatch
		### make sure there are records that match between batch and result - otherwise big problem
		# EACH TAB MUST HAVE A CORRESPONDING RELATED RECORD IN ALL THE OTHER TABS - JOIN TABLES BASED ON TOXBATCH AND LAB
		# batch
		errorLog(batch[~batch[['toxbatch','labagencycode']].isin(result[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)])
		checkLogic(batch[~batch[['toxbatch','labagencycode']].isin(result[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist(),'ToxBatch/LabAgencyCode','Logic Error','error','Each Toxicity Batch record must have a corresponding Toxicity Result record. Records are matched on ToxBatch and LabAgencyCode.',batch)
		errorLog(batch[~batch[['toxbatch','labagencycode']].isin(summary[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)])
		checkLogic(batch[~batch[['toxbatch','labagencycode']].isin(summary[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist(),'ToxBatch/LabAgencyCode','Logic Error','error','Each Toxicity Batch record must have a corresponding Toxicity Summary record. Records are matched on ToxBatch and LabAgencyCode.',batch)
		# result
		errorLog(result[~result[['toxbatch','labagencycode']].isin(batch[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)])
		checkLogic(result[~result[['toxbatch','labagencycode']].isin(batch[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist(),'ToxBatch/LabAgencyCode','Logic Error','error','Each Toxicity Result record must have a corresponding Toxicity Batch record. Records are matched on ToxBatch and LabAgencyCode.',result)
		errorLog(result[~result[['toxbatch','labagencycode']].isin(summary[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)])
		checkLogic(result[~result[['toxbatch','labagencycode']].isin(summary[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist(),'ToxBatch/LabAgencyCode','Logic Error','error','Each Toxicity Result record must have a corresponding Toxicity Summary record. Records are matched on ToxBatch and LabAgencyCode.',result)
		# summary 
		errorLog(summary[~summary[['toxbatch','labagencycode']].isin(batch[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)])
		checkLogic(summary[~summary[['toxbatch','labagencycode']].isin(batch[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist(),'ToxBatch/LabAgencyCode','Logic Error','error','Each Toxicity Summary record must have a corresponding Toxicity Batch record. Records are matched on ToxBatch and LabAgencyCode.',summary)
		errorLog(summary[~summary[['toxbatch','labagencycode']].isin(result[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)])
		checkLogic(summary[~summary[['toxbatch','labagencycode']].isin(result[['toxbatch','labagencycode']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist(),'ToxBatch/LabAgencyCode','Logic Error','error','Each Toxicity Summary record must have a corresponding Toxicity Result record. Records are matched on ToxBatch and LabAgencyCode.',summary)

		## END LOGIC CHECKS ##

		## CUSTOM CHECKS ##
		## END CUSTOM CHECKS ##
		## END CHECKS ##

		## START MAP CHECK ##
		# get a unique list of stations from stationcode
		list_of_stations = pd.unique(result['stationcode'])
		unique_stations = ','.join("'" + s + "'" for s in list_of_stations)
		## END MAP CHECKS

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
		return custom_checks, custom_redundant_checks, message, unique_stations
	except ValueError:
		message = "Critical Error: Failed to run toxicity checks"	
		errorLog(message)
		state = 1
		return jsonify(message=message,state=state)
