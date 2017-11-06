from flask import Blueprint, request, jsonify, current_app
import json
import pandas as pd
import numpy as np
#from time import sleep
from .ApplicationLog import *


def getIndividualErrors(df):
	errorLog("Core: start getIndividualErrors")
	#  clear dataframe of rows that have no errors
	dfjson = df
        dfjson = dfjson[pd.notnull(dfjson['row'])]
        # must re-index dataframe - set to 0 after removing rows
	dfjson.reset_index(drop=True,inplace=True) 
	tmp_dict = {}
	count = 0
	for index, row in dfjson.iterrows():
		tmp_dict[count] = '[{"row":"%s","value":[%s]}]' % (row['row'],row["errors"])
		count = count + 1
	errorLog("Core: end getIndividualErrors")
	return tmp_dict

def getRedundantErrors(check,df):
	errorLog("Core: start getRedundantErrors")
	errorLog("Core: checking on: %s" % check)
	# group  by dataframe by field_errors and print row numbers
	tmp_dict = {}
	count = 0
	#errorLog("Core: pre-function check: %s" % check)	
	for error_message,group in df.groupby(check):
		#errorLog("Core: post-function check: %s" % check)	
		#errorLog("Core: post-function error message: %s" % error_message)	
		# only return errors if there are more one (redundant)
		#errorLog("Core: grouped rows count: %s" % len(group.row))
		if len(group.row) > 1:
			row_fix = []
			# for custom checks we look at the tmp_row column instead of row
			if check == "custom_error_bs" or check == "stddev_errors" or check == "mean_ee_errors" or check == "mean_mg_errors" or check == "coef_errors" or check == "toxicity_errors" or check == "toxicity_summary_errors":
				for r in group.tmp_row:
					row_fix.append(str(int(r) + 2))
				rows = ', '.join(row_fix)
			else:
				for r in group.row:
					row_fix.append(str(int(r) + 2))
				rows = ', '.join(row_fix)
			#errorLog(row_fix)
			#errorLog('Core: [{"rows":"%s","value":[%s]}]' % (rows,error_message))
			tmp_dict[count] = '[{"rows":"%s","value":[%s]}]' % (rows,error_message)
		count = count + 1
	errorLog("Core: end getRedundantErrors")
	return tmp_dict


lookup_checks = Blueprint('lookup_checks', __name__)

@lookup_checks.route("/lookup", methods=["POST"])
def lookup():
	errorLog("Blueprint - Lookup")
	all_dataframes = current_app.all_dataframes
	db = current_app.db
	dbtype = current_app.dbtype
	eng = current_app.eng
	sheet_names = []
	try:
		message = "Lookup: Start looping through each dataframe."
		errorLog(message)
		# set state to 0 to start
		state = 0
		#message = "Core: %s" % all_dataframes.keys()
		# counter for applying errros to tabs
		count = 0
		for dataframe in all_dataframes.keys():
			df_sheet_and_table_name = dataframe.strip().split(" - ")
			sheet_name = str(df_sheet_and_table_name[0])
			sheet_names.append(str(df_sheet_and_table_name[1]))
			table_name = str(df_sheet_and_table_name[2])
			errorLog("Lookup: sheetname: %s" % sheet_name)
			errorLog("Lookup: tablename: %s" % table_name)
			### DATABASE BUSINESS RULES CHECKS ###
			try:
				dataframe = checkLookupCodes(db,dbtype,eng,table_name,all_dataframes[dataframe])
			except ValueError:
				message = "Critical Error: Failed to run checkLookupCodes."
				state = 1
			# retrieve all the errors that were found - to json - return to browser
			try:
				data_checks = {}
				data_checks_redundant = {}
				list_errors = []
				list_redundant_errors = []
				if 'errors' in  dataframe:
					list_errors.append(getIndividualErrors(dataframe))
				if 'lookup_error' in  dataframe:
					list_redundant_errors.append(getRedundantErrors("lookup_error",dataframe))
				data_checks[count] = json.dumps(list_errors, ensure_ascii=True)
				data_checks_redundant[count] = json.dumps(list_redundant_errors, ensure_ascii=True)
			except ValueError:
				message = "Core: Failed to retrieve errors."
				state = 1
			count = count + 1
		#state = 0
	except ValueError:
		message = "Critical Error: Failed to run core checks"	
		errorLog(message)
		state = 1
	return jsonify(message=message,state=state,business=data_checks,redundant=data_checks_redundant)
