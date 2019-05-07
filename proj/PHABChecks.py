from flask import Blueprint, request, jsonify, session
import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine, exc, Table, Column, Integer, Unicode, MetaData, String, Text, update, and_, select, func, types
from sqlalchemy.sql import text
from pandas import DataFrame
from .ApplicationLog import *
import rpy2
import rpy2.robjects as robjects
from rpy2.robjects.packages import SignatureTranslatedAnonymousPackage
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
import pandas_access as mdb
import re
from datetime import datetime
from PhabConvert import phabconvert
from PythonIPI import python_IPI
from PYHABMetrics import python_phabmetrics

PHABMetrics = importr("PHABMetrics")
phabmetrics = PHABMetrics.phabmetrics
PHAB = importr("PHAB")
ipi = PHAB.IPI
r = robjects.r

as_numeric = r['as.numeric']
as_integer = r['as.integer']
as_character = r['as.character']
as_POSIXct = r['as.POSIXct']

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





phab_checks = Blueprint('phab_checks', __name__)

@phab_checks.route("/phab", methods=["POST"])

def phab(all_dataframes,pmetrics_long,stations,sql_match_tables,errors_dict,project_code,login_info):
	errorLog("Function - phab")
	message = "Custom PHAB: Start checks."
	statusLog("Starting PHAB Checks")
	errorLog(message)
	errorLog("project code: %s" % project_code)

        errorLog(login_info)
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
	errorsCount(errors_dict,'custom')
        

        try:
            
            # Run Custom Checks here
            
            if custom_errors == []:
                # Run IPI Here
                try:
                    ipi_output = python_IPI(pmetrics_long, stations)
                    message = "IPI ran successfully"
                except Exception as errmsg:
                    message = "There was a problem processing IPI"
                    errorLog(message)
                    errorLog(errmsg)
                    ipi_output = pd.DataFrame({})

            
            
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
            summary_results_link = TIMESTAMP
            statusLog("Finalizing Report")



            return all_dataframes, ipi_output, assignment_table, custom_checks, custom_redundant_checks, summary_checks, summary_results_link, message

        except Exception as e:
            message = "Critical Error: Failed to run phab checks"	
            errorLog(message)
            errorLog(e)
            state = 1
            return jsonify(message=message,state=state)
















# ----------------------- #
#       CODE ARCHIVE
# ----------------------- #


# Block of code below is commented out because it may be useful later
# this was the way we had the thing running before to process PHabmetrics, where we had field and habitat separate until the last minute
# Now we are doing it a bit different, where we have that huge 92 column dataframe nad extract certain fields from it for the sake if running phabmetrics
'''
##### PRE-PROCESSINGS  FOR R PHABMETRICS
errorLog("Pre-Processing Data for PHAB Metrics...")

# Camel-case field names for integration with PHAB Metrics
habitat_query = habitat_query.rename(index=str, columns = {'stationcode':'StationCode','sampledate':'SampleDate','replicate':'Replicate','locationcode':'LocationCode','analytename':'AnalyteName','unitname':'UnitName','variableresult':'VariableResult','fractionname':'FractionName','result':'Result','resqualcode':'ResQualCode','qacode':'QACode'})

# add id field for integration with PHAB Metrics
habitat_query['id'] = habitat_query.StationCode

# Camel-case field_query fields for integration with PHAB Metrics
field_query = field_query.rename(index=str, columns = {'stationcode':'StationCode','sampledate':'SampleDate','replicate':'Replicate','locationcode':'LocationCode','analytename':'AnalyteName','unitname':'UnitName','variableresult':'VariableResult','fractionname':'FractionName','result':'Result','resqualcode':'ResQualCode','qacode':'QACode'})

# add id field for integration with PHAB Metrics
field_query['id'] = field_query.StationCode

# collect approprate data from the tables
habitat_rawdata = habitat_query[['StationCode', 'SampleDate', 'Replicate', 'LocationCode', 'AnalyteName', 'UnitName', 'VariableResult', 'FractionName', 'Result', 'ResQualCode', 'QACode', 'id']]
field_rawdata = field_query[['StationCode', 'SampleDate', 'Replicate', 'LocationCode', 'AnalyteName', 'UnitName', 'FractionName', 'Result', 'ResQualCode', 'QACode', 'id']]

# Merge habitat and field data to get a complete table ready to input to phabmetrics function
rawdata = habitat_rawdata.merge(field_rawdata, on = ['StationCode', 'SampleDate', 'Replicate', 'LocationCode', 'AnalyteName', 'UnitName', 'FractionName', 'Result', 'ResQualCode', 'QACode', 'id'], how='outer')

# Change value Length Reach to Length, Reach (must be done to continue)
rawdata.AnalyteName = rawdata['AnalyteName'].apply(lambda x: "Length, Reach" if x == "Length Reach" else x)
errorLog("Finished Pre-Processing Data for PHAB Metrics.")
##### END PRE-PROCESSING FOR R PHABMETRICS
'''
# Commented out for now. We will try to convert datatypes exclusively in the R world and see if that works.
'''
errorLog("Filling NA values with approriate values")
rawdata.Result.fillna(-88, inplace = True)

errorLog(rawdata)
errorLog(rawdata.Result)
errorLog("converting data types")
# Data type conversions, this part may not be necessary, but if its not broken, I don't want to try to fix it
errorLog("converting Result")
rawdata.Result = rawdata.Result.astype(float) # good
errorLog("converting Replicate")
rawdata.Replicate = rawdata.Replicate.astype(int) # good
errorLog("converting VariableResult")
rawdata['VariableResult'] = rawdata['VariableResult'].astype(str) # good
errorLog("converting StationCode")
rawdata['StationCode'] = rawdata['StationCode'].astype(str) # good
errorLog("converting LocationCode")
rawdata['LocationCode'] = rawdata['LocationCode'].astype(str) # good
errorLog("converting AnalyteName")
rawdata['AnalyteName'] = rawdata['AnalyteName'].astype(str) # good
errorLog("converting UnitName")
rawdata['UnitName'] = rawdata['UnitName'].astype(str) # good
errorLog("converting FractionName")
rawdata['FractionName'] = rawdata['FractionName'].astype(str) # good
errorLog("converting ResQualCode")
rawdata['ResQualCode'] = rawdata['ResQualCode'].astype(str) # good
errorLog("converting QACode")
rawdata['QACode'] = rawdata['QACode'].astype(str) # good
errorLog("converting id")
rawdata['id'] = rawdata['id'].astype(str) # good
#rawdata['Result'] = rawdata['Result'].fillna(0)
'''

# pandas2ri.activate() was slowing the code down a lot so we commented it out. Apparently it is not even necessary for what we are doing.
# 22 Feb 2019 uncommented for purpose of experimenting
# 25 Feb 2019 re commented out, also for experimenting to see if IPI will run with it deactivated
#errorLog("activating pandas2ri")
#pandas2ri.activate()
