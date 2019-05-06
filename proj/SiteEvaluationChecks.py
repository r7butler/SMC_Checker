#############
## IMPORTS ##
#############
from flask import Blueprint, request, jsonify, session
import os, time, datetime
import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine, exc, Table, Column, Integer, Unicode, MetaData, String, Text, update, and_, select, func, types
from pandas import DataFrame
from datetime import datetime
from .ApplicationLog import *


#####################
## ERROR FUNCTIONS ##
#####################
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



siteevaluation_checks = Blueprint('siteevaluation_checks', __name__)
@siteevaluation_checks.route("/siteevaluation", methods=["POST"])
def siteevaluation(all_dataframes,sql_match_tables,errors_dict,project_code,login_info):
	errorLog("Function - siteevaluation")
	message = "Custom SiteEvaluation: Start checks."
	statusLog("Starting SiteEvaluation Checks")
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
        ### get date and time
        gettime = int(time.time())
        timestamp_date = datetime.datetime.fromtimestamp(gettime)
	# add submitted table names to list
	tables = []
	# match tablenames to tabs
	errorLog(all_dataframes.keys())
	for dataframe in all_dataframes.keys():
		df_sheet_and_table_name = dataframe.strip().split(" - ")
		errorLog(df_sheet_and_table_name)
		table_name = str(df_sheet_and_table_name[2])
		errorLog(table_name)
		if table_name == "reconexample":
			tables.append("reconexample")
			reconexample = all_dataframes[dataframe]
			reconexample['tmp_row'] = reconexample.index
		if table_name == "tbl_siteeval":
			tables.append("recon")
			recon = all_dataframes[dataframe]
			recon['tmp_row'] = recon.index
        errorLog(recon.head())
        errorLog('Starting Check Functions')
	try:
                #####################
		## CHECK FUNCTIONS ##
                #####################
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

                # Robert 12/21/2018 #

                
                # Noticed that a bunch of this code has the same exact stuff in it... 
                # Below is my attempt to write a one size fits all function to knock the rest of this stuff out
                # I will try to write a function that I can call with various arguments to knock out all the checks here to get more "DRY" code




                # -- DOCUMENTATION -- #
                # I noticed a pattern among these checks. It seems to be "If this column has this value, then this column needs this value"
                # the variable names 'ifcolumn' and 'ifvalue' refer to the column and value in the first part of the check statement. 
                # the variable names 'thencolumn' and 'thenvalue' refer to the column and value in the second part of the check statement.
                

                # EXAMPLE:
                # "If evalstatuscode == 'NE' then waterbodystatuscode should be equal to 'U'"
                # in the above statement, the 'ifcolumn' would be evalstatuscode, and the 'ifvalue' would be 'NE'
                # The 'thencolumn' would be 'waterbodystatuscode' and the 'thenvalue' would be 'U'
                
                # Also the arguments of the function should be strings, except the last one. -- #
                # The last argument, I called notequal. the default value is False.
                # The notequal argument should be True if the "IF" condition wants to check if the "ifvalue" is NOT equal to something.
                # for example:
                # If waterbodystatuscode != 'S', then the value of flowstatuscode should be equal to 'NR'
                
                # Here is the function:
                def siteevalcheck(ifcolumn, ifvalue, thencolumn, thenvalue, notequal = False):
                    errorLog("starting siteevalcheck function")
                    if notequal == True:
                        errorLog("notequal == True")
                        errorLog("Check: if %s != %s then %s must be equal to %s" % (ifcolumn, ifvalue, thencolumn, thenvalue))
                        errorLog("Submitted Data where %s != %s but %s is not equal to %s" % (ifcolumn, ifvalue, thencolumn, thenvalue))
                        errorLog(recon[ (recon[ifcolumn] != ifvalue) & (recon[thencolumn] != thenvalue) ])
                        checkData(recon[ (recon[ifcolumn] != ifvalue) & (recon[thencolumn] != thenvalue) ].tmp_row.tolist(), thencolumn, 'Undefined Error', 'error', 'If the value of %s is anything other than %s, then the value of %s needs to be equal to %s' % (ifcolumn, ifvalue, thencolumn, thenvalue), recon) 
                    else:
                        errorLog("notequal == False")
                        errorLog("Check: if %s == %s then %s must be equal to %s" % (ifcolumn, ifvalue, thencolumn, thenvalue))
                        errorLog("Submitted Data where %s == %s but %s is not equal to %s" % (ifcolumn, ifvalue, thencolumn, thenvalue))
                        errorLog(recon[ (recon[ifcolumn] == ifvalue) & (recon[thencolumn] != thenvalue) ])
                        checkData(recon[ (recon[ifcolumn] == ifvalue) & (recon[thencolumn] != thenvalue) ].tmp_row.tolist(), thencolumn, 'Undefined Error', 'error', 'If the value of %s is %s, then the value of %s needs to be equal to %s' % (ifcolumn, ifvalue, thencolumn, thenvalue), recon) 




                # -- Check - if evalstatuscode == 'NE' then:
                
                # the following must be equal to "U":
                # waterbodystatuscode
                # flowstatuscode
                # wadeablestatuscode
                # physicalaccessstatuscode
                # landpermissionstatuscode
                
                # AND

                # fieldreconcode code must be equal to "N"

                # AND 

                # samplestatuscode must be equal to "NS"
                
                siteevalcheck('evalstatuscode', 'NE', 'waterbodystatuscode', 'U')
                siteevalcheck('evalstatuscode', 'NE', 'flowstatuscode', 'U')
                siteevalcheck('evalstatuscode', 'NE', 'wadeablestatuscode', 'U')
                siteevalcheck('evalstatuscode', 'NE', 'physicalaccessstatuscode', 'U')
                siteevalcheck('evalstatuscode', 'NE', 'landpermissionstatuscode', 'U')
                
                siteevalcheck('evalstatuscode', 'NE', 'fieldreconcode', 'N')
                siteevalcheck('evalstatuscode', 'NE', 'samplestatuscode', 'NS')

                # -- End of 'Check - evalstatuscode == "NE" ...' -- #





                # -- Check - if waterbodystatuscode != "S" then the following fields should be equal to "Not a stream"
                # flowstatuscode
                # wadeablestatuscode
                # physicalaccessstatuscode
                # landpermissionstatuscode
                # samplestatuscode
                
                siteevalcheck('waterbodystatuscode', 'S', 'flowstatuscode', 'Not a stream', notequal = True)
                siteevalcheck('waterbodystatuscode', 'S', 'wadeablestatuscode', 'Not a stream', notequal = True)
                siteevalcheck('waterbodystatuscode', 'S', 'physicalaccessstatuscode', 'Not a stream', notequal = True)
                siteevalcheck('waterbodystatuscode', 'S', 'landpermissionstatuscode', 'Not a stream', notequal = True)
                siteevalcheck('waterbodystatuscode', 'S', 'samplestatuscode', 'Not a stream', notequal = True)

                # -- End of 'Check - if waterbodystatuscode != "S" then the following fields should be equal to "Not a stream" ... '



                
                
                # -- Check - if samplestatuscode == "S" then ...
                # evalstatuscode == "E"
                # fieldreconcode == "Y"
                # waterbodystatuscode == "S"
                # flowstatuscode (SMC or PSA) == "P" or "Sp"
                # wadeablestatuscode (SMC or PSA) == "W"
                # physicalaccessstatuscode == "A"
                # landpermissionstatuscode == "G"
                
                siteevalcheck('samplestatuscode', 'S', 'evalstatuscode', 'E')
                siteevalcheck('samplestatuscode', 'S', 'fieldreconcode', 'Y')
                siteevalcheck('samplestatuscode', 'S', 'waterbodystatuscode', 'S')

                
                # for this one, flowstatuscode will not be checked using the function, since it is allowed to have two values.
                # I will write some "customized" code for flowstatuscode here
                errorLog("Check: if samplestatuscode == S then flowstatuscode must be equal to P or Sp")
                errorLog("Submitted Data where samplestatuscode == S but flowstatuscode is not equal P or Sp")
                errorLog(recon[ (recon.samplestatuscode == "S") & ((recon.flowstatuscode != "P") & (recon.flowstatuscode != "Sp")) ])
                checkData(recon[ (recon.samplestatuscode == "S") & ((recon.flowstatuscode != "P") & (recon.flowstatuscode != "Sp")) ].tmp_row.tolist(), 'flowstatuscode', 'Undefined Error', 'error', 'If the value of samplestatuscode is S, then the value of flowstatuscode needs to be equal to either P or Sp', recon) 

                siteevalcheck('samplestatuscode', 'S', 'wadeablestatuscode', 'W')
                siteevalcheck('samplestatuscode', 'S', 'physicalaccessstatuscode', 'A')
                siteevalcheck('samplestatuscode', 'S', 'landpermissionstatuscode', 'G')

                # -- End of "Check - if samplestatuscode == "S" then ..." -- #
                


                
                
                # -- Check - if samplestatuscode == "NS" then ...
                # evalstatuscode == "E"
                # fieldreconcode == "Y"
                # waterbodystatuscode == "S"
                # flowstatuscode (SMC or PSA) == "P" or "SP"
                # wadeablestatuscode (SMC or PSA) == "W"
                # physicalaccessstatuscode == "A"
                # landpermissionstatuscode == "G"
                # samplestatuscode == "NS"
                # sitename is required

                siteevalcheck('samplestatuscode', 'NS', 'evalstatuscode', 'E')
                siteevalcheck('samplestatuscode', 'NS', 'fieldreconcode', 'Y')
                siteevalcheck('samplestatuscode', 'NS', 'waterbodystatuscode', 'S')

                
                # for this one, flowstatuscode will not be checked using the function, since it is allowed to have two values.
                # I will write some "customized" code for flowstatuscode here
                errorLog("Check: if samplestatuscode == NS then flowstatuscode must be equal to P or Sp")
                errorLog("Submitted Data where samplestatuscode == NS but flowstatuscode is not equal P or Sp")
                errorLog(recon[ (recon.samplestatuscode == "NS") & (recon.flowstatuscode != "P") & (recon.flowstatuscode != "Sp") ])
                checkData(recon[ (recon.samplestatuscode == "NS") & (recon.flowstatuscode != "P") & (recon.flowstatuscode != "Sp") ].tmp_row.tolist(), 'flowstatuscode', 'Undefined Error', 'error', 'If the value of samplestatuscode is NS, then the value of flowstatuscode needs to be equal to either P or Sp', recon) 

                siteevalcheck('samplestatuscode', 'NS', 'wadeablestatuscode', 'W')
                siteevalcheck('samplestatuscode', 'NS', 'physicalaccessstatuscode', 'A')
                siteevalcheck('samplestatuscode', 'NS', 'landpermissionstatuscode', 'G')


                # The sitename check also can't be done with the function, so it is probably more efficient just to write custom code 
                # rather than make heavy modifications to the function just for one check...
                errorLog("Check: if samplestatuscode == NS then flowstatuscode must be equal to P or Sp")
                errorLog("Submitted Data where samplestatuscode == NS but flowstatuscode is not equal P or Sp")
                errorLog(recon[ (recon.samplestatuscode == "NS") & (recon.sitename == '' )])
                checkData(recon[ (recon.samplestatuscode == "NS") & (recon.sitename == '')].tmp_row.tolist(), 'sitename', 'Undefined Error', 'error', 'If the value of samplestatuscode is NS, then sitename is required.', recon) 


                # -- End of " Check - if samplestatuscode == "NS" then ..." -- #


            


                # -- End of Custom Checks for Site Evaluation -- #    
                

		## LOGIC ##
		message = "Starting SiteEvaluation Logic Checks"
		errorLog(message)
		statusLog(message)
		## END LOGIC CHECKS ##

		## CUSTOM CHECKS ##
		## END CUSTOM CHECKS ##
		## END CHECKS ##

		## START MAP CHECK ##
		# get a unique list of stations from stationcode
		list_of_stations = pd.unique(recon['stationcode'])
		unique_stations = ','.join("'" + s + "'" for s in list_of_stations)
		## END MAP CHECKS

		## NEW FIELDS ##
		#batch['project_code'] = project_code
		## END NEW FIELDS ##

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
		message = "Critical Error: Failed to run siteevaluation checks"	
		errorLog(message)
		state = 1
		return jsonify(message=message,state=state)
