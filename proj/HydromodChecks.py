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


######################################
## BEGIN CHANNEL ENGINEERING SCRIPT ##
######################################

hydromod_checks = Blueprint('hydromod_checks', __name__)
@hydromod_checks.route("/hydromod", methods=["POST"])

def hydromod(all_dataframes,sql_match_tables,errors_dict,project_code,login_info):
	errorLog("Function - hydromod")
	message = "Custom Hydromod: Start checks."
	statusLog("Starting Hydromod Checks")
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
		if table_name == "tbl_hydromod":
			tables.append("hydromod")
			hydro = all_dataframes[dataframe]
			hydro['tmp_row'] = hydro.index
        
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


                
                
                # -- Robert 12/14/2018 -- #
                
                # Checks for where FullyArmored is equal to 'No'
                # If "fullyarmored" == "No" then the following fields are required:
                required_fields = ['bankheightl1',
                                    'bankheightl2',
                                    'bankheightl3',
                                    'bankheightr1',
                                    'bankheightr2',
                                    'bankheightr3',
                                    'bankanglel1',
                                    'bankanglel2',
                                    'bankanglel3',
                                    'bankangler1',
                                    'bankangler2',
                                    'bankangler3']

                def NotFullyArmoredCheck(fieldname):
                    'If "FullyArmored" field is equal to "No" then check if the required fields are not empty'
                    errorLog('Check - if FullyArmored == No then the %s field is required' % fieldname)
                    errorLog('Submitted Data where FullyArmored == No, but the %s field is empty:' % fieldname)
                    # the .isnull() may have to be changed to -88... because anyways, these fields are NOT NULL in the database
                    # meaning if they left them blank, they won't pass CoreChecks....
                    # NOTE I need to ask Jeff what he wants them to put if they aren't submitting data for this field
                    errorLog(hydro[ (hydro.fullyarmored == 'No') & (hydro[fieldname].isnull())  ])
                    checkData(hydro[ (hydro.fullyarmored == 'No') & (hydro[fieldname].isnull()) ].tmp_row.tolist(), fieldname, 'Undefined Error', 'error', 'The FullyArmored field is equal to No, but the %s field is empty' % fieldname, hydro)
                
                # This for loop will check all the required fields in all rows where the FullyArmored value is equal to 'No'
                for field in required_fields:
                    NotFullyArmoredCheck(field)


                # -- Checks for where FullyArmored is equal to 'Yes' -- #
                errorLog("# -- Checks for where FullyArmored is equal to Yes -- # ")
                def FullyArmoredCheck(column, value):
                    'if Fully Armored is equal to yes, then the column must be equal to a certain value'
                    errorLog("Check - If fullyarmored == Yes then %s must be %s" % (column, value) )
                    errorLog("Submitted Data where FullyArmored is Yes but the value of %s is not %s" % (column, value) )
                    errorLog(hydro[ (hydro.fullyarmored == 'Yes') & (hydro[column] != value) ])
                    checkData(hydro[ (hydro.fullyarmored == 'Yes') & (hydro[column] != value) ].tmp_row.tolist(), column, "Undefined Error", 'error', 'The Value of the FullyArmored column is Yes, but the value of %s is not equal to %s' % (column, value) , hydro)



                # Check - If "fullyarmored" == "Yes" then "streambedstate" must be "C"
                # Jeff confirmed that this check is correct.
                FullyArmoredCheck('streambedstate', "C")

                
                # Check - If "fullyarmored" == "Yes" then "gradecontrol" must be "A"
                # Jeff has NOT confirmed that this check is correct
                FullyArmoredCheck('gradecontrol', "A")
                
                
                # These next two checks may have to be changed. I was unclear on what to do.
                # Check - If "fullyarmored" == "Yes" then "lateralsusceptibilityl" must be "1"
                # Jeff has NOT confirmed that this check is correct
                FullyArmoredCheck('lateralsusceptibilityl', 1)

                # Check - If "fullyarmored" == "Yes" then "lateralsusceptibilityr" must be "1"
                # Jeff has NOT confirmed that this check is correct
                FullyArmoredCheck('lateralsusceptibilityr', 1) 

                # -- End of Checks where FullyArmored == Yes -- #





                # -- Additional Checks from Submission Guide -- #

                # From Hydromod Submission Guide Page 1 - Bullet points 7, 8, 9, 10, 13
                # For the fields Area, Precipitation, ValleySlope, ValleyWidth, D50...
                # It says if data is unavailable at time of submission, enter "NR"
                # NOTE Ask Jeff: Is there a way to check whether the data was available or not at the time of submission?
                # NOTE He said no to the above question.
                #       - He also said that D50 is needed if FullyArmored is No AND StreamBedState is B
                #
                # NOTE There is another can of worms that gets opened up with this part of the submission guide.
                #       - The guide says that if the data is unavailable, enter 'NR' BUT these are integer fields
                #       - We can do one of the following:
                #           1) Make them put -88 if data is unavailable
                #           2) Change the fields to strings, so that the data entered is either a string of a number, or 'NR'




                # Hydromod Submission Guide Bullet points 15 through 20, 22 through 27
                # BankHeightL1, BankHeightL2,... BankAngleL1, BankAngleL2... etc...
                # Submission guide says "Not required at fully armored channels"
                # Questions:
                #   1) These are not null fields in the database. Therefore if they leave these blank, they will not pass CoreChecks.
                #       What should they enter into this field in order to leave the field "blank"? Maybe -88??
                #       I am thinking these fields should be -88 if they aren't submitting data for that field, otherwise a positive number.
                #
                #   2) When the submission guide says "Not required" 
                #       does that mean that you guys would rather have them NOT submit anything in these fields when the channel is fully armored?
                # These questions are still unanswered.




                
                
                # Submission Guide page 2 - Bullet point #29
                # If the channel is fully armored then StreamBedState must be "C"
                # We have this check in place already



                
                
                # Submission Guide page 2 - Bullet point #31
                # Word for word from Submission Guide:
                # "If StreambedState is B, then ArmoringPotential is required. Otherwise enter NA"
                # I have concerns with this.
                # If the user puts the string NA, the checker interprets this as a null value when it reads in the data, 
                #   and then it will say that the blank field doesn't match the lookup list for ArmoringPotential.
                # Furthermore, the submission guide says that the four allowed values for ArmoringPotential are "A", "B", "C", and "NA"
                #   BUT the lookup list in the database says the four allowed values are "A", "B", "C", and "NR"
                # I am thinking we should go with what is currently in the lookup list.
                # 
                # So here is my thinking for this check:
                #   If StreambedState == "B" then ArmoringPotential should be "A", "B", or "C", otherwise it should be "NR"
                #
                # NOTE For Bullet point #33 (GradeControl) I have the same exact concern as I did for ArmoringPotential
                #
                #
                # NOTE From Jeff:
                #       - If StreambedState == 'B' then ArmoringPotential needs to be in ['A', 'B', 'C']
                #       - If StreambedState == 'B' then GradeControl needs to be in ['A', 'B', 'C']
                #       - If StreambedState != 'B' then it does NOT matter what ArmoringPotential or GradeControl are.
                
                errorLog("If StreambedState == 'B' then ArmoringPotential needs to be in ['A', 'B', 'C']")
                checkData(hydro[(hydro.streambedstate == 'B')&(~hydro.armoringpotential.isin(['A','B','C']))].tmp_row.tolist(), 'ArmoringPotential', 'Undefined Error', 'error', 'If StreamBedState is B then ArmoringPotential must be either A, B, or C', hydro) 

                errorLog("If StreambedState == 'B' then GradeControl needs to be in ['A', 'B', 'C']")
                checkData(hydro[(hydro.streambedstate == 'B')&(~hydro.gradecontrol.isin(['A','B','C']))].tmp_row.tolist(), 'GradeControl', 'Undefined Error', 'error', 'If StreamBedState is B then GradeControl must be either A, B, or C', hydro)







                # Submission Guide page 3 - Bullet point #35 and 36
                # Concerning LateralSusceptibilityL and LateralSusceptibilityR
                # The Submission Guide says that these fields are not required. However, these fields are listed as NOT NULL in the database.
                # Questions and concerns:
                #   
                #   1) Should we allow the database to accept NULL values for these fields? or simply make them fill them in with -88 if there is no data?
                #   
                #   2) The Submission Guide says they may enter 1, 2, 3, 4, or 5 for these fields. Should we create a lookup list in the database?
                #       Or we may simply check if the field has one of those specified values during the Extended Checks process.
                # NOTE We created a lookup list for this one. This check will now be taken care of in the CoreChecks process.







                # NOTE Are there any Logic Checks?
                # From what I can tell, no. Logic Checks are usually outlined in the submission guide.
		
                
                ## LOGIC ##
		message = "Starting Hydromod Logic Checks"
		errorLog(message)
		statusLog(message)
		## END LOGIC CHECKS ##

		## CUSTOM CHECKS ##
		## END CUSTOM CHECKS ##
		## END CHECKS ##

		## START MAP CHECK ##
		# get a unique list of stations from stationcode
                list_of_stations = pd.unique(hydro['stationcode'])
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
                message = "Hydromod Checks completed successfully"
		errorLog(message)
                statusLog(message)
		#assignment_table = result.groupby(['stationid','lab','analyteclass']).size().to_frame(name = 'count').reset_index()
		# lets reassign the analyteclass field name to species so the assignment query will run properly - check StagingUpload.py for details
		#assignment_table = assignment_table.rename(columns={'analyteclass': 'species'})
                errorLog('Custom Checks')
                errorLog(custom_checks)
                errorLog(custom_redundant_checks)
		return custom_checks, custom_redundant_checks, message, unique_stations
	
        except ValueError:
		message = "Critical Error: Failed to run channel engineering checks"	
		errorLog(message)
		state = 1
		return jsonify(message=message,state=state)
