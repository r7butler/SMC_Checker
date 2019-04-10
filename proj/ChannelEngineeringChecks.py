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

channelengineering_checks = Blueprint('channelengineering_checks', __name__)
@channelengineering_checks.route("/channelengineering", methods=["POST"])

def channelengineering(all_dataframes,sql_match_tables,errors_dict,project_code,login_info):
	errorLog("Function - channelengineering")
	message = "Custom ChannelEngineering: Start checks."
	statusLog("Starting ChannelEngineering Checks")
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
		if table_name == "tbl_channelengineering":
			tables.append("channelengineering")
			chaneng = all_dataframes[dataframe]
			chaneng['tmp_row'] = chaneng.index

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


                # Robert - If "Other" for the following fields then corresponding comment field required (ie. If bottom = "Other" then bottomcomments requires a value):
                
                # - bottom
                # - determination
                # - leftsideofstructure
                # - rightsideofstructure
                # - structureshape

                # - bottom - # 
                errorLog('Check - If Other for the bottom field then corresponding bottomcomments field is required')
                errorLog('Submitted Data where Bottom field is Other, but no comment:')
                errorLog(chaneng[(chaneng.bottom == 'Other')&(chaneng.bottomcomments.isnull())])
                checkData(chaneng[(chaneng.bottom == 'Other')&(chaneng.bottomcomments.isnull())].tmp_row.tolist(),'bottomcomments','Undefined Error','error','You have entered Other for bottom field, comment is required.',chaneng)

                # - determination - # 
                errorLog('Check - If Other for the determination field then corresponding determinationcomments field is required')
                errorLog('Submitted Data where determination field is Other, but no comment:')
                errorLog(chaneng[(chaneng.determination == 'Other')&(chaneng.determinationcomments.isnull())])
                checkData(chaneng[(chaneng.determination == 'Other')&(chaneng.determinationcomments.isnull())].tmp_row.tolist(),'determinationcomments','Undefined Error','error','You have entered Other for determination field, comment is required.',chaneng)

                # - leftsideofstructure - # 
                errorLog('Check - If Other for the leftsideofstructure field then corresponding leftsidecomments field is required')
                errorLog('Submitted Data where leftsideofstructure field is Other, but no comment:')
                errorLog(chaneng[(chaneng.leftsideofstructure == 'Other')&(chaneng.leftsidecomments.isnull())])
                checkData(chaneng[(chaneng.leftsideofstructure == 'Other')&(chaneng.leftsidecomments.isnull())].tmp_row.tolist(),'leftsidecomments','Undefined Error','error','You have entered Other for leftsideofstructure field, comment is required.',chaneng)

                # - rightsideofstructure - # 
                errorLog('Check - If Other for the rightsideofstructure field then corresponding rightsidecomments field is required')
                errorLog('Submitted Data where rightsideofstructure field is Other, but no comment:')
                errorLog(chaneng[(chaneng.rightsideofstructure == 'Other')&(chaneng.rightsidecomments.isnull())])
                checkData(chaneng[(chaneng.rightsideofstructure == 'Other')&(chaneng.rightsidecomments.isnull())].tmp_row.tolist(),'rightsidecomments','Undefined Error','error','You have entered Other for rightsideofstructure field, comment is required.',chaneng)

                # - structureshape - # 
                errorLog('Check - If Other for the structureshape field then corresponding structureshapecomments field is required')
                errorLog('Submitted Data where structureshape field is Other, but no comment:')
                errorLog(chaneng[(chaneng.structureshape == 'Other')&(chaneng.structureshapecomments.isnull())])
                checkData(chaneng[(chaneng.structureshape == 'Other')&(chaneng.structureshapecomments.isnull())].tmp_row.tolist(),'structureshapecomments','Undefined Error','error','You have entered Other for structureshape field, comment is required.',chaneng)





                
                # Robert - 12/6/2018
                # This check appears to be taken care of in CoreChecks. Need confirmation.
                '''
                # Robert 12/6/2018 #
                # Check - if channeltype == "Engineered" then the following fields are required:
                # - bottom 
                # - structurewidth
                # - structureshape
                # - leftsideofstructure
                # - leftvegetation
                                            
                                            
                # - rightsideofstructure
                # - rightvegetation
                # - vegetation
                # - lowflowpresence
                # - lowflowwidth
                def EngineeredChannelChecks(fieldname):
                    errorLog('Check - if channeltype == Engineered then the %s field is required' % fieldname)
                    errorLog('Submitted Data where channeltype == Engineered but the %s is missing:' % fieldname)
                    errorLog(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng[fieldname].isnull()) ])
                    checkData(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng[fieldname].isnull()) ].tmp_row.tolist(), fieldname, 'Undefined Error', 'error', 'The channeltype is Engineered, but the %s field is missing' % fieldname, chaneng)

                EngineeredChannelChecks('bottom')
                EngineeredChannelChecks('structurewidth')
                EngineeredChannelChecks('structureshape')
                EngineeredChannelChecks('leftsideofstructure')
                EngineeredChannelChecks('leftvegetation')
                EngineeredChannelChecks('rightsideofstructure')
                EngineeredChannelChecks('rightvegetation')
                EngineeredChannelChecks('vegetation')
                EngineeredChannelChecks('lowflowpresence')
                EngineeredChannelChecks('lowflowwidth')
                '''



                # Robert 12/6/2018 #
                # Check - if channeltype == Natural then the following fields should be filled with default empty values (-88 and NR)
                # - bottom 
                # - structurewidth
                # - structureshape
                # - leftsideofstructure
                # - leftvegetation
                # - rightsideofstructure
                # - rightvegetation
                # - vegetation
                # - lowflowpresence
                # - lowflowwidth
                def NaturalChannelCheck(fieldname):
                    errorLog('Check - if channeltype == Natural then the %s field should be filled out as NR' % fieldname)
                    errorLog('Submitted Data where channeltype == Natural but the %s field is not filled with NR:' % fieldname)
                    acceptable_values = ['NR']
                    if fieldname == 'structureshape':
                        acceptable_values.append('Natural')
                    if fieldname == 'bottom':
                        acceptable_values.append('Soft/Natural')
                    errorLog(chaneng[ (chaneng.channeltype == 'Natural') & (~(chaneng[fieldname].isin(acceptable_values))) ])
                    checkData(chaneng[ (chaneng.channeltype == 'Natural') & (~(chaneng[fieldname].isin(acceptable_values))) ].tmp_row.tolist(), fieldname, 'Undefined Error', 'error', 'The channeltype is Natural, but the %s field is not filled with NR' % fieldname, chaneng)

                NaturalChannelCheck('bottom')    
                NaturalChannelCheck('structurewidth')    
                NaturalChannelCheck('structureshape')    
                NaturalChannelCheck('leftsideofstructure')    
                NaturalChannelCheck('leftvegetation')    
                NaturalChannelCheck('rightsideofstructure')    
                NaturalChannelCheck('rightvegetation')    
                NaturalChannelCheck('vegetation')    
                NaturalChannelCheck('lowflowpresence')    
                NaturalChannelCheck('lowflowwidth')    
                


                # Robert - 12/6/2018
                # This check appears to be taken care of in CoreChecks. Need Confirmation.
                '''
                # Robert 12/6/2018
                # Check - if lowflowpresence == "Present" then lowflowwidth is required
                errorLog('Check - if lowflowpresence == Present then lowflowwidth is required')
                errorLog('Submitted Data where lowflowpresence == Present but the lowflowwidth field is missing:')
                errorLog(chaneng[ (chaneng.lowflowpresence == 'Present') & (chaneng.lowflowwidth.isnull()) ])
                checkData(chaneng[ (chaneng.lowflowpresence == 'Present') & (chaneng.lowflowwidth.isnull()) ].tmp_row.tolist(), 'lowflowwidth', 'Undefined Error', 'error', 'The lowflowpresence field is recorded as Present, but the lowflowwidth field is missing', chaneng)
                '''

                # Robert - 12/6/2018
                # This check appears to be taken care of in CoreChecks. Need Confirmation.
                '''
                # Robert 12/6/2018
                # Check - if gradecontrolpresence  == "Present" then gradecontrollocation is required
                errorLog('Check - if gradecontrolpresence == Present then gradecontrollocation is required')
                errorLog('Submitted Data where gradecontrolpresence == Present but the gradecontrollocation field is missing:')
                errorLog(chaneng[ (chaneng.gradecontrolpresence == 'Present') & (chaneng.gradecontrollocation.isnull()) ])
                checkData(chaneng[ (chaneng.lowflowpresence == 'Present') & (chaneng.gradecontrollocation.isnull()) ].tmp_row.tolist(), 'gradecontrollocation', 'Undefined Error', 'error', 'The gradecontrolpresence field is recorded as Present, but the gradecontrollocation field is missing', chaneng)
                '''
               




		## LOGIC ##
		message = "Starting ChannelEngineering Logic Checks"
		errorLog(message)
		statusLog(message)
		## END LOGIC CHECKS ##

		## CUSTOM CHECKS ##
		## END CUSTOM CHECKS ##
		## START MAP CHECK ##
		# get a unique list of stations from stationcode
		list_of_stations = pd.unique(chaneng['stationcode'])
		unique_stations = ','.join("'" + s + "'" for s in list_of_stations)
		## END MAP CHECKS
		## END CHECKS ##

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
		
                #assignment_table = result.groupby(['stationid','lab','analyteclass']).size().to_frame(name = 'count').reset_index()
		# lets reassign the analyteclass field name to species so the assignment query will run properly - check StagingUpload.py for details
		#assignment_table = assignment_table.rename(columns={'analyteclass': 'species'})
		
                message = "Channel Engineering Checks completed successfully"
                errorLog(message)
                statusLog(message)

                return custom_checks, custom_redundant_checks, message, unique_stations
	except Exception as e:
		message = "Critical Error: Failed to run channelengineering checks"	
		errorLog(message)
                errorLog(e)
		state = 1
		return jsonify(message=message,state=state)
