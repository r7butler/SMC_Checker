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

chemistry_checks = Blueprint('chemistry_checks', __name__)

@chemistry_checks.route("/chemistry", methods=["POST"])

def chemistry(all_dataframes,sql_match_tables,errors_dict,project_code,login_info):
	errorLog("Function - chemistry")
	message = "Custom Chemistry: Start checks."
	statusLog("Starting Chemistry Checks")
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
		if table_name == "tbl_chemistrybatch":
			tables.append("batch")
			batch = all_dataframes[dataframe]
			batch['tmp_row'] = batch.index
                if table_name == 'tbl_chemistryresults':
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
					errorsCount(errors_dict,'warnings')
                                        
		def checkLogic(statement,column,warn_or_error,error_label,human_error,dataframe):
			for item_number in statement:
				unique_error = '{"column": "%s", "error_type": "%s", "error": "%s"}' % (column,warn_or_error,human_error)
				addErrorToList("custom_errors",item_number,unique_error,dataframe)
				errorsCount(errors_dict,'custom')

		######################################
                ## ESTABLISH CONNECTION TO DATABASE ##
                ######################################
                eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
                
                
                
                
                ###########
                ## LOGIC ##
                ###########
		message = "Starting Chemistry Logic Checks"
		errorLog(message)
		statusLog(message)
		



                ###################
		## CUSTOM CHECKS ##
		###################
                
                ##########################
                # CUSTOM CHECK FUNCTIONS #
                ##########################
                
                def nameUpdate(df, field, conditions, oldname, newname):
                    """
                    DESCRIPTION:
                    This function returns an error if the field in df under conditions contains an oldname.
                    
                    PARAMETERS:
                    
                    df - pandas dataframe of interest
                    field - string of the field of interest
                    conditions - a dictionary of conditions placed on the dataframe (i.e. {'field':['condition1',...]})
                    oldname - string of the name returned to user if found in field
                    newname - string of the suggested fix for oldname.
                    """
                    mask = pd.DataFrame([df[k].isin(v) for k,v in conditions.items()]).T.all(axis = 1)
                    sub = df[mask]
                    errs = sub[sub[field].str.contains(oldname)]
                    errorLog(errs)
                    checkData(errs.tmp_row.tolist(),field,'Undefined Error','error','%s must now be written as %s.' %(oldname, newname),df)

                def exactMatch(df, field, conditions, value):
                   """
                   DESCRIPTION:
                   This function returns an error if field value in df under conditions does not exactly match specified value

                   PARAMETERS:
                   df - pandas dataframe of interest
                   field - string of the field of interest
                   conditions - a dictionary of conditions placed on the dataframe (i.e. {'field':['condition1',...]})
                   value - string or numeric of exact value needed
                   """
                   mask = pd.DataFrame([df[k].isin(v) for k,v in conditions.items()]).T.all(axis = 1)
                   sub = df[mask]
                   errs = sub[sub[field] != value]
                   errorLog(errs)
                   checkData(errs.tmp_row.tolist(), field,'Undefined Error','error','%s mismatch. %s should be %s' %(field,field,value), result)
                

                ############################
                # SMC RAPHAEL MAZOR AUDITS #
                ############################

                # If MatrixName is samplewater, blankwater, or labwater then the following AnalyteNames must be updated:
                #   Nitrate as NO3 -> Nitrate as N
                #   Phosphorus as PO4 -> Phosphorus as P
                errorLog("If MatrixName is samplewater, blankwater, or labwater then some AnalyteNames must be updated:")
                nameUpdate(result, 'analytename',{'matrixname':['samplewater','blankwater','labwater']}, 'Nitrate as NO3','Nitrate as N')
                nameUpdate(result, 'analytename',{'matrixname':['samplewater','blankwater','labwater']}, 'Phosphorus as PO4', 'Phosphorus as P')

                # If MatrixName is samplewater, blankwater, or labwater then the following must hold true:                
                #   AnalyteName = Ash Free Dry Mass -> Unit = mg/cm2
                #   AnalyteName = Chlorophyll a -> Unit = ug/cm2
                errorLog("If MatrixName is samplewater, blankwater, or labwater then some units must be matched:")
                exactMatch(result, 'unit', {'matrixname':['samplewater','blankwater','labwater'], 'analytename': ['Ash Free Dry Mass']}, 'mg/cm2')
                exactMatch(result, 'unit', {'matrixname':['samplewater','blankwater','labwater'], 'analytename': ['Chlorophyll a']}, 'ug/cm2')


                ############################                
                #       SWAMP AUDITS       #
                ############################

                # If result < 0, then resqualcode must be either ND or NR
                errorLog(" If result < 0, then resqualcode must be either ND or NR")
                checkData(result[ (result.result < 0) & ((result.resqualcode != 'NR') | (result.resqualcode != 'ND'))].tmp_row.tolist(), "Result", "Undefined Warning", "warning", "Result value is negative. ResQualCode must equal ND or NR", result)

                # if resqualcode is NR then result must be equal to -88 AND labresultcomment must not be empty                
                errorLog("if resqualcode is NR then result must be equal to -88 AND labresultcomment must not be empty")
                checkData(result[(result.resqualcode == 'NR') & (result.result != -88)].tmp_row.tolist(), "Result", "Undefined Warning", "warning", "ResQualCode is NR. Result value must be -88", result)
                checkData(result[(result.resqualcode == 'NR') & (result.labresultcomments == '')].tmp_row.tolist(), "LabResultComment", "Undefined Warning", "warning", "ResQualCode is NR. A LabResultComment is Required", result)

                # If resqualcode is DNQ then mdl < result < rl                
                errorLog("If resqualcode is DNQ then mdl < result < rl")
                checkData(result[ (result.resqualcode == 'DNQ') & ((result.result < result.mdl) | (result.result > result.rl))].tmp_row.tolist(), "Result", "Undefined Warning", "warning", "Result was not within the MDL and RL for ResQualCode = DNQ", result)

                # RL and MDL cannot both be -88
                errorLog("RL and MDL cannot both be -88")
                checkData(result[(result.rl == -88) & (result.mdl == -88)].tmp_row.tolist(), "RL", "Undefined Warning", "warning", "The MDL and RL cannot both be -88", result)

                # RL cannot be less than MDL                
                errorLog("RL cannot be less than MDL")
                checkData(result[result.rl < result.mdl].tmp_row.tolist(), "RL", "Undefined Warning", "warning", "The RL cannot be less than MDL", result)

                # If SampleTypeCode is in the set MS1, MS2, LCS, CRM, MSBLDup and the Unit is NOT % THEN......
                # Expected Value cannot be zero
                errorLog("line 281, expected Value Check")
                checkData(result[((result.sampletypecode.isin(['MS1', 'MS2', 'LCS', 'CRM', 'MSBLDup'])) & (result.unit != "%")) & (result.expectedvalue == 0)].tmp_row.tolist(), "ExpectedValue", "Undefined Warning", "warning", "Expected Value required based on SampleTypeCode", result)

                # If multiple records have equal labbatch, analytename, and dilfactor                
                # Then MDL values for those records must also be equivalent
                errorLog("For each record in a LabBatch/AnalyteName/DilFactor group, MDL values should all be the same:")
                groups = result.groupby(['labbatch','analytename','dilfactor'])['mdl'].apply(list).reset_index(name = 'mdl counts')
                same_mdls = groups['mdl counts'].apply(lambda x: x[1:] == x[:-1])
                bad_groups = groups.where(~same_mdls).dropna()
                errorLog(bad_groups)
                for i in bad_groups.index:
                    lb = bad_groups.labbatch[i]
                    an = bad_groups.analytename[i]
                    df = bad_groups.dilfactor[i]
                    checkData(result[(result.labbatch == lb)&(result.analytename == an)&(result.dilfactor == df)].tmp_row.tolist(),'MDL','Undefined Warning','warning','For LabBatch/AnalyteName/DilFactor = %s/%s/%s, all MDL values must be equivalent.' %(lb,an,df), result)

                # If multiple records have equal labbatch, analytename, and dilfactor                
                # Then RL values for those records must also be equivalent
                errorLog("For each record in a LabBatch/AnalyteName/DilFactor group, RL values should all be the same:")
                groups = result.groupby(['labbatch','analytename','dilfactor'])['rl'].apply(list).reset_index(name = 'rl counts')
                same_rls = groups['rl counts'].apply(lambda x: x[1:] == x[:-1])
                bad_groups = groups.where(~same_rls).dropna()
                errorLog(bad_groups)
                for i in bad_groups.index:
                    lb = bad_groups.labbatch[i]
                    an = bad_groups.analytename[i]
                    df = bad_groups.dilfactor[i]
                    checkData(result[(result.labbatch == lb)&(result.analytename == an)&(result.dilfactor == df)].tmp_row.tolist(),'RL','Undefined Warning','warning','For LabBatch/AnalyteName/DilFactor = %s/%s/%s, all RL values must be equivalent.' %(lb,an,df), result)

                # If multiple records have equal labbatch, analytename                
                # Then MethodNames should also be equivalent
                errorLog("For each record in a LabBatch/AnalyteName group, MethodNames should all be the same:")
                groups = result.groupby(['labbatch','analytename'])['methodname'].apply(list).reset_index(name = 'methods')
                same_methods = groups['methods'].apply(lambda x: x[1:] == x[:-1])
                bad_groups = groups.where(~same_methods).dropna()
                errorLog(bad_groups)
                for i in bad_groups.index:
                    lb = bad_groups.labbatch[i]
                    an = bad_groups.analytename[i]
                    checkData(result[(result.labbatch == lb)&(result.analytename == an)].tmp_row.tolist(),'MethodName','Undefined Warning','warning','For LabBatch/AnalyteName = %s/%s, same MethodName should be used.' %(lb,an), result)

                # If multiple records have equal labbatch, analytename
                # Then Unit should also be equivalent
                errorLog("For each record in a LabBatch/AnalyteName group, Unit should all be the same:")
                groups = result.groupby(['labbatch','analytename'])['unit'].apply(list).reset_index(name = 'units')
                same_units = groups['units'].apply(lambda x: x[1:] == x[:-1])
                bad_groups = groups.where(~same_units).dropna()
                errorLog(bad_groups)
                for i in bad_groups.index:
                    lb = bad_groups.labbatch[i]
                    an = bad_groups.analytename[i]
                    checkData(result[(result.labbatch == lb)&(result.analytename == an)].tmp_row.tolist(),'Unit','Undefined Warning','warning','For LabBatch/AnalyteName = %s/%s, same Unit should be used.' %(lb,an), result)


                # If LabSubmissionCode is A, MD, or QI
                # Then LabBatchComments are required
                errorLog("If LabSubmissionCode is A, MD, or QI, a LabBatchComment is required.")
                bad_records = batch[(batch.labsubmissioncode.isin(['A','MD','QI']))&(batch.labbatchcomments.isnull())]
                errorLog(bad_records)
                checkData(bad_records.tmp_row.tolist(),'LabBatchComments','Undefined Warning','warning','LabSubmissionCode is A, MD, or QI. LabBatchComment is required.', batch)





                ###################
		## MAP CHECKS ##
		###################
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

		errorLog(message)
		return custom_checks, custom_redundant_checks, message, unique_stations
	except ValueError:
		message = "Critical Error: Failed to run chemistry checks"	
		errorLog(message)
		state = 1
		return jsonify(message=message,state=state)
