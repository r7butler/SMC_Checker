from flask import Blueprint, request, jsonify, session
import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine, exc, Table, Column, Integer, Unicode, MetaData, String, Text, update, and_, select, func, types
from pandas import DataFrame
from .ApplicationLog import *
import time

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
		errorLog("df_sheet_and_table_name")
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
		
                # NOTE Looks to me like the only two matching columns in result and batch are LabAgencyCode and LabBatch.
                #       Need to confirm with Jeff if this is what we are matching the records on for the Logic Checks.
                #       Answer: YES
                # May 1, 2019 - The Logic Check appears to be working properly - Robert
                # Validated
                
                errorLog("Logic checks - creating sets of batches")
                result_lab_batches = set(zip(result.labbatch, result.labagencycode))
                batch_lab_batches = set(zip(batch.labbatch, batch.labagencycode))
                
                result_notin_batch = result_lab_batches - batch_lab_batches
                batch_notin_result = batch_lab_batches - result_lab_batches

                errorLog("Finding what is in result but not in batch")
                for record in result_notin_batch:
                    checkData(result[(result.labbatch == record[0])&(result.labagencycode == record[1])].tmp_row.tolist(), 'LabBatch/LabAgencyCode', 'Undefined Error', 'error', 'The LabBatch %s and/or the LabAgencyCode %s does not exist in the LabBatch tab. Each record in Results must have a corresponding record in Batch. Records are matched based on LabBatch and LabAgencyCode.' % record, result)

                errorLog("Finding what is in batch but not in result")
                for record in batch_notin_result:
                    checkData(result[(result.labbatch == record[0])&(result.labagencycode == record[1])].tmp_row.tolist(), 'LabBatch/LabAgencyCode', 'Undefined Error', 'error', 'The LabBatch %s and/or the LabAgencyCode %s does not exist in the Result tab. Each record in Batch must have a corresponding record in Results. Records are matched based on LabBatch and LabAgencyCode.' % record, result)




                ###################
		## CUSTOM CHECKS ##
		###################
                statusLog("Starting Chemistry Custom Checks") 
                errorLog("Starting Chemistry Custom Checks")               
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
                    errorLog("function - nameUpdate")
                    errorLog("creating mask dataframe")
                    mask = pd.DataFrame([df[k].isin(v) for k,v in conditions.items()]).T.all(axis = 1)
                    errorLog("extract the appropriate subset of the original dataframe (df)")
                    sub = df[mask]
                    errorLog("Find where the column has the outdated name")
                    errs = sub[sub[field].str.contains(oldname)]
                    errorLog(errs)
                    errorLog("Call the checkData function")
                    checkData(errs.tmp_row.tolist(),field,'Undefined Error','error','%s must now be written as %s.' %(oldname, newname),df)
                    errorLog("nameUpdate function completed")
                

                # Commented out only because we should come up with a way to modify the function 
                # so that the error message is more descriptive to the user.
                # However, the code seems very efficient and powerful and could be of good use later.
                # In this particular case, we only call it twice, so for now, I will do it the old school way.
                '''
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
                '''



                ############################
                # SMC RAPHAEL MAZOR AUDITS #
                ############################
                errorLog("Begin Raphael Mazor Audits")

                # If MatrixName is samplewater, blankwater, or labwater then the following AnalyteNames must be updated:
                #   Nitrate as NO3 -> Nitrate as N
                #   Phosphorus as PO4 -> Phosphorus as P
                # May 1, 2019 - This check appears to be working properly - Robert
                # Validated

                errorLog("If MatrixName is samplewater, blankwater, or labwater then some AnalyteNames must be updated:")
                nameUpdate(result, 'analytename',{'matrixname':['samplewater','blankwater','labwater']}, 'Nitrate as NO3','Nitrate as N')
                nameUpdate(result, 'analytename',{'matrixname':['samplewater','blankwater','labwater']}, 'Phosphorus as PO4', 'Phosphorus as P')
                errorLog("Done with nameUpdate checks")


                
                # If MatrixName is samplewater, blankwater, or labwater then the following must hold true:                
                #   AnalyteName = Ash Free Dry Mass -> Unit = mg/cm2
                #   AnalyteName = Chlorophyll a -> Unit = ug/cm2
                # May 1, 2019 - This check appears to be working properly - Robert
                # Validated

                errorLog("If MatrixName is samplewater, blankwater, or labwater then some units must be matched:")
                #exactMatch(result, 'unit', {'matrixname':['samplewater','blankwater','labwater'], 'analytename': ['Ash Free Dry Mass']}, 'mg/cm2')
                #exactMatch(result, 'unit', {'matrixname':['samplewater','blankwater','labwater'], 'analytename': ['Chlorophyll a']}, 'ug/cm2')
                
                errorLog("checking units where analytename is Ash Free Dry Mass")
                checkData(result[(result.matrixname.isin(['samplewater','blankwater','labwater']))&(result.analytename == 'Ash Free Dry Mass')&(result.unit != 'mg/cm2')].tmp_row.tolist(), 'Unit', 'Undefined Error', 'error', 'If the AnalyteName is Ash Free Dry Mass, the Unit must be mg/cm2', result)

                errorLog("checking units where analytename is Chlorophyll a")
                checkData(result[(result.matrixname.isin(['samplewater','blankwater','labwater']))&(result.analytename == 'Chlorophyll a')&(result.unit != 'ug/cm2')].tmp_row.tolist(), 'Unit', 'Undefined Error', 'error', 'If the AnalyteName is Chlorophyll a, the Unit must be ug/cm2', result)






                ############################                
                #       SWAMP AUDITS       #
                ############################
                
                errorLog("Begin SWAMP Audits")

                # if resqualcode is NR or ND then result must be negative.
                # Also comment is required
                # Both Validated
                errorLog("if resqualcode is NR or ND then result must be equal to -88")
                checkData(result[((result.resqualcode == 'NR')|(result.resqualcode == 'ND')) & ((result.result.astype(float) > 0))].tmp_row.tolist(), "Result", "Undefined Warning", "warning", 'ResQualCode is NR or ND. Result value must be negative.', result)

                errorLog("if resqualcode is NR or ND then labresultcomments should not be empty")
                checkData(result[((result.resqualcode == 'NR')|(result.resqualcode == 'ND')) & ((result.labresultcomments == '')|(result.labresultcomments.isnull()))].tmp_row.tolist(), "LabResultComments", "Undefined Warning", "warning", 'ResQualCode is NR or ND, so you should enter a comment under the \\"labresultcomments\\" field.', result)




                # NOTE Below code has not been written. 4/25/19
                # If resqualcode is DNQ then result < rl
                # if result == -88, then resqualcode needs to be ND
                # NOTE NEED if mdl < result < rl THEN resqualcode needs to be DNQ and vice versa
                # NOTE if result is below MDL but not -88, then resqualcode needs to be DNQ

                # NOTE above is what we discussed with Jeff. However, these won't work since they contradict each other. 5/3/2019
                #       We need the following to meet the criteria he wants:
                #       1) if result < rl but result != -88 then resqualcode should be DNQ
                #       2) if result = -88 then resqualcode should be ND
                # Validated
                
                # Validated
                errorLog("If result is less than mdl but NOT -88, then resqualcode should be DNQ")
                checkData(result[((result.result.astype(float) < result.rl.astype(float))&(result.result.astype(float) != -88))&(result.resqualcode.astype(str) != 'DNQ')].tmp_row.tolist(), "ResQualCode", 'Undefined Warning', 'warning', "If result is less than RL, but not -88, then ResQualCode should be DNQ", result)

                # Validated
                errorLog("If Result is -88 then resqualcode needs to be ND")
                checkData(result[ (result.result.astype(str) == '-88') & (result.resqualcode != 'ND') ].tmp_row.tolist(), "ResQualCode", "Undefined Warning", "warning", "If Result is -88 then the ResQualCode should be ND", result)
                
                
                # RL and MDL cannot both be -88
                # Validated
                errorLog("RL and MDL cannot both be -88")
                errorLog(result[(result.rl.astype(str) == '-88') & (result.mdl.astype(str) == '-88')])
                checkData(result[(result.rl.astype(str) == '-88') & (result.mdl.astype(str) == '-88')].tmp_row.tolist(), "RL", "Undefined Warning", "warning", "The MDL and RL cannot both be -88", result)
                
                # RL cannot be less than MDL                
                # Validated
                errorLog("RL cannot be less than MDL")
                checkData(result[result.rl < result.mdl].tmp_row.tolist(), "RL", "Undefined Warning", "warning", "The RL cannot be less than MDL", result)
                
                
                # If SampleTypeCode is in the set MS1, MS2, LCS, CRM, MSBLDup, BlankSp and the Unit is NOT % THEN......
                # Expected Value cannot be zero
                # NOTE confirm with Jeff: Should LabBlank be included in here?
                #       NO, LabBlank is not part of this check
                # Ask Jeff if it should be unit is "%" or "% recovery"
                # May 1, 2019 - This check appears to be working properly - Robert
                # Validated

                errorLog("line 281, expected Value Check")
                checkData(result[((result.sampletypecode.isin(['MS1', 'MS2', 'LCS', 'CRM', 'MSBLDup', 'BlankSp'])) & (result.unit != "%")) & (result.expectedvalue.astype(str) == '0')].tmp_row.tolist(), "ExpectedValue", "Undefined Warning", "warning", "Expected Value required based on SampleTypeCode", result)
                checkData(result[((result.sampletypecode.isin(['MS1', 'MS2', 'LCS', 'CRM', 'MSBLDup', 'BlankSp'])) & (result.unit != "%")) & (result.expectedvalue.astype(str) == '-88')].tmp_row.tolist(), "ExpectedValue", "Undefined Warning", "warning", "Expected Value required based on SampleTypeCode", result)
                

                # If multiple records have equal labbatch, analytename, and dilfactor                
                # Then MDL values for those records must also be equivalent
                # May 1, 2019 - This check appears to be working properly - Robert
                # Validated
                errorLog("For each record in a LabBatch/AnalyteName/DilFactor group, MDL values should all be the same:")
                groups = result.groupby(['labbatch','analytename','dilfactor'])['mdl'].apply(list).reset_index(name = 'mdl counts')
                same_mdls = groups['mdl counts'].apply(lambda x: x[1:] == x[:-1])
                bad_groups = groups.where(~same_mdls).dropna()
                errorLog(bad_groups)
                for i in bad_groups.index:
                    lb = bad_groups.labbatch[i]
                    an = bad_groups.analytename[i]
                    df = bad_groups.dilfactor[i]
                    checkData(result[(result.labbatch == lb)&(result.analytename == an)&(result.dilfactor == df)].tmp_row.tolist(),'MDL','Undefined Warning','warning','Multiple MDLs reported for %s in Batch %s. For each LabBatch, Analytes with equivalent Dilution Factors must have equivalent  MDL values.' %(an,lb), result)

                # If multiple records have equal labbatch, analytename, and dilfactor                
                # Then RL values for those records must also be equivalent
                # May 1, 2019 - This check appears to be working properly - Robert
                # Validated
                errorLog("For each record in a LabBatch/AnalyteName/DilFactor group, RL values should all be the same:")
                groups = result.groupby(['labbatch','analytename','dilfactor'])['rl'].apply(list).reset_index(name = 'rl counts')
                same_rls = groups['rl counts'].apply(lambda x: x[1:] == x[:-1])
                bad_groups = groups.where(~same_rls).dropna()
                errorLog(bad_groups)
                for i in bad_groups.index:
                    lb = bad_groups.labbatch[i]
                    an = bad_groups.analytename[i]
                    df = bad_groups.dilfactor[i]
                    checkData(result[(result.labbatch == lb)&(result.analytename == an)&(result.dilfactor == df)].tmp_row.tolist(),'RL','Undefined Warning','warning','Multiple RLs reported for %s in Batch %s. For each LabBatch, Analytes with equivalent Dilution Factors must have equivalent RL values.' %(an,lb), result)

                # If multiple records have equal labbatch, analytename                
                # Then MethodNames should also be equivalent
                # May 1, 2019 - This check appears to be working properly
                # Validated
                errorLog("For each record in a LabBatch/AnalyteName group, MethodNames should all be the same:")
                groups = result.groupby(['labbatch','analytename'])['methodname'].apply(list).reset_index(name = 'methods')
                same_methods = groups['methods'].apply(lambda x: x[1:] == x[:-1])
                bad_groups = groups.where(~same_methods).dropna()
                errorLog(bad_groups)
                for i in bad_groups.index:
                    lb = bad_groups.labbatch[i]
                    an = bad_groups.analytename[i]
                    checkData(result[(result.labbatch == lb)&(result.analytename == an)].tmp_row.tolist(),'MethodName','Undefined Warning','warning','Different methods used for %s in Batch %s. For each LabBatch, the same MethodName should be used for the same analyte.' %(an,lb), result)

                # If multiple records have equal labbatch, analytename
                # Then Unit should also be equivalent
                # May 1, 2019 - This check appears to be working properly - Robert
                # Validated
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
                # Validated
                errorLog("If LabSubmissionCode is A, MD, or QI, a LabBatchComment is required.")
                bad_records = batch[(batch.labsubmissioncode.isin(['A','MD','QI']))&((batch.labbatchcomments.isnull())|(batch.labbatchcomments == ''))]
                errorLog(bad_records)
                checkData(bad_records.tmp_row.tolist(),'LabBatchComments','Undefined Warning','warning','LabSubmissionCode is A, MD, or QI. LabBatchComment is required.', batch)
                
                
                # Check - If SampleTypeCode == 'Grab' or 'LabBlank' or 'Integrated' then expectedvalue must be -88 (unless the unit is percent recovery)
                # May 1, 2019 - This check appears to be working properly - Robert
                # Validated
                errorLog("Check - If SampleTypeCode == 'Grab' or 'LabBlank' or 'Integrated' then expectedvalue must be -88 (unless the unit is percent recovery)")
                bad_records = result[((result.sampletypecode.isin(['Grab', 'LabBlank', 'Integrated'])) & (result.unit != '% recovery')) & (result.expectedvalue.astype(str) != '-88') ]
                errorLog(bad_records)
                checkData(bad_records.tmp_row.tolist(), 'ExpectedValue', 'Undefined Error', 'error', "If SampleTypeCode is either Grab, LabBlank, or Integrated, then ExpectedValue must be -88 (unless the unit is percent recovery)", result)
                
                
                # Check - if sampletypecode 'MS1' or 'LCS' or 'CRM' or 'BlankSp' then expected value cannot be -88
                # Validated
                errorLog("If sampletypecode is 'MS1, 'LCS', 'BlankSp' or 'CRM' then expectedvalue cannot be -88")
                errorLog(result[(result.sampletypecode.isin(['MS1', 'LCS', 'CRM', 'BlankSp'])) & (result.expectedvalue.astype(str) == '-88')])
                checkData(result[(result.sampletypecode.isin(['MS1', 'LCS', 'CRM', 'BlankSp'])) & (result.expectedvalue.astype(str) == '-88')].tmp_row.tolist(), 'ExpectedValue', 'Undefined Error', 'error', "If sampletypecode is MS1, LCS, LabBlank, BlankSp, or CRM then expectedvalue cannot be -88", result)

                # Check - if unit == '% recovery', then expectedvalue cannot have -88
                # Validated
                errorLog("if unit == % recovery, then expectedvalue cannot have -88")
                errorLog(result[(result.unit == '% recovery') & (result.expectedvalue.astype(str) == '-88')])
                checkData(result[(result.unit == '% recovery') & (result.expectedvalue.astype(str) == '-88')].tmp_row.tolist(), 'ExpectedValue', 'Undefined Error', 'error', 'If unit is percent recovery, then expected value cannot be blank or -88', result)



                ########################################
                ##                                    ##
                ## Extra Checks from Submission Guide ##
                ##                                    ##
                ########################################


                # Here are checks that were found in submission guide



                # Submission Guide page 8 - Results versus ExpectedValue
                '''
                The Expected Value is only reported for matrix spikes, blank spikes, and surrogates.
                An Expected Value of -88 will be reported for all other samples.
                '''
                # NOTE This check appears to have been taken care of in the SWAMP Audits
                # I have interpreted this to mean the following:
                #       If the SampleTypeCode is 'MS1', 'LCS', 'CRM', 'MSBLDup' or 'BlankSp' then ExpectedValue cannot be -88
                #       If the SampleTypeCode is NOT one mentioned above, then the ExpectedValue MUST be -88, as long as the unit is NOT % recovery
                #       Confirmed with Jeff 4/30/2019
                # Also this check is taken care of in the SWAMP Audits.
                





                # Submission Guide page 8 - Results versus ExpectedValue
                '''
                For matrix spikes, the ExpectedValue shall NOT be corrected for native concentrations. In contrast, 
                the result for matrix spikes shall have the native concentration subtracted, so that the result and 
                the ExpectedValue are expected to be the same.
                '''
                # NOTE The above check is something that I am not sure that we can even check. Confirm with Jeff.
                #       ANSWER: Can't Check it.






                # Submission Guide - (bottom of page 8, top of page 9) - Lab Replicates
                '''
                Lab Replicates are defined as replicate samples taken from the same sample bottle.
                The result for each replicate will be numbered starting from one.
                '''
                # NOTE Confirm with Jeff that we want to write a check for this.
                #       I think it should be easy to write. Just group by the primary key (except LabReplicate) and make sure there's a 1 in there
                #       If there is more than one value for LabReplicate, make sure the values are consecutive integers, and there is a one in there
                #       If There is only one value for LabReplicate, make sure that value is 1.
                #
                # NOTE Unique records are determined by:
                #       ['stationcode', 'sampledate', 'labbatch', 
                #        'matrixname', 'sampletypecode', 
                #        'analytename', 'fractionname', 
                #        'fieldreplicate', 'labreplicate', 
                #        'labsampleid', 'labagencycode']
                #   That is according to the submission guide (bottom of page 9, top of page 10)
                # Validated

                errorLog("LabReplicates should be numbered starting from one")
                
                grouping_cols = ['stationcode', 'labbatch', 'sampledate',
                                 'matrixname', 'sampletypecode', 'analytename', 
                                 'fractionname', 'fieldreplicate', 
                                 'labsampleid', 'labagencycode']

                errorLog("grouping by the grouping columns")
                grouped = result.groupby(grouping_cols)['labreplicate', 'tmp_row'].apply(lambda x: tuple([x.labreplicate.tolist(), x.tmp_row.tolist()]))
                
                errorLog("creating repsandrows column")
                grouped = grouped.reset_index(name='repsandrows')
                
                errorLog("creating reps column")
                grouped['reps'] = grouped.repsandrows.apply(lambda x: x[0])
                
                errorLog("creating rows column")
                grouped['rows'] = grouped.repsandrows.apply(lambda x: x[1])
                
                errorLog("dropping the temporary repsandrows column")
                grouped.drop('repsandrows', axis=1, inplace=True)

                errorLog("checking to see if LabReplicates were labeled correctly")
                grouped['passed'] = grouped.reps.apply(lambda x: (sum(x) == ((len(x) * (len(x) + 1)) / 2)) & ([num > 0 for num in x] == [True] * len(x)) )
                errorLog("checking to see if LabReplicates were labeled correctly: DONE")

                errorLog("extracting bad rows")
                badrows = [item for sublist in grouped[grouped.passed == False].rows.tolist() for item in sublist]
                errorLog("extracting the badrows: DONE")

                errorLog("running checkData function")
                checkData(badrows, "LabReplicate", "Undefined Warning", "warning", "Labreplicates must be numbered starting from one, and they must be consecutive", result)
                errorLog("running checkData function: DONE")






                # Submission Guide page 8 - Special Information for Matrix Spikes
                '''
                The SampleTypeCode for matrix spikes must be MS1 (LabReplicate 1, or 2 for the duplicate) and the 
                same LabSampleIDs must be the same for both. MS2 is only used for spikes of field duplicates and is NOT
                a required SampleTypeCode for the SMC Program
                '''
                # NOTE This seems similar to what we did in Bight Chem. 
                #           It is noteworthy also that in the database (tbl_chemistryresults) there are no sampletypecodes of MS2. Only MS1's
                
                # NOTE There is another requirement for Labreplicates that they must be numbered starting from 1. (Top of page 9)
                #           It would make sense to put that check before this one (I did put it before)
                
                # NOTE This check is actually taken care of in the above LabReplicate Check. However, I believe that the purpose of this check
                #           is to ensure that Matrix spike duplicates are NOT determined with SampleTypeCode MS2.
                #           For this reason, I will issue a warning wherever they put SampleTypeCode as MS2.
                # Validated

                # We want to issue a warning wherever the SampleTypeCode is MS2
                errorLog("Matrix spike duplicates are determined by LabReplicate 1 and 2")
                checkData(result[result.sampletypecode == 'MS2'].tmp_row.tolist(), 'SampleTypeCode', 'Undefined Warning', 'warning', 'MS2 is only used for spikes of field duplicates, and it is not a required sampletype for the SMC Program', result)




                # Submission Guide page 8 - Recovery Corrected Data
                '''
                Recovery corrected data are NOT reported because they can be calculated using 
                the ExpectedValue of the reference material processed within the same batch.
                '''
                # NOTE Confirm with Jeff if this is even something we can check or not.
                #       I am not even sure what this means.
                #       Can't check it






                # Submission Guide page 9 - Non-Detects
                '''
                If the result is not reportable, a result qualifier of "ND" should be used, and the result reported as -88. 
                In the case where the result is below the method detection limit, or the reporting limit, a qualifier of DNQ may be used.
                '''
                # NOTE Confirm with Jeff what this is actually saying. I don't quite understand.
                #       I think the first part of the check is taken care of in the SWAMP Audits starting at line 273
                #       Yes.



                # Submission Guide page 9 - QA Samples Generated in the Lab
                '''
                QA Samples not performed on site-collected samples (e.g., lab blanks, reference material)
                shall be given a stationcode of LABQA. All QA samples performed on site-collected samples
                (e.g. matrix spikes) should be given the relevant station code.
                '''
                # NOTE Ask Jeff if this is something we can even check
                # ANSWER: YES. 
                
                errorLog("Submission Guide Page 9 Check - QA Samples Generated in the Lab")
                errorLog(result[(result.sampletypecode.isin(['LCS', 'CRM', 'BlankSp', 'LabBlank']))&(result.stationcode != 'LABQA')])

                # If sampletypecode is CRM, LCS, LabBlank or BlankSp then stationcode should be LABQA.
                # This check appears to be working properly
                # Validated
                checkData(result[(result.sampletypecode.isin(['LCS', 'CRM', 'BlankSp', 'LabBlank']))&(result.stationcode != 'LABQA')].tmp_row.tolist(), "StationCode", "Undefined Warning", "warning", "For QA Samples generated in the Lab (CRM, LCS, BlankSp, LabBlank) the stationcode should be LABQA", result)
                errorLog(result[(result.sampletypecode.isin(['LCS', 'CRM', 'BlankSp', 'LabBlank']))&(result.stationcode != 'LABQA')].tmp_row.tolist())
                
                # Also Jeff wants to make sure that if the sampletype is MS1, then the stationcode is NOT LABQA <- This one works
                # this check appears to be working properly
                # Validated
                checkData(result[(result.sampletypecode == 'MS1')&(result.stationcode == 'LABQA')].tmp_row.tolist(), 'StationCode', 'Undefined Warning', 'warning', 'If the SampleTypeCode is MS1, then the stationcode should not be LABQA', result)


            


                # Submission Guide page 9 - Non-project QA Samples
                '''
                Required QA analyses are sometimes performed on samples from other projects, in batches mixed with SMC samples.
                In these cases, all relevant QA data must still be submitted. Matrix spikes and lab duplicates not performed
                on SMC samples shall be given a stationcode of 000NONPJ, and a QACode of DS,
                (i.e. "Batch quality assurance data from another project")
                '''
                # NOTE Ask Jeff if this is something we can even check
                # ANSWER: YES. This check is covered with CoreChecks.





                # Submission Guide page 9
                errorLog("Submission Guide page 9")
                '''
                Certain analytes are expected to be reported with specific fractions and units.
                Please refer to the table in the appendix of this guide to see the acceptable combinations.
                '''
                # NOTE The dataframe that has the required analytename, fraction and unit combinations is stored in a csv file
                #          Full path is "/var/www/smc/proj/files/analytefractionunitcombos.csv"
                #          Relative to where this ChemistryChecks.py file is, the path is just "files/analytefractionunitcombos.csv"
                # May 1, 2019 - This check appears to be working properly - Robert
                # Validated

                required_combos = pd.read_excel('/var/www/smc/proj/files/analytefractionunitcombos.xlsx')
                required_combos.rename(columns={'fraction':'required_fractionname', 'unit': 'required_unit'}, inplace=True)
                errorLog("required analytename, fraction, and unit combinations:")
                errorLog(required_combos.head(7))
                
                errorLog('creating tmp_result dataframe')
                tmp_result = result

                errorLog('creating required combos sets')
                required_combos['acceptable_combos'] = required_combos.apply(lambda x: set([x.required_fractionname, x.required_unit]), axis = 1)
                errorLog(required_combos.head(7))

                # we assume required_combos is NOT empty since we already know what it is (the csv file we are reading in)
                errorLog("Creating lists of acceptable combinations of fractions and units for each analyte")
                required_combos = required_combos.groupby('analytename')['acceptable_combos'].apply(list).reset_index()
                errorLog(required_combos.head(7))
                errorLog("Creating lists of acceptable combinations of fractions and units for each analyte: DONE")
                
                errorLog("creating sets of fraction unit combos found in the submitted dataframe")
                errorLog("tmp_result.head()")
                errorLog(tmp_result[['tmp_row', 'analytename', 'fractionname', 'unit']].head())
                errorLog(tmp_result.apply(lambda x: set([x.fractionname, x.unit]), axis = 1))
                tmp_result['fraction_unit_combos'] = tmp_result[['fractionname','unit']].apply(lambda x: set([x.fractionname, x.unit]), axis = 1)
                errorLog(tmp_result.head(7))
                errorLog("creating sets of fraction unit combos found in the submitted dataframe: DONE")
                
                errorLog("Merging tmp_result with the required combos dataframe")
                tmp_result = tmp_result.merge(required_combos, on='analytename', how='inner')
                errorLog(tmp_result.head(7))
                errorLog("Merging tmp_result with the required combos dataframe: DONE")
                
                errorLog("creating column of booleans indicating whether the row passed the check or not")
                tmp_result['passed_check'] = tmp_result[['fraction_unit_combos','acceptable_combos']].apply(lambda x: x.fraction_unit_combos in x.acceptable_combos, axis=1)
                errorLog(tmp_result)
                errorLog("creating column of booleans indicating whether the row passed the check or not: DONE")
                

                errorLog(tmp_result[tmp_result.passed_check == False])
                checkData(tmp_result[(tmp_result.passed_check == False)&(tmp_result.unit != "% recovery")].tmp_row.tolist(), 'analytename', 'Undefined Warning', 'warning', "This analyte was submitted without the proper fraction and unit combination. For more information, refer to the <a href=\'ftp://ftp.sccwrp.org/pub/download/smcstreamdata/SubmissionGuides/SMCChemistrySubmissionGuide_10-03-2012.pdf\'>submission guide</a> on page 18.", result)

                result.drop('fraction_unit_combos', axis=1,inplace=True)




                # Submission Guide page 9 - Field-Based versus Lab-Based Measurements
                '''
                The chemistry tables are meant to store all water chemistry measurements that occur in a lab. 
                Several water chemistry analytes that are measured in the field should instead be stored in the physical habitat database
                (e.g., Oxygen, Dissolved; pH; Alkalinity as CaCO3; Salinity; SpecificConductivity; Temperature; and Turbidity) 
                Only include them with other chemistry results if they were analyzed in a lab.
                '''
                # NOTE Ask Jeff if this is something we can even check
                # ANSWER: NO


                # NEW CHECK #
                # Based on Discussion with Jeff during the week of 4/22
                # If sampletypecode is MS1 then the matrixname should be samplewater
                # May 1, 2019 - This check appears to be working properly - Robert
                # Validated
                errorLog("If sampletypecode is MS1 then the matrixname should be samplewater")
                checkData(result[(result.sampletypecode == 'MS1') & (result.matrixname != 'samplewater')].tmp_row.tolist(), "MatrixName", "Undefined Warning", "warning", "If the SampleTypeCode is MS1 then the MatrixName should be samplewater", result)


                ###################
		## MAP CHECKS ##
		###################
		# get a unique list of stations from stationcode
		list_of_stations = pd.unique(result['stationcode'])
                unique_stations = ','.join("'" + s + "'" for s in list_of_stations)
		## END MAP CHECKS
                
                ## RETRIEVE ERRORS ##
                custom_checks = ""
                custom_redundant_checks = ""
                custom_errors = []
                custom_warnings = []
                custom_redundant_warnings = []
		
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
	except Exception as errormsg:
		message = "Critical Error: Failed to run chemistry checks"
		errorLog(errormsg)
		errorLog(message)
		state = 1
		return jsonify(message=message,state=state)
