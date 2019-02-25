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





###########################
## BEGIN TAXONOMY SCRIPT ##
###########################
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
		if table_name == "tbl_taxonomysampleinfo":
			tables.append("sampleinfo")
			sampleinfo = all_dataframes[dataframe]
			sampleinfo['tmp_row'] = sampleinfo.index
		if table_name == "tbl_taxonomyresults":
			tables.append("result")
			result = all_dataframes[dataframe]
			result['tmp_row'] = result.index


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




                
                ##################
		## LOGIC CHECKS ##
                ##################
		errorLog("Starting Taxonomy Logic Checks")
		statusLog("Starting Taxonomy Logic Checks")
		# each sampleinfo information record must have a corresponding result record. records are matched on stationcode, sampledate, fieldreplicate.
		errorLog("## EACH SAMPLEINFO INFORMATION RECORD MUST HAVE A CORRESPONDING RESULT RECORD. RECORDS ARE MATCHED ON STATIONCODE, SAMPLEDATE, FIELDREPLICATE ##") 
		errorLog(sampleinfo[~sampleinfo[['stationcode','sampledate','fieldreplicate']].isin(result[['stationcode','sampledate','fieldreplicate']].to_dict(orient='list')).all(axis=1)])
		checkLogic(sampleinfo[~sampleinfo[['stationcode','sampledate','fieldreplicate']].isin(result[['stationcode','sampledate','fieldreplicate']].to_dict(orient='list')).all(axis=1)].index.tolist(),'StationCode/SampleDate/FieldReplicate','Logic Error','error','Each Taxonomy SampleInfo record must have a corresponding Taxonomy Result record. Records are matched on StationCode,SampleDate, and FieldReplicate.',sampleinfo)
		errorLog(result[~result[['stationcode','sampledate','fieldreplicate']].isin(sampleinfo[['stationcode','sampledate','fieldreplicate']].to_dict(orient='list')).all(axis=1)])
		checkLogic(result[~result[['stationcode','sampledate','fieldreplicate']].isin(sampleinfo[['stationcode','sampledate','fieldreplicate']].to_dict(orient='list')).all(axis=1)].index.tolist(),'StationCode/SampleDate/FieldReplicate','Logic Error','error','Each Taxonomy Result record must have a corresponding Taxonomy SampleInfo record. Records are matched on StationCode,SampleDate, and FieldReplicate.',result)






                ###################
		## CUSTOM CHECKS ##
                ###################
		message = "Starting Custom Taxonomy Checks"
		errorLog(message)
		statusLog(message)
		## Jordan - Taxonomicqualifier Multi Value Lookup List: check to make sure taxonomicqualifier field data is valid (multiple values may be accepted).
		errorLog(result['taxonomicqualifier'])
		errorLog("Taxonomicqualifier Multi Value Lookup List: check to make sure taxonomicqualifier field data is valid (multiple values may be accepted).")
		nan_rows, invalid_codes, subcodes  = dcValueAgainstMultipleValues(current_app.eng,'lu_taxonomicqualifier','taxonomicqualifiercode',result,'taxonomicqualifier')
		errorLog("Check submitted data for at least one code:")
		checkData(nan_rows,'TaxonomicQualifier','Custom Error','error','At least one TaxonomicQualifier code required please check the list: <a href=http://smcchecker.sccwrp.org/smc/scraper?action=help&layer=lu_taxonomicqualifier target=_blank>lu_taxonomicqualifier</a>.',result)
		errorLog("Check submitted data for invalid code (or code combination):")
		checkData(invalid_codes,'TaxonomicQualifier','Custom Error','error','At least one TaxonomicQualifier code is invalid please check the list: <a href=http://smcchecker.sccwrp.org/smc/scraper?action=help&layer=lu_taxonomicqualifier target=_blank>lu_taxonomicqualifier</a>',result)

		## Jordan -  Sample/Result SampleDate field - make sure user did not accidentally drag down date
                errorLog('Sample/Result SampleDate field - make sure user did not accidentally drag down date')
		# If every date submitted is consecutive from the first, it will error out every row. Otherwise, no error is thrown.
		if sampleinfo.sampledate.diff()[1:].sum() == pd.Timedelta('%s day' %(len(sampleinfo)-1)):
			checkData(sampleinfo.loc[sampleinfo.sampledate.diff() == pd.Timedelta('1 day')].tmp_row.tolist(),'SampleDate','Custom Error','Error','Consecutive Dates. Make sure you did not accidentally drag down the date',sampleinfo)
		if result.sampledate.diff()[1:].sum() == pd.Timedelta('%s day' %(len(result)-1)):
			checkData(result.loc[result.sampledate.diff() == pd.Timedelta('1 day')].tmp_row.tolist(),'SampleDate','Custom Error','Error','Consecutive Dates. Make sure you did not accidentally drag down the date',result)

                ## Jordan - FinalID / LifeStageCode combination must match combination found in vw_organism_lifestage_lookup
                errorLog('FinalID / LifeStageCode combination must match combination found in vw_organism_lifestage_lookup')
                # build list of FinalID/LifeStageCode combinations from lookup lists
                eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
                lu_organisms = "SELECT organismcode, finalid, lifestagecode FROM vw_organism_lifestage_lookup;"
                #lu_organismdetaillookup = "SELECT organismcode, lifestagecode FROM lu_organismdetaillookup;"
                organisms = pd.read_sql_query(lu_organisms,eng)
                #organismdetaillookup = pd.read_sql_query(lu_organismdetaillookup,eng)
                #valid_pairs = organisms.merge(organismdetaillookup, on = ['organismcode'], how = 'inner')
                valid_pairs_list = list(organisms['finalid'] + '_' + organisms['lifestagecode'])

                # compare pairs of submitted FinalID / LifeStageCode to valid_pairings from lookup lists
                errorLog("result where FinalID/LifeStageCode does not match pair from lookup list:")
                errorLog(result[pd.Series(result.finalid + '_' + result.lifestagecode).isin(valid_pairs_list)])

                # perform check on data
                checkData(result[~pd.Series(result.finalid + '_' + result.lifestagecode).isin(valid_pairs_list)].tmp_row.tolist(),'FinalID/LifeStageCode','Undefined Error','error','FinalID/LifeStageCode pair is not valid. Refer to <a href=http://smcchecker.sccwrp.org/smc/scraper?action=help&layer=vw_organism_lifestage_lookup target=_blank>vw_organism_lifestage_lookup</a> for valid pairings',result)
                #####################
                ## START MAP CHECK ##
                #####################
		# get a unique list of stations from results file
		rlist_of_stations = pd.unique(result['stationcode'])
		result_unique_stations = ','.join("'" + s + "'" for s in rlist_of_stations)


                ################
		## NEW FIELDS ##
                ################
		sampleinfo['project_code'] = project_code
		result['project_code'] = project_code
		

                ############################  Note: failure to run csci should not result in a failure
		## BUILD and Process CSCI ##        to submit data - csci status should always = 0
                ############################
                
                # Dont run csci code if there are custom errors - data must be clean
                total_count = errors_dict['total']
                errorLog("total error count: %s" % total_count)
                errorLog("project code: %s" % project_code)
                if total_count == 0:

		        message = "Starting CSCI Processing..."
		        errorLog(message)
		        statusLog(message)
                        msgs = []
                        # combine results and sampleinfo on stationcode we want to get collectionmethod field from sampleinfo
                        bugs = pd.merge(result,sampleinfo[['stationcode','fieldsampleid','fieldreplicate','collectionmethodcode']], on=['stationcode','fieldsampleid','fieldreplicate'], how='left')

                        # original submitted stations
                        list_of_original_unique_stations = pd.unique(bugs['stationcode'])
                        errorLog("list_of_original_unique_stations:")
                        errorLog(list_of_original_unique_stations)
                        unique_original_stations = ','.join("'" + s + "'" for s in list_of_original_unique_stations)

                        # concatenate stationcode, sampledate, collectionmethod, fieldreplicate into one field called sampleid
                        errorLog("create sampleid:")
                        # first get adjusted date
                        bugs["samplerealdate"] = bugs["sampledate"].dt.strftime('%m%d%Y').map(str)
                        bugs["samplemonth"] = bugs["sampledate"].dt.strftime('%m').map(str)
                        bugs["sampleday"] = bugs["sampledate"].dt.strftime('%d').map(str)
                        bugs["sampleyear"] = bugs["sampledate"].dt.strftime('%Y').map(str)
                        # merge two
                        bugs["codeanddate"] = bugs.stationcode.astype(str).str.cat(bugs['samplerealdate'], sep='_')
                        # merge two
                        bugs["collectionandreplicate"] = bugs.collectionmethodcode.astype(str).str.cat(bugs['fieldreplicate'].astype(str), sep='_')
                        # merge both
                        bugs["sampleid"] = bugs.codeanddate.str.cat(bugs.collectionandreplicate, sep='_')
                        # drop temp columns
                        bugs.drop(['samplerealdate','codeanddate','collectionandreplicate'],axis=1,inplace=True)

                        # BUGS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISSTATIONCODEXWALK
                        # BUT STATIONCODE SHOULD ACTUALLY BE GISCODE NOT STATIONCODE
                        # ResultsTable:StationCode links to Crosswalk:StationCode, which links to GISMetrics:GISCode
                        # call gisxwalk table using unique stationcodes and get databasecode and giscode
                        errorLog("building xwalk...")
                        eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
                        sqlwalk = 'select stationcode,databasecode,giscode from lu_newgisstationcodexwalk where stationcode in (%s)' % unique_original_stations
                        gisxwalk = pd.read_sql_query(sqlwalk,eng)

                        #bugs = pd.merge(bugs,gisxwalk[['stationcode','giscode','databasecode']], on = ['stationcode'], how='inner')
                        bugs = pd.merge(bugs,gisxwalk[['stationcode','giscode','databasecode']], on = ['stationcode'], how='inner')

                        # only process stations that have associated gismetric data
                        missing_bugs_xwalk = set(list_of_original_unique_stations)-set(bugs.stationcode.tolist())

                        # send email if stations missing GIS Metric data.
                        if missing_bugs_xwalk:
                            bad_stations = '\n'.join(str(x) for x in missing_bugs_xwalk)
                            msgs.append('CSCI Error:\n')
                            msgs.append('The following stations are missing GISXWalk data:\n')
                            msgs.append(bad_stations)
                            print msgs

                        # original stations translated to smc stations using giscode
                        list_of_unique_stations = pd.unique(bugs['giscode'])
                        errorLog("list_of_unique_stations:")
                        errorLog(list_of_unique_stations)
                        unique_stations = ','.join("'" + s + "'" for s in list_of_unique_stations)

                        #### STATIONS IS BUILT OFF THE MERGENCE OF BUG FILE AND GISMETRICS
                        errorLog("building gismetrics...")
                        sqlmetrics = 'select * from tbl_newgismetrics'
                        gismetrics = pd.read_sql_query(sqlmetrics,eng)
                        # merge gismetrics and gisxwalk to get giscode into dataframe
                        # merge bugs/stationcode and gismetrics/giscode
                        # check stations
                        test_stations = pd.unique(bugs['stationcode'])
                        # problem - gismetrics stationcode is replacing bugs-originalsubmission stationcode thats a problem
                        errorLog(test_stations)
                        # copy bugs.stationcode to retain in stations below
                        bugs['original_stationcode'] = bugs['stationcode']
                        stations = pd.merge(gismetrics,bugs[['giscode','original_stationcode']], left_on = ['stationcode'], right_on = ['giscode'], how='inner')
                        # drop gismetrics stationcode
                        stations.drop(['stationcode'],axis=1,inplace=True)
                        stations.rename(columns={'original_stationcode': 'stationcode'}, inplace=True)
                        eng.dispose()
                        # check stations
                        test2_stations = pd.unique(stations['stationcode'])
                        errorLog(test2_stations)

                        # only process stations that have associated gismetric data
                        missing_bugs_stations = set(list_of_unique_stations)-set(bugs.giscode.tolist())
                        missing_stations_stations = set(list_of_unique_stations)-set(stations.giscode.tolist())

                        # send email if stations missing GIS Metric data.
                        if missing_bugs_stations|missing_stations_stations:
                            bad_stations = '\n'.join(str(x) for x in missing_bugs_stations.union(missing_stations_stations))
                            msgs.append('CSCI Error:\n')
                            msgs.append('The following stations are missing GISMetric data:\n')
                            msgs.append(bad_stations)
                            print msgs

                        # drop unnecessary columns
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

                        # Import and Execute cleanData and CSCI functions 
                        import rpy2
                        import rpy2.robjects as robjects
                        from rpy2.robjects import pandas2ri
                        import rpy2.robjects.packages as rpackages
                        from rpy2.robjects.packages import importr
                        import rpy2.rinterface as rinterface

                        # shortens notation for accessing robjects
                        r = robjects.r

                        # imports R package: CSCI
                        CSCI = importr('CSCI')

                        # convert cleanData() and CSCI() functions from CSCI package to python
                        cd = CSCI.cleanData
                        csci = CSCI.CSCI

                        # collect errors and error counts for each group
                        error_count = {'clean data':0, 'CSCI': 0}
                        cd_group_errors = []
                        csci_group_errors = []

                        # process cleanData and CSCI for each Sample
                        bugs_grouped = bugs.groupby(['SampleID'])

                        # open log file for printing status
                        TIMESTAMP = str(int(round(time.time()*1000)))
                        logfile = '/var/www/smc/testfiles/' + TIMESTAMP + '.log'

                        # Activate R to Python DataFrame conversions
                        pandas2ri.activate()

                        start_time = int(time.time())
                        count = 0
                        for name, group in bugs_grouped:
                            # print current group
                            print "group name: %s" %(name)
                            bug_sample_id = name
                            
                            # group stationcode to get just one
                            single_station = group.StationCode.unique()
                            
                            # check to makesure there is only one
                            print "stations_grouped: %s" % single_station[0]

                            # find stationcode that matches between the bugs record and what is in stations
                            station = stations.loc[stations['stationcode'] == single_station[0]]
                            
                            # convert group, station to R dataframe
                            errorLog("convert group, station to R dataframe")
                            group = pandas2ri.py2ri(group)
                            station = pandas2ri.py2ri(station)

                            # copy of group
                            errorLog("make a copy of group and adjust sampledate fields")
                            group_copy = group
                            #errorLog("group_copy:")
                            #errorLog(group_copy)
                            group_copy = pandas2ri.ri2py(group_copy)
                            #errorLog("list group copy:")
                            #errorLog(list(group_copy))
                            group_copy.columns = [x.lower() for x in group_copy.columns]
                            # get samplemonth, sampleday, sampleyear for later use
                            #group_copy["sampledate"] = pd.datetime.strptime(group_copy['sampledate'], '%Y-%m-%d')
                            #group_copy["samplemonth"] = group_copy.sampledate.dt.month
                            #group_copy["sampleday"] = group_copy.sampledate.dt.day
                            #group_copy["sampleyear"] = group_copy.sampledate.dt.year
                            
                            '''
                            # clean group with cleanData()
                            cd_list = cd(group,msgs=True)
                            group = cd_list[0]
                            warn_msg = cd_list[1]

                            # if data cannot be cleaned, prepare email message
                            if warn_msg[0] != 'Data already clean':
                                errorLog('cleanData Failed:\n')
                                bad_station = 'cleanData failed on station %s:\n' %single_station[0]
                                bad_group = 'Sample %s could not be cleaned because %s.' %(bug_sample_id,warn_msg[0])
                                errorLog(bad_station)
                                errorLog(bad_group)
                                msgs.append('CSCI Error:\n')
                                msgs.append(bad_station)
                                msgs.append(bad_group)
                                
                            else:
                            '''
                            try:
                                    errorLog("data is clean process csci")
                                    errorLog(station)
                                    errorLog(group)
                                    report = csci(group,station)

                                    # assign csci elements to proper tables
                                    errorLog("assign elements to specific tables")
                                    core = pandas2ri.ri2py(report[0])
                                    s1mmi = pandas2ri.ri2py(report[1])
                                    s1grps = pandas2ri.ri2py(report[2])
                                    s1oe = pandas2ri.ri2py(report[3])
                                    s2oe = pandas2ri.ri2py(report[4])
                                    s2mmi = pandas2ri.ri2py(report[5])

                                    # fields that need to be filled
                                    errorLog("first - csci")
                                    errorLog(core)
                                    core.columns = [x.lower() for x in core.columns]
                                    core['processed_by'] = "checker"
                                    core['cleaned'] = "Yes"
                                    core['scorenotes'] = "Distinct set to NA"
                                    core['rand'] = 2
                                    core['scoredate'] = timestamp_date
                                    core['record_origin'] = project # should probably be SMC
                                    core['origin_lastupdatedate'] = timestamp_date
                                    core['record_publish'] = "False"
                                    core = pd.merge(core,group_copy[['sampleid','sampledate','sampleday','samplemonth','sampleyear','collectionmethodcode','fieldreplicate']], on = ['sampleid'], how='left')
                                    core['sampledate'] = pd.to_datetime(core['sampledate'], unit = 's').dt.date
                                    core = core.drop_duplicates()
                                    core_file = "/var/www/smc/logs/%s.core.csv" % TIMESTAMP
                                    # only show header once
                                    
                                    if count == 0:
                                        core.to_csv(core_file, sep=',', mode='a', encoding='utf-8', index=False)
                                    else:
                                        # skip next loop
                                        core.to_csv(core_file, sep=',', mode='a', encoding='utf-8', index=False, header=False)
                                   
                                    errorLog("second - s1mmi")
                                    s1mmi.columns = [x.lower() for x in s1mmi.columns]
                                    s1mmi['processed_by'] = "checker"
                                    s1mmi.rename(columns={'coleoptera_percenttaxa_predicted': 'coleoptera_percenttaxa_predict'}, inplace=True)
                                    s1mmi['record_origin'] = project # should probably be SMC
                                    s1mmi['origin_lastupdatedate'] = timestamp_date
                                    s1mmi['record_publish'] = "False"
                                    s1mmi = s1mmi.drop_duplicates()
                                    s1mmi_file = "/var/www/smc/logs/%s.Suppl1_mmi.csv" % TIMESTAMP
                                    # only show header once
                                    if count == 0:
                                        s1mmi.to_csv(s1mmi_file, sep=',', mode='a', encoding='utf-8', index=False)
                                    else:
                                        # skip next loop
                                        s1mmi.to_csv(s1mmi_file, sep=',', mode='a', encoding='utf-8', index=False, header=False)

                                    errorLog("third - s2mmi")
                                    s2mmi.columns = [x.lower() for x in s2mmi.columns]
                                    s2mmi['processed_by'] = "checker"
                                    s2mmi['record_origin'] = project # should probably be SMC
                                    s2mmi['origin_lastupdatedate'] = timestamp_date
                                    s2mmi['record_publish'] = "False"
                                    s2mmi = s2mmi.drop_duplicates()
                                    s2mmi_file = "/var/www/smc/logs/%s.Suppl2_mmi.csv" % TIMESTAMP
                                    # only show header once
                                    if count == 0:
                                        s2mmi.to_csv(s2mmi_file, sep=',', mode='a', encoding='utf-8', index=False)
                                    else:
                                        # skip next loop
                                        s2mmi.to_csv(s2mmi_file, sep=',', mode='a', encoding='utf-8', index=False, header=False)

                                    errorLog("fourth - s1grps")
                                    s1grps.columns = [x.lower() for x in s1grps.columns]
                                    s1grps['processed_by'] = "checker"
                                    s1grps['record_origin'] = project # should probably be SMC
                                    s1grps['origin_lastupdatedate'] = timestamp_date
                                    s1grps['record_publish'] = "False"
                                    s1grps = s1grps.drop_duplicates()
                                    s1grps_file = "/var/www/smc/logs/%s.Suppl1_grps.csv" % TIMESTAMP
                                    # only show header once
                                    if count == 0:
                                        s1grps.to_csv(s1grps_file, sep=',', mode='a', encoding='utf-8', index=False)
                                    else:
                                        # skip next loop
                                        s1grps.to_csv(s1grps_file, sep=',', mode='a', encoding='utf-8', index=False, header=False)

                                    errorLog("fifth - s1oe")
                                    s1oe.columns = [x.lower() for x in s1oe.columns]
                                    #print s1oe
                                    #s1oe['objectid'] = s1oe.apply(lambda x: int(x.objectid) + x.index, axis=1)
                                    s1oe['processed_by'] = "checker"
                                    s1oe['record_origin'] = project # should probably be SMC
                                    s1oe['origin_lastupdatedate'] = timestamp_date
                                    s1oe['record_publish'] = "False"
                                    s1oe = s1oe.drop_duplicates()
                                    s1oe_file = "/var/www/smc/logs/%s.Suppl1_OE.csv" % TIMESTAMP
                                    # only show header once
                                    if count == 0:
                                        s1oe.to_csv(s1oe_file, sep=',', mode='a', encoding='utf-8', index=False)
                                    else:
                                        # skip next loop
                                        s1oe.to_csv(s1oe_file, sep=',', mode='a', encoding='utf-8', index=False, header=False)
            
                                    errorLog("sixth - s2oe")
                                    s2oe.columns = [x.lower() for x in s2oe.columns]
                                    # fill na with -88
                                    #s2oe.fillna(-88, inplace=True)
                                    s2oe['captureprob'].replace(['NA'], -88, inplace=True)
                                    s2oe['processed_by'] = "checker"
                                    s2oe['record_origin'] = project # should probably be SMC
                                    s2oe['origin_lastupdatedate'] = timestamp_date
                                    s2oe['record_publish'] = "False"
                                    s2oe = s2oe.drop_duplicates()
                                    s2oe_file = "/var/www/smc/logs/%s.Suppl2_OE.csv" % TIMESTAMP
                                    # only show header once
                                    if count == 0:
                                        s2oe.to_csv(s2oe_file, sep=',', mode='a', encoding='utf-8', index=False)
                                    else:
                                        # skip next loop
                                        s2oe.to_csv(s2oe_file, sep=',', mode='a', encoding='utf-8', index=False, header=False)

                                    summary_results_link = TIMESTAMP

                                    count = count + 1
                                    #file_to_get = "/var/www/smc/logs/%s.core.csv" % TIMESTAMP
                                    #errorLog("file to get:")
                                    #errorLog(file_to_get)
                                    #all_dataframes["2 - core_csv - tmp_cscicore"] = pd.read_csv('/var/www/smc/logs/%s.core.csv' % TIMESTAMP)
                                    #all_dataframes["2 - core_csv - tmp_cscicore"].columns = [x.lower() for x in all_dataframes["2 - core_csv - tmp_cscicore"].columns]

                                    ## WHAT HAPPENS IF CSCI SCORE IS ALREADY IN DATABASE - MAY WANT TO CHECK ABOVE
                                    #errorLog("print core_csv columns:")
                                    #errorLog(list(all_dataframes["2 - core_csv - tmp_cscicore"]))
                                    #errorLog("remove index:")
                                    #all_dataframes["2 - core_csv - tmp_cscicore"].drop(['unnamed: 0'],axis=1, inplace=True)
                                    #errorLog(list(all_dataframes["2 - core_csv - tmp_cscicore"]))
                                    #errorLog(all_dataframes["2 - core_csv - tmp_cscicore"])
                                    #
                                    #summary_results_link = 'http://smcchecker.sccwrp.org/smc/logs/%s.core.csv' % TIMESTAMP
                                    #summary_results_link = TIMESTAMP

                                    ### IMPORTANT LOAD ONE CSCI FIELD FROM CSV FILE AND MAP IT TO EXISTING BUGS/STATIONS DATAFRAME THEN OUTPUT TO CSV LOAD FILE FOR IMPORT
                                    ### AT STAGING INTO DATABASES
                                    message = "Success CSCI"
                                    errorLog(message)
                                    '''
                                    # code below wont work do to sampledate getting changed to number instead of date - fails on submission
                                    all_dataframes["2 - CSCI_Core - csci_core"] = core
                                    all_dataframes["3 - CSCI_Suppl1_MMI - csci_suppl1_mmi"] = s1mmi
                                    all_dataframes["4 - CSCI_Suppl2_MMI - csci_suppl2_mmi"] = s2mmi
                                    all_dataframes["5 - CSCI_Suppl1_GRPS - csci_suppl1_grps"] = s1grps
                                    all_dataframes["6 - CSCI_Suppl1_OE - csci_suppl1_oe"] = s1oe
                                    all_dataframes["7 - CSCI_Suppl2_OE - csci_suppl2_oe"] = s2oe
                                    '''
                                    
                                    message = str(msgs)
                                    state = 0
                            except Exception as e:
                                    # here is where we email sccwrp to let them know we couldnt get csci score for sampleid - we still need load the data and try to load other sampleids 
                                    bad_station = '\n CSCI Processing Failed on station %s:\n' %single_station[0]
                                    bad_group = 'Sample %s could not be processed because %s.\n' % (bug_sample_id,e[0])
                                    
                                    msgs.append('CSCI Error:\n')
                                    msgs.append(bad_station)
                                    msgs.append(bad_group)
                                    
                                    errorLog("CSCI ran into the following error: %s" % e[0])
                                    msgs.append('Failed to run csci\n')
                                        
                                        
                                                                
                        all_dataframes["2 - CSCI_Core - csci_core"] = pd.read_csv("/var/www/smc/logs/%s.core.csv" %TIMESTAMP)
                        all_dataframes["3 - CSCI_Suppl1_MMI - csci_suppl1_mmi"] = pd.read_csv("/var/www/smc/logs/%s.Suppl1_mmi.csv" %TIMESTAMP)
                        all_dataframes["4 - CSCI_Suppl2_MMI - csci_suppl2_mmi"] = pd.read_csv("/var/www/smc/logs/%s.Suppl2_mmi.csv" %TIMESTAMP)
                        all_dataframes["5 - CSCI_Suppl1_GRPS - csci_suppl1_grps"] = pd.read_csv("/var/www/smc/logs/%s.Suppl1_grps.csv" %TIMESTAMP)
                        all_dataframes["6 - CSCI_Suppl1_OE - csci_suppl1_oe"] = pd.read_csv("/var/www/smc/logs/%s.Suppl1_OE.csv" %TIMESTAMP)
                        all_dataframes["7 - CSCI_Suppl2_OE - csci_suppl2_oe"] = pd.read_csv("/var/www/smc/logs/%s.Suppl2_OE.csv" %TIMESTAMP)
                        message = msgs
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

