from flask import Blueprint, request, jsonify
import os, collections
import pandas as pd
import re
from .ApplicationLog import *

def getTableAndColumns(db,dbtype,eng):
	errorLog("start getTableAndColumns")
	statusLog("start getTableAndColumns")
	system_fields = current_app.system_fields # hidden database fields like id
	sqlFields = {}
	if dbtype == "mysql" or dbtype == "mysql-rest":
		query = eng.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s' and TABLE_NAME LIKE '%s'" % (db,"tbl%%"))
	elif dbtype == "postgresql":
		# added BASE TABLE filter - exclude views
		query = eng.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_CATALOG='%s' AND TABLE_NAME LIKE '%s'" % (db,"tbl_%%"))
	elif dbtype == "azure":
		query = eng.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG ='%s' and TABLE_NAME LIKE '%s'" % (db,"tblToxicity%%"))
	elif dbtype == "msql":
		query = eng.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG ='Bight2008RegionalMonitoring' and TABLE_NAME LIKE ' tblToxicity%%'")
	errorLog(query)
	#### if there are no tables returned then how can we proceed
	errorLog(query.rowcount)
	for x in query:
		errorLog(x)
		if dbtype == "mysql" or dbtype == "azure":
			name_of_table = x.TABLE_NAME # mysql and microsoftsql
			sql = "select column_name from information_schema.columns where table_name = '%s'" % name_of_table # to be used with all databases
		elif dbtype == "postgresql":
			name_of_table = x.table_name # postgresql
			sql = "select column_name from information_schema.columns where table_name = '%s'" % name_of_table # to be used with all databases
		elif dbtype == "mysql-rest":
			name_of_table = x.TABLE_NAME # mysql
			sql = "SELECT COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s'" % name_of_table # to be used with survey only
		errorLog(sql)
		subquery = eng.execute(sql)
		for y in subquery:
			# setdefault - If key is in the dictionary, return its value. If not, insert key with a value of default and return default. default defaults to None.
			if dbtype == "mysql-rest":
				sqlFields.setdefault(name_of_table,[]).append(y.COLUMN_COMMENT) #  user for survey
			else:
				if y.column_name not in system_fields:
					sqlFields.setdefault(name_of_table,[]).append(y.column_name) #  used for all databases
		subquery.close()
	query.close()
	# close engine connections
	eng.dispose()
	errorLog("end getTableAndColumns")
	return sqlFields

def matchColumnsToTable(tab_name,tab,sqlFields,tabCounter,errors_dict):
	# purpose: does the data match a table, if not we should let user know which table it matches closest to
	# result: true, false/matched table or closest matching table/columns matched
	# tab_name = name of tab or sheet being worked on
	# tab = rows of data in the individual tab or sheet
	# sqlFields = dictionary of tables and columns in database
	# tabCounter = numeric identity of tab or sheet being worked on
	errorLog("start matchColumnsToTable")
	statusLog("start matchColumnsToTable")
	matchset = False # if we can match the tab or sheet to a table then this gets returned at bottom
	match = [] # matched columns to return
	nomatch = [] # unmatched columns to return
	tabColumns = [] # list of lowercase column names for matching
	counter_key = ""
	tabCount = len(tab.columns) # how many columns are in the tab or sheet
	tableCountList = [] # total match for each table to tab comparison - ie. tblresult = 8, tblbatch = 3, etc..
	for table in sqlFields:
		errorLog("-----Loop through table: %s" % table)
		tableCount = 0
		# loop through each column in a table
		# list of columns that match
		collect_columns = []
		columnCount = len(sqlFields[table]) # how many columns are in the table
		for column in sqlFields[table]:
			#errorLog("-------------Loop through columns in table: %s" % column)
			# check each column in the excel against a database column
			for field in tab.columns:
				#lowercase column name
				lcolumn = column.lower()
				# TURN ON FOR DEBUGGIN
				#errorLog("field: %s and lcolumn: %s" % (field,lcolumn))
				# re.match is not an exact match it needs a $ at end of searched element to make it so
				find_field = field.lower() + "$"
				m = re.match(find_field, lcolumn)
				if m:
					# increment count for table
					collect_columns.append(str(m.group(0)))
					tableCount += 1
					# TURN ON FOR DEBUGGIN
					#errorLog("-------------------------Matched: %s -- TableCount: %s" % (m.group(0), tableCount))
		errorLog("##### We were able to match sheet - %s to table - %s the following times: %s #######" % (tab_name,table,tableCount))
		#errorLog("##### columnCount: %s" % columnCount)
		#counter[counter_key] = collect_columns
		tableCountList.append(tableCount)
		#errorLog("##### tableCount: %s and columnCount: %s" % (tableCount,columnCount))
		#if tableCount >= tabCount:
		# the total number of columns in a table must match the total number of matched columns in a tab or sheet
		#if tableCount == tabCount: - this is wrong it should be tableCount == columnCount
		#errorLog("tableCount/tabCount: %s/%s" % (tableCount,tabCount))
		errorLog("tableCount/columnCount: %s/%s" % (tableCount,columnCount))
		# new code added 16jan18
		if tableCount == columnCount:
			# TURN ON FOR DEBUGGING
			errorLog("\n\nFOUND tab count/column count: %s/%s" % (tabCount,columnCount))
			errorLog("-----+Sheet %s is matched to table %s with count %s" % (tab_name,table,str(columnCount)))
			#counter_key = str(tabCounter) + "-" + tab_name + "-" + str(tabCount) + "-" + table + "-" + "True" + "-" + str(tableCount) + "-" + str(collect_columns) 
			counter_key = str(tabCounter) + "-" + tab_name + "-" + str(tabCount) + "-" + table + "-" + "True" + "-" + str(tableCount) + "-" + str(','.join(collect_columns)) 
			matchset = True
			match.append(counter_key)
			# what do we do if we have multiple tables in a database that are duplicates
			# try to match table name to sheet name - or at least give that priority
			# we can skip checking for other tables but we need to remove any others found
			if tab_name == table and len(match) > 1:
				item_to_keep = len(match) - 1
				count = 0
				while item_to_keep != count:
					match.pop(count)
					count = count + 1
				errorLog("EQUAL: %s" % match)
				errorLog("EQUALCOUNT: %s" % len(match))
				return True, match
			errorLog("tab is set to: %s and value is: %s" % (matchset, match))
			errorLog("END FOUND\n\n")
		else:
			# find columns that failed to match - also get columns that do not match
			# successfully matched columns = collect_columns
			# failed match columns = collect__failed_columns
			errorLog("find columns that failed to match - also get columns that do not match")
			#errorLog("tab.columns: %s" % tab.columns)
			#errorLog("collect_columns: %s" % collect_columns)
			# carrot operator acts as an xor below
			collect_failed_columns = set(tab.columns)^set(collect_columns)
			collect_failed_table_columns = set(sqlFields[table])^set(collect_columns)
			errorLog("collect_failed_columns: %s" % collect_failed_columns)
			errorLog("collect_failed_table_columns: %s" % collect_failed_table_columns)
			# 
			#errorLog(sqlFields[table]
			counter_key = str(tabCounter) + "-" + tab_name + "-" + str(tabCount) + "-" + table + "-" + "False" + "-" + str(tableCount) + "-" + str(','.join(collect_columns)) + "-" + str(','.join(collect_failed_columns)) + "-" + str(','.join(collect_failed_table_columns))
			nomatch.append(counter_key)
			#errorLog("tab is set to: %s and value is: %s" % (matchset, nomatch))
	if matchset == True:
		return True, match
	else:
		# we need to find the closest match in the nomatch list - the largest tabCount
		# max gives you the largest element in list - index on the outer gives you the element index
		if max(tableCountList):
			closest_match = tableCountList.index(max(tableCountList))
		# in case there are no matches
		else:
			#closest_match = tableCountList[0]
			closest_match = 0
			nomatch[0] = "%s-%s-%s-%s-%s-%s" % (str(tabCounter),tab_name,str(tabCount),"None","False","No match for tab")
		#errorLog(closest_match)
		#errorLog(nomatch[0])
		errorsCount(errors_dict,"match")
		return False, nomatch[closest_match]
	errorLog("end matchColumnsToTable")


match_file = Blueprint('match_file', __name__)

@match_file.route("/match", methods=["POST"])
def match(infile,errors_dict):
	errorLog("Function - match")
	statusLog("Attempting to Match Tab to Table")
	errorLog("DEBUG")
	#inFile = current_app.infile
	inFile = infile
	#errorLog(inFile)
	message = ""
	match_tables = []
	#sql_match_tables = current_app.sql_match_tables
	match_sheets_to_tables = {}
	# dictionaries by default are unorder we are using ordered dictionary to store dataframes
	# create new dictionary and assign it to global all_dataframes variable
	all_dataframes = collections.OrderedDict()
	sql_match_tables = []
	#current_app.all_dataframes = all_dataframes	
	#current_app.sql_match_tables = sql_match_tables

	try:
		errorLog("run basic checks on file before proceeding")
		if os.stat(inFile).st_size == 0:
			message = "Critical error: file is empty"
			errorLog(message)
			errorsCount(errors_dict,"match")
			state = 1
		else: 
			# place excel file into a dataframe
			#errorLog("chunksize=1000")
			# may need to use ssconvert to convert excel file to csv first (speed things up)
			df = pd.ExcelFile(inFile, keep_default_na=False, na_values=['NaN'])
			#df= pd.read_excel(inFile, sheetname = None)
			# dictionary of tables and columns in database
			# get tables and columns for matching by dcMatchColumnsToTable
			try:
				sqlFields = getTableAndColumns(current_app.db,current_app.dbtype,current_app.eng)
				errorLog('sqlFields: %s' % sqlFields)
			except ValueError:
				sqlFields = 1
				message = "Critical error: Failed to get database tables and columns."
				state = 1
			# we cant match tabs to tables unless we have sql fields
			if sqlFields != 1:
                     		# we have a dataframe that may have multiple tabs
                       		df_tab_names = df.sheet_names
				message = "Excel sheet names = %s" % df_tab_names
                       		errorLog(message)
				# count = 0 -> not sure why this is necessary - tabCounter may be necessary only
				# tabCounter keep track of working tab inside of matchColumnsToTable
				tabCounter = 0
				for tab in df_tab_names:
					# name only
					tab_name = tab
					# drop any tab_name = lookups which is a reserved word added 15jan18
					if tab_name == 'lookups':
						errorLog('The application is skipping sheet "%s" because it is named "lookups" which is reserved' % tab_name)
						continue
					# assign actual data in tab to tab
					#tab = df.parse(tab)
					# added code to fix issue: Pandas interprets cell values with NA as NaN bug #3
					# changed back on 22feb18 due to multiple blank rows causing application to fail
					# decision was made that the checker should not accept NA or NAN (reserved words)
					#tab = pd.read_excel(inFile, keep_default_na=False, na_values=['NaN'], sheetname = tab)
					tab = pd.read_excel(inFile, sheetname = tab)
					if tab.empty:
						message = "tab %s has no matching table" % tab_name
						errorLog(message)
						errorsCount(errors_dict,"match")
						# if the sheet is blank skip to the next sheet
						continue
		                        # clean up sheet before doing anything else
                        		# drop all empty columns - not working properly - picks up comments as empty
                        		#tab.dropna(axis=1, how='all', inplace=True)
                        		# drop all empty rows
                        		#tab.dropna(axis=0, how='all', inplace=True)
					# drop all columns that should never be in file
					if 'errors' in tab.columns:
						tab.drop(tab[['errors']], axis=1, inplace=True)
					if 'custom_errors' in tab.columns:
						tab.drop(tab[['custom_errors']], axis=1, inplace=True)
					if 'custom_warnings' in tab.columns:
						tab.drop(tab[['custom_warnings']], axis=1, inplace=True)
					if 'field_error' in tab.columns:
						tab.drop(tab[['field_error']], axis=1, inplace=True)
					if 'lookup_error' in tab.columns:
						tab.drop(tab[['lookup_error']], axis=1, inplace=True)
					if 'duplicate_production_submission' in tab.columns:
						tab.drop(tab[['duplicate_production_submission']], axis=1, inplace=True)
					if 'duplicate_session_submission' in tab.columns:
						tab.drop(tab[['duplicate_session_submission']], axis=1, inplace=True)
					# lower case column names
					tab.columns = [x.lower() for x in tab.columns]
					errorLog('Match: tab_name: %s' % tab_name)
					#errorLog('Match: tab: %s' % tab)
					statusLog('Attempting to match sheet %s' % tab_name)
					match_result, match_fields = matchColumnsToTable(tab_name,tab,sqlFields,tabCounter,errors_dict)
					errorLog('Match: result: %s' % match_result)
					if match_result == True:
						#errorLog('Match: fields: %s' % match_fields)
						match_tables.append(match_fields)
						# get split key to get matching table
						split_match_fields = match_fields[0].split('-')
						# field three is matched table
						table_match = split_match_fields[3]
						errorLog('Matched sheet %s to %s' % (tab_name,table_match))
						statusLog('Matched sheet %s to %s' % (tab_name,table_match))
						sql_match_tables.append(str(table_match))
						match_sheets_to_tables[tabCounter] = str(str(tabCounter) + " - " + df.sheet_names[tabCounter] + " - " + table_match)
						#match_sheets_to_tables[count] = str(str(count) + " - " + df.sheet_names[count] + " - " + table_match)
						tab_and_table = str(str(tabCounter) + " - " + df.sheet_names[tabCounter] + " - " + table_match)
						#tab_and_table = str(str(count) + " - " + df.sheet_names[count] + " - " + table_match)
						all_dataframes[tab_and_table] = tab
						errorLog(message)
						#count = count + 1
					else:
						message = "tab %s has no matching table" % tab_name
						errorLog(message)
						match_tables.append(match_fields)
						#  if no tab match we must increment error count
						#errorsCount(errors_dict,"match")
					tabCounter = tabCounter + 1
					message = "match_tables: %s" % match_tables
					#except:
					#	message = "Critical error: We attempted to lowercase the column names for tab: %s and failed." % tab_name
					#	errorLog(message)
					#	#[u'1-results-18-tbltoxicityresults-True-18-stationid,toxbatch,matrix,labcode,species,dilution,treatment,concentration,concentrationunits,endpoint,labrep,result,resultunits,qacode,sampletypecode,fieldreplicate,samplecollectdate,comments']]"
					#	if match_tables:
					#		message_string = "%s-%s-0-none-False-0-failed_to_lowercase_columns" % (tabCounter,tab_name)
					#		test = []
					#		test.append(message_string)
					#		match_tables.append(test)
					#	state = 1
				#errorLog("all_dataframes count: %s" % len(all_dataframes))
				# we were unable to match any of the excel tabs to database tables
				#if len(all_dataframes) == 0:	 -> not necessary tagged inside of match columns to table
				#	message = "Critical Error: Failed to match any excel tabs to database tables."
				#	state = 1
				#	errorsCount(errors_dict,"match")
				#else:
				#	state = 0
		errorLog(match_tables)
		errorLog(message)
		return all_dataframes, sql_match_tables, match_tables
	except ValueError:
		message = "Critical Error: Failed to run matching checks."	
		errorLog(message)
		state = 1
	#return jsonify(message=message,state=state,table_match=match_tables)
