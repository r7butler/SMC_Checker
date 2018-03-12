from flask import Blueprint, request, jsonify, session
import json
import pandas as pd
import numpy as np
import cerberus
from sqlalchemy import create_engine, exc
import re
from cerberus import Validator, SchemaError, errors
#from time import sleep
from .ApplicationLog import *

def addErrorToList(error_column, row, error_to_add,df):
	df.ix[int(row), 'row'] = str(row)
	if error_column in df.columns:
		# check if cell value is empty (nan) 
		if(pd.isnull(df.ix[int(row), error_column])):
			# no data exists in cell so add error
        		df.ix[int(row), error_column] = error_to_add
			errorLog("Core: Row: %s, Error To Add: %s" % (int(row),error_to_add))
		else:
			# a previous error was recorded so append to it
			# even though there may be data lets check to make sure it is not empty
			if str(df.ix[int(row), error_column]):
				#print("There is already a previous error recorded: %s" % str(df.ix[int(row), error_column]))
				df.ix[int(row), error_column] = str(df.ix[int(row), error_column]) + "," + error_to_add
				errorLog("Core: Row: %s, Error To Add: %s" % (int(row),error_to_add))
			else:
				#print("No error is recorded: %s" % str(df.ix[int(row), error_column]))
        			df.ix[int(row), error_column] = error_to_add
				errorLog("Core: Row: %s, Error To Add: %s" % (int(row),error_to_add))
				
	else:
        	df.ix[int(row), error_column] = error_to_add
		errorLog("Core: Row: %s, Error To Add: %s" % (int(row),error_to_add))
	return df

def checkDuplicatesInProduction(db,dbtype,eng,table_match,errors_dict,df):
	errorLog("Core: start checkDuplicatesInProduction")
	statusLog("Check Duplicates in Production for table: %s" % table_match)
	if dbtype == "mysql" or dbtype == "mysql-rest":
		sql_primary_key = "select column_name from information_schema.key_column_usage where table_name =  '%s' and constraint_name = 'PRIMARY'" % (table_match) # mysql
	if dbtype == "postgresql" or dbtype == "azure":
		sql_primary_key = "SELECT DISTINCT(kcu.column_name) FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'PRIMARY KEY' AND tc.table_name='%s'" % (table_match) # postgresql and azure
	errorLog("dcDuplicatesInProduction sql statement get primary keys: %s" % sql_primary_key)
	sql_primary_keys= eng.execute(sql_primary_key)

	primary_keys = sql_primary_keys.cursor.fetchall()
	sql_primary_keys.close()

	primary_key_list = []
	for p in primary_keys:
		if p[0].lower() in ["id", "record_timestamp", "timestamp","errors","lastchangedate","toxbatchrecordid","submissionid","toxicityresultsrecordid","picture_url", "coordinates", "device_type", "qcount", "status"]:
			errorLog("Not adding: %s to primary_key_list" % p[0])
		else:
			if dbtype == "mysql" or dbtype == "mysql-rest":
				primary_key_list.append(str(p[0])) # mysql
			elif dbtype == "postgresql" or dbtype == "azure":
				primary_key_list.append(p[0]) # postgresql and azure
				#primary_key_list.append(p[0].lower()) # postgresql and azure
	errorLog("Core: Primary key list with which to check for duplicates: %s" % primary_key_list)

	# if primary_key_list is empty then no reason to check for duplicates
	if not primary_key_list:
		errorLog("Core: Primary key list is empty!")
		return df

	if dbtype == "mysql" or dbtype == "mysql-rest":
		field_list_string = ','.join(map(str, primary_key_list))  # mysql
		sql2 = "select %s from %s" % (field_list_string, table_match)
	elif dbtype == "postgresql" or dbtype == "azure":
		# change table to view for versioned geodatabases
		#table_match_view = table_match + "_evw" - used in new site 
		field_list_string = ','.join([str('"'+x+'"') for x in primary_key_list])  # postgresql and azure
		#sql2 = 'select %s from "%s"' % (field_list_string, table_match_view) - used in new site
		sql2 = 'select %s from "%s"' % (field_list_string, table_match)
	errorLog("Core: second sql statement return existing primary key values: %s" % sql2)
	df_sql= pd.read_sql_query(sql2 ,eng)

	# lowercase all column names in preparation for match
	df.columns = [x.lower() for x in df.columns]
	df_sql.columns = [x.lower() for x in df_sql.columns]
	primary_key_list = [x.lower() for x in primary_key_list]
	# merge dataset together
	common = df_sql.merge(df,on=primary_key_list)

	### THIS CODE WORKS BUT ISN'T IDEAL/NO NEED TO LOOP THROUGH PRIMARY KEYS ONLY LOOP THROUGH EACH RECORD IN common AND MATCH TO
	### THE ROW IN df 
	#item_index = None
	errorLog("common item found")
	errorLog(common)
	for index, row in common.iterrows():
		#errorLog("index: %s row: %s" % (index,row))
		human_error = 'You submitted a record that is already in the database'
		unique_error = '{ "column": "", "error_type": "Duplicate Production Submission", "error": "%s" }' % (human_error)
		addErrorToList("errors",index,unique_error,df)
		addErrorToList("duplicate_production_submission",index,unique_error,df)
		errorsCount(errors_dict,"duplicate")
	errorLog("Core: end checkDuplicatesInProduction")
	return df

def checkDuplicatesInSession(db,dbtype,eng,table_match,errors_dict,df):
	errorLog("Core: start checkDuplicatesInSession")
	statusLog("Check Duplicates in Session for table: %s" % table_match)
	if dbtype == "mysql" or dbtype == "mysql-rest":
		sql_primary_key = "select column_name from information_schema.key_column_usage where table_name =  '%s' and constraint_name = 'PRIMARY'" % (table_match) # mysql
	elif dbtype == "postgresql" or dbtype == "azure":
		sql_primary_key = "SELECT DISTINCT(kcu.column_name) FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'PRIMARY KEY' AND tc.table_name='%s'" % (table_match) # postgresql and azure
	errorLog("Core: Checking for duplicates in session sql statement: %s" % sql_primary_key)
	sql_primary_keys= eng.execute(sql_primary_key)

	primary_keys = sql_primary_keys.cursor.fetchall()
	sql_primary_keys.close()
	
	errorLog(primary_keys)

	if(primary_keys):
		primary_key_list = []
		for p in primary_keys:
			# remove primary key fields in sql table that wont be in excel sheet (like id)
			if p[0].lower() in ["id", "record_timestamp", "timestamp","errors","lastchangedate","toxbatchrecordid","toxicityresultsrecordid","picture_url", "coordinates", "device_type", "qcount", "status"]:
				errorLog("Core: Not adding: %s to primary_key_list" % p[0])
			else:
				primary_key_list.append(p[0].lower())
		errorLog("Core: Primary key list with which to check for duplicates: %s" % primary_key_list)

		df_duplicates = df.duplicated(primary_key_list)	
		# create dataframe of all duplicates both only the first occurence for searching through later
		df_duplicates_first = df.drop_duplicates(primary_key_list)
		errorLog("Core: Duplicate dataframe below:")
		errorLog(df_duplicates_first)

		count = 0
		for item_number in df_duplicates.index:
			#print("index: %s and value: %s") % (i, df_duplicates[i])
			if(df_duplicates[item_number]):
				errorLog("index: %s and value: %s" % (item_number, df_duplicates[item_number]))
				#errorLog("row: %s" % df.iloc[[item_number, primary_key_list])
				#errorLog("row: %s" % df_duplicates_first.isin(df.iloc[[item_number], [0,1,6]]))
				message = "<font color=orange>This row is duplicate of another in this same submission. Fields %s in combination create a unique submission. Please remove the duplicate row/s.</font>" % (primary_key_list)
				#xmessage = "SESSION[%s]: { type: 'duplicates in session', error: %s }" % (str(count),primary_key_list)
				#unique_error = '{ "column": "", "error_type": "Duplicate Submission", "error": "%s" }' % (primary_key_list)
				human_error = 'You have a duplicate record that matches on the following fields: %s' % (primary_key_list)
				unique_error = '{ "column": "", "error_type": "Duplicate Session Submission", "error": "%s" }' % (human_error)
				addErrorToList("errors",item_number,unique_error,df)
				addErrorToList("duplicate_session_submission",item_number,unique_error,df)
				#errorLog(unique_error)
				#df.ix[item_number, 'duplicate_session_submission'] = message
				#if 'errors' in df.columns:
				#	df.ix[int(item_number), 'errors'] = str(df.ix[item_number, 'errors']) + xmessage
				#else:
				#	df.ix[int(item_number), 'errors'] = xmessage
				df.ix[int(item_number), 'row'] = str(item_number)
				errorsCount(errors_dict,"duplicate")
			count = count + 1
	errorLog("end checkDuplicatesInSession")
	return df


# check submission against database metadata
def checkTableMetadata(db,dbtype,eng,table_match,errors_dict,df):
	def checkConstraint(row_number,row,schema):
		errorLog("Core: Validate row_number: %s, row: %s, schema: %s" % (row_number,row,schema))
        	result = v.validate(row,schema)
        	if(result):
        		errorLog("Core: Successful validation: %s " % result)
		else:
                	errorLog("Core: Failed validation: %s " % result)
			count = 0
			for e in v.errors:
				# validation
				# lookup
				errorLog(e)
				errorLog("v.errors: %s" % v.errors[e][0])
				if "float" in v.errors[e][0]:
					#human_error = ("You have text ('%s') in a number field - <a href=http://192.168.1.24:5000/tbltoxicitybatchinformation/meta>help</a>" % row[e])
					human_error = ("You have text ('%s') in a number field." % row[e])
					unique_error = '{"column": "%s", "error_type": "Data Type", "error": "%s"}' % (e,human_error)
				elif "empty values not allowed" in v.errors[e][0]:
					human_error = "You left a required field empty"
					unique_error = '{"column": "%s", "error_type": "Data Type", "error": "%s"}' % (e,human_error)
				else:
					# there may be an issue with using row[e] instead of just row - for now will leave as is 5nov17
					errorLog("column: %s, error: %s, row: %s" % (e,v.errors[e],row[e])); # removed 4nov17 may have issues
					#errorLog("column: %s, error: %s, row: %s" % (e,v.errors[e],row)); # replacement 4nov17
					#unique_error = '{"column": "%s", "error_type": "Data Type", "error": "%s"}' % (e,v.errors[e]) # replacement 4nov17 
					unique_error = '{"column": "%s", "error_type": "Data Type", "error": "%s/%s"}' % (e,v.errors[e],row[e]) # removed 4nov17 may have issues
					#unique_error = json.dumps(unique_error_string, ensure_ascii=True)
				errorLog("Core: field_unique_error: %s" % unique_error)
				# redundant_error = "{ row: %s, column: %s, error_type: 'Data Type', error: %s }" % (row_number,e,v.errors[e])
				addErrorToList("errors",row_number,unique_error,df)
				addErrorToList("field_error",row_number,unique_error,df)
				errorsCount(errors_dict,'mia')
				count = count + 1
			# ix index must be an int or it wont work
			df.ix[int(row_number), 'row'] = str(row_number)
		#return errorcount

	errorLog("Core: start checkTableMetadata on %s" % table_match)
	statusLog("Check Table Metadata for table: %s" % table_match)
	system_fields = current_app.system_fields
	df['errors'] = np.nan
	## get metadata from matching table ##
	if dbtype == "mysql" or dbtype == "mysql-rest":
		sql_match = "select column_name,data_type,character_maximum_length,is_nullable,numeric_precision,numeric_scale from information_schema.columns where table_schema = '%s' and table_name = '%s'" % (db, table_match) #  mysql - working but added decimal precision and scale
	elif dbtype == "postgresql" or dbtype == "azure":
		# added numeric precision and scale on 25july17 to check decimal placeholder
		sql_match = "select column_name,data_type,character_maximum_length,is_nullable,numeric_precision,numeric_scale from information_schema.columns where table_catalog = '%s' and table_name = '%s'" % (db, table_match) # postgresql and azure
	errorLog("Core: sql statement: %s" % sql_match)
	sql_schema = eng.execute(sql_match)
	rows = sql_schema.cursor.fetchall()
	sql_schema.close()
	rows_count = len(rows)
	errorLog("Core: query returned %s rows" % rows_count)
	## get metadata from matching table ##

	## build validation schema from matching table ## mysql, postgresql, and azure data types
	def datatypeSwap(term):
    		if(term == "NO"):
       			return True                                                                                   
    		elif(term == "YES"):                             
       			return False
    		elif(term == "character varying"):                             
       			return "string"
    		elif(term == "varchar"):                             
       			return "string"
    		elif(term == "nvarchar"):                             
       			return "string"
    		elif(term == "int"):                             
       			return "integer"
    		elif(term == "smallint"):                             
       			return "integer"
		elif(term == "double"):                             
        		return "float"
		elif(term == "double precision"):                             
        		return "float"
    		elif(term == "numeric"):                             
       			return "float"
		elif(term == "smalldatetime"):                             
        		return "date"
		elif(term == "timestamp without time zone"):                             
        		return "integer"
    		#elif(term == "None"):                             
       		#	return "255"
    		#elif(term == None):                             
       		#	return "255"
    		else:
       			return term

	## create schema on sql metadata
	count_rows = 1
	schema = {}
	for r in rows:
		#errorLog("Core: count_rows: %s and rows_count: %s" % (count_rows,rows_count))
		# lowercase sql column names
		itemkey = r[0].lower() # column_name
		req = datatypeSwap(r[3]) # is_nullable
		typ = datatypeSwap(r[1]) # data_type
		if typ == "float":
			max = datatypeSwap(r[4]) # precision
		else:
			max = datatypeSwap(r[2]) # character_maximum_length
		# added numeric precision and scale on 25july17
		#prec = datatypeSwap(r[4])
		#scal = datatypeSwap(r[5])

		if itemkey not in system_fields:
			schema[itemkey] = {}
			schema[itemkey]['required'] = req
			if req == True:
				schema[itemkey]['empty'] = False
			if req == False:
				schema[itemkey]['nullable'] = True
			# major change allow text or number in a database field configured for text - 22sep17 - added for ocpw project machine submission
			if typ == "text" or typ == "string":
				schema[itemkey]['type'] = ['string', 'number']
			else:
				schema[itemkey]['type'] = typ
			#if typ == "int" or type == "string":
			if max:
				schema[itemkey]['maxlength'] = max
			errorLog("Core: field: %s, required: %s, type: %s, maximum: %s" % (itemkey,req,typ,max))
		count_rows += 1

	### RUN DATAFRAME AGAINST SCHEMA ###
	### turn dataframe into json string
	## v = Validator(error_handler=CustomErrorHandler(custom_messages={'empty': {'special bulletin for empty'}}))
	v = Validator()
	## leave off errors column from validation
	# make a copy of dataframe

	#### replace nan with empty values for string - nan is not a string, validation will break #### 
	# http://stackoverflow.com/questions/17173524/pandas-dataframe-object-types-fillna-exception-over-different-datatypes
	#df = df.fillna('') - replaces everything and changes datatypes
	### ** dangerous code - changes data ** ###
	for col in df:
    		#get dtype for column
    		dt = df[col].dtype 
    		if dt == object:
        		df[col].fillna("", inplace=True)
	
	dfjson = df
	# drop errors column
	del dfjson['errors']
	try:
		df_json_string = dfjson.to_json(orient='index')
		### now json object
		df_json_object = (json.loads(df_json_string))

		for i in df_json_object:
    			checkConstraint(i,df_json_object[i],schema)
	except:
		raise ValueError('Failed to convert dataframe to json.')
	### end matchTableMetadata
	errorLog("Core: end checkTableMetadata")
	return df

def getIndividualErrors(df):
	errorLog("Core: start getIndividualErrors")
	tmp_dict = {}
	#  clear dataframe of rows that have no errors
	dfjson = df
        dfjson = dfjson[pd.notnull(dfjson['row'])]
        # must re-index dataframe - set to 0 after removing rows
	dfjson.reset_index(drop=True,inplace=True) 
	count = 0
	#errorLog(dfjson)
	for index, row in dfjson.iterrows():
		# valid json -
		#errorLog(row["errors"])
		#errorLog('{{"{0}": {{"row": "{1}", "value": [{2}]}}}}'.format(count,row["row"],row["errors"]))
		#tmp_dict[count] = '"{0}": {{"row": "{1}", "value": [{2}]}}'.format(count,row["row"],row["errors"])
		#tmp_dict[count] = row["errors"]
		tmp_dict[count] = '[{"row":"%s","value":[%s]}]' % (row['row'],row["errors"])
		#errorLog(json_string)
		#errorLog(tmp_dict)
		#errorLog("row: %s" % row["row"])
		#errorLog("errors: %s" % row["errors"])
		#tmp_dict['{"pear": "fish", "apple": "cat", "banana": "dog"}'I
		count = count + 1
	errorLog("Core: end getIndividualErrors")
	#tmp_json = json.dumps(tmp_dict, ensure_ascii=True)
	#errorLog(tmp_json)
	#return tmp_json
	return tmp_dict

def getRedundantErrors(check,df):
	errorLog("Redundant Core: start getRedundantErrors")
	errorLog("Redundant Core: checking on: %s" % check)
	# group  by dataframe by field_errors and print row numbers
	tmp_dict = {}

	# new code below to fix bug number 9 - extract whole row that may contain multiple errors which can be put into a single column and then groupby
	# get the row number and error record
	tmp_df = df[['row',check]]
	errorLog("tmp_df")
	errorLog(tmp_df)
	errorLog("gextract:")
	# extract all error records based on {} - if there are multiple errors in a cell this will extract them
	gextract = tmp_df[check].str.extractall(r'(\{.*?\})').stack().reset_index(level=1, drop=True)
	errorLog(gextract)
	# based on whether the error set we are looking at has multiple errors per cell we will provide two different ways of combining data together 
	# new dataframe with broken up errors per line
	df_extract = pd.DataFrame(columns = ('row',check))
	dcount = 0
	if gextract.index.get_duplicates():
		errorLog("We have duplicate indexes")
		errorLog(gextract.index.get_duplicates())
		for i in gextract.index.values:
			for r in gextract[i]:
				errorLog("start")
				errorLog(r)
				errorLog("dcount: %s, row: %s, value: %s" % (dcount,i[0],r))
				df_extract.loc[dcount] = [i[0],r]
				dcount = dcount + 1
				errorLog("end")
		# drop duplicates from new dataframe by row and error
		df_extract = df_extract.drop_duplicates(['row',check])
		errorLog(df_extract)
	else:
		errorLog("No duplicate indexes")
		errorLog(gextract.index.get_duplicates())
		for i in gextract.index.values:
			errorLog("dcount: %s, row: %s, value: %s" % (dcount,i[0],gextract[i]))
			df_extract.loc[dcount] = [i[0],gextract[i]]
			dcount = dcount + 1
		# drop duplicates from new dataframe by row and error
		df_extract = df_extract.drop_duplicates(['row',check])
		errorLog(df_extract)
	count = 0
	#for error_message,group in df.groupby(check):  - old code can be deleted later 21sep17
	for error_message,group in df_extract.groupby(check):
		print(error_message)
		print(group)
		# only return errors if there are more one (redundant)
		#errorLog("Redundant Core: grouped rows count: %s" % len(group.row))
		if len(group.row) > 1:
			row_fix = []
			# for custom checks we look at the tmp_row column instead of row
			if check == "custom_error_bs" or check == "stddev_errors" or check == "mean_ee_errors" or check == "mean_mg_errors" or check == "coef_errors" or check == "toxicity_errors" or check == "toxicity_summary_errors":
				for r in group.tmp_row:
					row_fix.append(str(int(r) + 2))
				rows = ', '.join(row_fix)
			else:
				errorLog("group.row: %s" % group.row)
				for r in group.row:
					row_fix.append(str(int(r) + 2))
				rows = ', '.join(row_fix)
			errorLog(row_fix)
			errorLog('Redundant Core: [{"rows":"%s","value":[%s]}]' % (rows,error_message))
			tmp_dict[count] = '[{"rows":"%s","value":[%s]}]' % (rows,error_message)
		count = count + 1
	# drop temporary column
	#del df['groupbyextract']
	errorLog("Redundant Core: end getRedundantErrors")
	return tmp_dict
	#return - why is this here?

def checkLookupCodes(db,dbtype,eng,table_match,errors_dict,df):
	errorLog("Core: start checkLookupCodes")
	statusLog("Check Lookup Codes for table: %s" % table_match)
	match_dataframe_to_tables = df.keys() # removed commment out on 9feb17
	def lookupCodes(table,column):
		errorLog("Core: start lookupCodes")
		if dbtype == "mysql" or dbtype == "mysql-rest":
			sql = "select referenced_column_name,referenced_table_name from information_schema.key_column_usage where table_name =  '%s' and column_name = '%s' and constraint_name like '%s'" % (table,column,'fk%%') # mysql
		# specifc to postgresql or databases where column name has _id in it
		elif dbtype == "postgresql":
			sql = "SELECT ccu.column_name AS foreign_column_name,ccu.table_name AS foreign_table_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='%s' AND kcu.column_name='%s'" % (table,column) # postgresql
		elif dbtype == "azure":
			sql = "SELECT col2.name AS [referenced_column], tab2.name AS [referenced_table] FROM sys.foreign_key_columns fkc INNER JOIN sys.objects obj ON obj.object_id = fkc.constraint_object_id INNER JOIN sys.tables tab1 ON tab1.object_id = fkc.parent_object_id INNER JOIN sys.schemas sch ON tab1.schema_id = sch.schema_id INNER JOIN sys.columns col1 ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id INNER JOIN sys.tables tab2 ON tab2.object_id = fkc.referenced_object_id INNER JOIN sys.columns col2 ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id WHERE tab1.name = '%s' AND col1.name = '%s'" % (table,column) # azure
		errorLog("Core: sql statement: %s" % sql)
		query = eng.execute(sql)
		for q in query:
			# if we have a foreign key we need to check lookup tables and corresponding submission 
			#select LabCode from tblToxicityBatchInformation
			foreign_key_field = q[0]
			foreign_key_table = q[1]
			if dbtype == "mysql" or dbtype == "mysql-rest":
				sub_sql = "select %s from %s" % (foreign_key_field,foreign_key_table) # mysql
			elif dbtype == "postgresql":
				sub_sql = 'select "%s" from "%s"' % (q[0],q[1]) # postgresql
			elif dbtype == "azure":
				sub_sql = 'select "%s" from %s' % (q[0],q[1]) # azure
			errorLog("Core: sub_sql: %s" % sub_sql)
			sub_query = eng.execute(sub_sql)
			sub_rows = sub_query.cursor.fetchall()
			#errorLog(sub_rows)
			if sub_rows:
				errorLog("NOT A PROBLEM")
				sub_list = []
				for s in sub_rows:
					errorLog(s[0])
					sub_list.append(s[0])
				return q[1],sub_list
			# there is a foreign key but no records we need to check against excel submission instead
			else:
				errorLog("HERE IS OUR PROBLEM")
				# find foreign table that matches excel submission
				for key in match_dataframe_to_tables: # Index([u'email', u'user_id'], dtype='object')
					#errorLog(key)
					#errorLog(foreign_key_table)
					#mdt = key.strip().split(" - ") # modified 9feb17
					#if foreign_key_table == mdt[2]: # modified 9feb17
					if foreign_key_table == key:
						errorLog("match: %s" % key)
						#errorLog("match: %s" % mdt[2]) # modified 9feb17
						# get the foreign key fields from excel submission as a list
						errorLog("Core: Foreign key fields from submission as a list:")
						errorLog(all_dataframes[key][foreign_key_field.lower()].astype(str).tolist())
						sub_list = all_dataframes[key][foreign_key_field.lower()].tolist()	
						return q[1],sub_list

					# what do we do if we cant find a matching foreign key table - user didnt submit
					# probably just log - the user will end up with lookup errors
					else:
						errorLog("Core: Unable to find foreign key table in excel submission")
			sub_query.close()
		query.close()
	if dbtype == "mysql" or dbtype == "mysql-rest":
		sql_match = "select column_name,data_type,character_maximum_length,is_nullable from information_schema.columns where table_schema = '%s' and table_name = '%s'" % (db, table_match) # mysql
	elif dbtype == "postgresql" or dbtype == "azure":
		sql_match = "select column_name,data_type,character_maximum_length,is_nullable from information_schema.columns where table_catalog = '%s' and table_name = '%s'" % (db, table_match) # postgresql  and azure
	errorLog("Core: sql_match: %s" % sql_match)
	sql_schema = eng.execute(sql_match)

	rows = sql_schema.cursor.fetchall()
	sql_schema.close()

	lookup_list = {}
	for r in rows:
		# only return columns which have a foreign key - lookup code
		errorLog("Core: Columns to check for foreign keys: %s" % r[0])
		#lookup_list[r[0]] = lookupCodes(table_match,r[0])
		if (lookupCodes(table_match,r[0])): #- code doesn't make sense
			lookup_list[r[0]] = lookupCodes(table_match,r[0])

	for item in lookup_list:
		message = ""
		errorLog("Core: We are looping through item: %s" % item)
		# make it a string in case field names are unicode
    		lower_item = str(item.lower())
		if lower_item not in ["id","objectid","record_timestamp","timestamp","errors","lastchangedate","toxbatchrecordid","toxicityresultsrecordid","picture_url","coordinates","device_type","qcount","status","created_date","created_user","last_edited_date"]:
			# lookup_list[item][0] = lulist name
			# lookup_list[item][1] = lulist values
			errorLog("0: %s" % lookup_list[item][0])
			errorLog("1: %s" % lookup_list[item][1])
    			df_items = df[~df[lower_item].isin(lookup_list[item][1])]
    			if(df_items.empty):
        			print("empty")
    			else:
				errorLog("IN LOOP")
				errorLog(df_items[lower_item].index)
				row_number = df_items[lower_item].index
				if 'lookup_error' not in df.ix[row_number]:
					df.ix[row_number, 'lookup_error'] = ""
				count = 0
				for item_number in (df_items[lower_item].index):
					errorLog(item_number)
					#xmessage = "LOOKUP[%s]: { type: lookup, column: %s, error: %s }" % (str(count),item,df_items[lower_item].loc[item_number])
					human_error = ("The data inserted ('%s') does not match the lookup list <a href='http://checker.sccwrp.org/smc/scraper?action=help&layer=%s' target='_blank'>%s</a> for the column" % (df_items[lower_item].loc[item_number],lookup_list[item][0],lookup_list[item][0]))
					unique_error = '{ "column": "%s", "error_type": "Lookup Fail", "error": "%s" }' % (item,human_error)
					addErrorToList("errors",item_number,unique_error,df)
					addErrorToList("lookup_error",item_number,unique_error,df)
					errorsCount(errors_dict,"lookup")
					count = count + 1
	errorLog("Core: end checkLookupCodes")
	return df

core_checks = Blueprint('core_checks', __name__)

@core_checks.route("/core", methods=["POST"])
def core(all_dataframes,sql_match_tables,errors_dict):
	errorLog("Blueprint - Core")
	statusLog("Starting Core Checks...")
	#print(all_dataframes)
	#print(sql_match_tables)
	#all_dataframes = current_app.all_dataframes
	db = current_app.db
	dbtype = current_app.dbtype
	eng = current_app.eng
	TIMESTAMP=str(session.get('key'))
	sheet_names = []
	# return error variables
	data_checks = {}
	data_checks_redundant = {}
	#list_errors = [] - moved down
	#list_redundant_errors = [] - moved down
	try:
		message = "Core: Start looping through each dataframe."
		errorLog(message)
		statusLog(message)
		# set state to 0 to start
		state = 0
		#message = "Core: %s" % all_dataframes.keys()
		# counter for applying errros to tabs
		count = 0
		for dataframe in all_dataframes.keys():
			list_errors = []
			list_redundant_errors = []
			df_sheet_and_table_name = dataframe.strip().split(" - ")
			sheet_name = str(df_sheet_and_table_name[0])
			sheet_names.append(str(df_sheet_and_table_name[1]))
			table_name = str(df_sheet_and_table_name[2])
			errorLog("Core: sheetname: %s" % sheet_name)
			errorLog("Core: tablename: %s" % table_name)
			### DATABASE BUSINESS RULES CHECKS ###
			try:
				# check for empty rows
				errorLog("Check for empty rows:")
				empty_rows = all_dataframes[dataframe].index[all_dataframes[dataframe].isnull().all(1)].tolist()
				errorLog(empty_rows)
				if empty_rows:
					for item_number in empty_rows:
						errorLog("Add empty row error to list: %s" % item_number)
			                        unique_error = '{ "column": "", "error_type": "Empty Row", "error": "Empty row please remove." }'
                                		addErrorToList("errors",item_number,unique_error,all_dataframes[dataframe])
						errorsCount(errors_dict,'mia')
                                	#tab.dropna(axis=0, how='all', inplace=True)
	                        	# drop all empty rows
                                	all_dataframes[dataframe].dropna(axis=0, how='all', inplace=True)
					state = 1
				else:
					state = 0
			except ValueError:
				message = "Critical Error: Failed to run check for empty rows."
				state = 1
			if state != 1:
				try:
					# check field requirements, types, and sizes - SIZES TO DO
					checkTableMetadata(db,dbtype,eng,table_name,errors_dict,all_dataframes[dataframe])
					errorLog("state: %s" % state)
					errorLog(all_dataframes[dataframe])
        				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
					sql_session = "update submission_tracking_table set mia = 'yes' where sessionkey = '%s'" % TIMESTAMP
        				session_results = eng.execute(sql_session)
        				eng.dispose()
				except ValueError:
					message = "Critical Error: Failed to run checkTableMetadata"
					errorLog("Issue: %s on %s" % (message,table_name))
					#errorLog(all_dataframes[dataframe])
        				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
					sql_session = "update submission_tracking_table set mia = 'no' where sessionkey = '%s'" % TIMESTAMP
        				session_results = eng.execute(sql_session)
        				eng.dispose()
					#state = 1
			if state != 1:
				try:
					errorLog("db: %s, dbtype: %s, eng: %s, table_name: %s" % (db,dbtype,eng,table_name))
					errorLog(all_dataframes[dataframe])	
					checkLookupCodes(db,dbtype,eng,table_name,errors_dict,all_dataframes[dataframe])
        				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
					sql_session = "update submission_tracking_table set lookup = 'yes' where sessionkey = '%s'" % TIMESTAMP
        				session_results = eng.execute(sql_session)
        				eng.dispose()
				except ValueError:
					message = "Critical Error: Failed to run checkLookupCodes"
        				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
					sql_session = "update submission_tracking_table set lookup = 'no' where sessionkey = '%s'" % TIMESTAMP
        				session_results = eng.execute(sql_session)
        				eng.dispose()
					state = 1
			if state != 1:
				try:
					checkDuplicatesInSession(db,dbtype,eng,table_name,errors_dict,all_dataframes[dataframe])
        				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
					sql_session = "update submission_tracking_table set duplicates = 'yes' where sessionkey = '%s'" % TIMESTAMP
        				session_results = eng.execute(sql_session)
        				eng.dispose()
				except ValueError:
					message = "Critical Error: Failed to run checkDuplicatesInSession"
        				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
					sql_session = "update submission_tracking_table set duplicates = 'no' where sessionkey = '%s'" % TIMESTAMP
        				session_results = eng.execute(sql_session)
        				eng.dispose()
					state = 1
			if state != 1:
				try:
					checkDuplicatesInProduction(db,dbtype,eng,table_name,errors_dict,all_dataframes[dataframe])
        				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
					sql_session = "update submission_tracking_table set duplicates = 'yes' where sessionkey = '%s'" % TIMESTAMP
        				session_results = eng.execute(sql_session)
        				eng.dispose()
				except ValueError:
					message = "Critical Error: Failed to run checkDuplicatesInProduction"
        				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
					sql_session = "update submission_tracking_table set duplicates = 'no' where sessionkey = '%s'" % TIMESTAMP
        				session_results = eng.execute(sql_session)
        				eng.dispose()
					state = 1
			# retrieve all the errors that were found - to json - return to browser
			try:
				if 'errors' in all_dataframes[dataframe]:
					list_errors.append(getIndividualErrors(all_dataframes[dataframe]))
					#data_checks = getIndividualErrors(dataframe)
				if 'field_error' in all_dataframes[dataframe]:
					list_redundant_errors.append(getRedundantErrors("field_error",all_dataframes[dataframe]))
				if 'lookup_error' in all_dataframes[dataframe]:
					list_redundant_errors.append(getRedundantErrors("lookup_error",all_dataframes[dataframe]))
				if 'duplicate_production_submission' in all_dataframes[dataframe]:
					list_redundant_errors.append(getRedundantErrors("duplicate_production_submission",all_dataframes[dataframe]))
				if 'duplicate_session_submission' in all_dataframes[dataframe]:
					list_redundant_errors.append(getRedundantErrors("duplicate_session_submission",all_dataframes[dataframe]))
				#data_checks[count] = str(list_errors)
				#data_checks = str(list_errors)
				data_checks[count] = json.dumps(list_errors, ensure_ascii=True)
				#data_checks[count] = list_errors
				data_checks_redundant[count] = json.dumps(list_redundant_errors, ensure_ascii=True)
			except ValueError:
				message = "Core: Failed to retrieve errors."
				state = 1
			count = count + 1
		# close engine connections
		eng.dispose()
		#state = 0
		#errorLog(json.loads(data_checks))
		#errorLog(json.dumps(data_checks, ensure_ascii=True))
		print("End core checks")
		return data_checks, data_checks_redundant, errors_dict
	except ValueError:
		message = "Critical Error: Failed to run core checks"	
		errorLog(message)
		state = 1
	#return jsonify(message=message,state=state,business=data_checks,redundant=data_checks_redundant)
