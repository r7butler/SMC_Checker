import os.path
from flask import Blueprint, request, jsonify
import pandas as pd
import numpy as np
import urllib, json
from sqlalchemy import create_engine, exc, Table, Column, Integer, Unicode, MetaData, String, Text, update, and_, select, func, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry
from odo import odo
import re
import random 
import datetime
from random import randint
from .ApplicationLog import *

def createStagingTable(table_name,df,database_to_use,TIMESTAMP):
	errorLog("start createStagingTable")
	statusLog("Create Staging Table")
	errorLog(table_name)
	errorLog(df.columns)
	errorLog(TIMESTAMP)
	# placeholder SCCWRP is for agency variable in future
	staging_table_name = "staging_agency_" + table_name + "_" + TIMESTAMP
     	# create engine, reflect existing columns, and create table object for oldTable
	if 'field_error' in  df:
		df = df.drop('field_error', 1)
	if 'logic_error' in  df:
		df = df.drop('logic_error', 1)
	if 'lookup_error' in  df:
		df = df.drop('lookup_error', 1)
	if 'duplicate_production_submission' in  df:
		df = df.drop('duplicate_production_submission', 1)
	if 'duplicate_session_submission' in df:
		df = df.drop('duplicate_session_submission', 1)
	if database_to_use == 'smcphab':
		db = "smcphab"  # postgresql
		dbtype = "postgresql"
     		srcEngine = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
     		srcEngine._metadata = MetaData(bind=srcEngine,schema='sde')
     		srcEngine._metadata.reflect(srcEngine) # get columns from existing table
     		srcTable = Table(table_name, srcEngine._metadata)
     		destEngine = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
     		destEngine._metadata = MetaData(bind=destEngine,schema='sde')
		errorLog(staging_table_name)
     		destTable = Table(staging_table_name, destEngine._metadata)
		def getRandomTimeStamp(row):
			row['objectid'] = int(TIMESTAMP) + int(row.name)
			return row
		df = df.apply(getRandomTimeStamp, axis=1)
		errorLog(df['objectid'])
		# timestamp to date format - bug fix #4
		timestamp_date = datetime.datetime.fromtimestamp(int(TIMESTAMP)).strftime('%Y-%m-%d %H:%M:%S')
		df['created_user'] = "checker"
		#df['created_date'] = "2017-07-13 21:51:00"
		df['created_date'] = timestamp_date
		df['last_edited_user'] = "checker"
		#df['last_edited_date'] = "2017-07-13 21:51:00"
		df['last_edited_date'] = timestamp_date
	elif database_to_use == 'demo':
		db = "demo2"  # postgresql
     		srcEngine = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/demo2')
     		srcEngine._metadata = MetaData(bind=srcEngine,schema='sde')
     		srcEngine._metadata.reflect(srcEngine) # get columns from existing table
     		srcTable = Table(table_name, srcEngine._metadata)
     		destEngine = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/demo2')
     		destEngine._metadata = MetaData(bind=destEngine,schema='sde')
		errorLog(staging_table_name)
		errorLog(destEngine._metadata)
     		destTable = Table(staging_table_name, destEngine._metadata)
		# add objectid column to dataframe
		#df['objectid'] = int(uuid4().int & (1<<64)-1)
		#df['objectid'] = int(uuid1().int>>64)
		def getRandomTimeStamp(row):
			row['objectid'] = int(TIMESTAMP) + int(row.name)
			return row
		df = df.apply(getRandomTimeStamp, axis=1)
		errorLog(df['objectid'])
     	# copy schema and create newTable from oldTable
	#for dest_column in destTable.columns:
	#	if dest_column == "id":
	#		dest_column.drop()
     	for column in srcTable.columns:
		errorLog("srcTable.columns")
		# get everthing to right of the dot on tbltoxicityresults.objectid
		#if (tail != "id") or (tail != "objectid") or (tail != "shape") or (tail != "gdb_geomattr_data"):
		if str(column) != "tbltoxicityresults.id":
			errorLog(column)
			destTable.append_column(column.copy())
	destTable.create()
	try:
		if database_to_use == 'smcphab':
			df.columns = [x.lower() for x in df.columns]
			outcome = df.to_sql(name=staging_table_name,con=destEngine,if_exists='append',index=False)
			# use odo instead of pandas - should be faster
			#odo('myfile.*.csv', 'postgresql://hostname::tablename')
			#outcome = odo(df, destEngine)  # Migrate dataframe to Postgres
			#outcome = df.to_sql(name=staging_table_name,con=destEngine,flavor='mysql',if_exists='append',index=True,index_label="id")
		elif database_to_use == 'bight2018geo':
			# lowercase all column names in preparation for match
			df.columns = [x.lower() for x in df.columns]
			outcome = df.to_sql(name=staging_table_name,con=destEngine,if_exists='append',index=False)
		elif database_to_use == 'demo':
			# lowercase all column names in preparation for match
			df.columns = [x.lower() for x in df.columns]
			outcome = df.to_sql(name=staging_table_name,con=destEngine,if_exists='append',index=False)
	except ValueError:
		errorLog("failed df.to_sql")
		errorLog(outcome)
	errorLog("end createStagingTable")
	return staging_table_name

#### retrieves all of tables and columns in the database for use in matching by dcMatchColumnsToTable function ###
def dcGetTableAndColumns(db,dbtype,eng):
	errorLog("start dcGetTableAndColumns")
	system_fields = current_app.system_fields # hidden database fields like id
	sqlFields = {}
	if dbtype == "mysql" or dbtype == "mysql-rest":
		query = eng.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s' and TABLE_NAME LIKE '%s'" % (db,"tbl%%"))
	elif dbtype == "postgresql":
		# added BASE TABLE filter - exclude views
		query = eng.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_CATALOG='%s' AND TABLE_NAME LIKE '%s'" % (db,"tbl_%%"))
	elif dbtype == "azure":
		query = eng.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG ='%s' and TABLE_NAME LIKE '%s'" % (db,"tblToxicity%%")) # microsoftsql
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
	errorLog("end dcGetTableAndColumns")
	return sqlFields

def dcMatchColumnsToTable(tab_name,tab,sqlFields,tabCounter):
	# purpose: does the data match a table, if not we should let user know which table it matches closest to
	# result: true, false/matched table or closest matching table/columns matched
	# tab_name = name of tab or sheet being worked on
	# tab = rows of data in the individual tab or sheet
	# sqlFields = dictionary of tables and columns in database
	# tabCounter = numeric identity of tab or sheet being worked on
	errorLog("start dcMatchColumnsToTable")
	statusLog("Check Matching Columns to Table")
	matchset = False # if we can match the tab or sheet to a table then this gets returned at bottom
	match = [] # matched columns to return
	nomatch = [] # unmatched columns to return
	tabColumns = [] # list of lowercase column names for matching
	counter_key = ""
	tabCount = len(tab.columns) # how many columns are in the tab or sheet
	#tabColumns.append([x.lower() for x in tab.columns]) # lowercase tab.columns - easiest way to match against sql fields
	tableCountList = [] # total match for each table to tab comparison - ie. tblresult = 8, tblbatch = 3, etc..
	for table in sqlFields:
		# TURN ON FOR DEBUGGING
		errorLog("-----Loop through table: %s" % table)
		tableCount = 0
		# loop through each column in a table
		collect_columns = []
		# failed columns 
		# minus id
		columnCount = len(sqlFields[table]) # how many columns are in the table
		errorLog("columnCount: %s" % columnCount)
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
		if tableCount == tabCount:
			# TURN ON FOR DEBUGGIN
			#errorLog("-----+Sheet %s is matched to table %s with count %s" % (tab_name,table,str(tableCount)))
			#counter_key = str(tabCounter) + "-" + tab_name + "-" + str(tabCount) + "-" + table + "-" + "True" + "-" + str(tableCount) + "-" + str(collect_columns) 
			counter_key = str(tabCounter) + "-" + tab_name + "-" + str(tabCount) + "-" + table + "-" + "True" + "-" + str(tableCount) + "-" + str(','
.join(collect_columns)) 
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
				# TURN ON FOR DEBUGGIN print("EQUAL: %s" % match)
				# TURN ON FOR DEBUGGIN print("EQUALCOUNT: %s" % len(match))
				return True, match
			#errorLog("tab is set to: %s and value is: %s" % (matchset, match))
		else:
			# find columns that do not match return to user
			collect_failed_columns = set(tab.columns)^set(collect_columns)
			counter_key = str(tabCounter) + "-" + tab_name + "-" + str(tabCount) + "-" + table + "-" + "False" + "-" + str(tableCount) + "-" + str(','.join(collect_columns)) + "-" + str(','.join(collect_failed_columns))
			nomatch.append(counter_key)
			#errorLog("tab is set to: %s and value is: %s" % (matchset, nomatch))
	if matchset == True:
		return True, match
	else:
		# we need to find the closest match in the nomatch list - the largest tabCount
		#tchColumnsToTable max gives you the largest element in list - index on the outer gives you the element index
		if max(tableCountList):
			closest_match = tableCountList.index(max(tableCountList))
		# in case there are no matches
		else:
			#closest_match = tableCountList[0]
			closest_match = 0
			nomatch[0] = "%s-%s-%s-%s-%s-%s" % (str(tabCounter),tab_name,str(tabCount),"None","False","No match for tab")
		#errorLog(closest_match)
		#errorLog(nomatch[0])
		errorsCount("match")
		return False, nomatch[closest_match]
	errorLog("end dcMatchColumnsToTable")


def moveStagingToProduction(eng,staging,destination,records,fieldlist):
	errorLog("start moveStagingToProduction")
	statusLog("Move Staging to Production")
	# we will need the engine, staging table name, destination table name, and sql fields
	errorLog("staging table: %s" % staging)
	errorLog("destination table: %s" % destination)
	# change table to view - this code only gets used when we are dealing with geodatabase versioned or archived tables
	# there are some additional fields that get created and you can only submit changes directly through the view not the tables
	#destination = destination + "_evw"
	errorLog("records submitted: %s" % records)
	errorLog("destination fields: %s" % fieldlist)
	errorLog("destination type: %s" % type(fieldlist))
	fields = fieldlist
	#sql = "insert into sde." + destination + " ("+ fields + ") select " + fields + " from sde." + staging
	#insert into tblfieldversion_evw (objectid, globalid) values (sde.next_rowid('bight18','tblfieldversion_evw'), sde.next_globalid());
	#test = "'sde','tblfieldversion_evw'"
	#sql = 'insert into "' + destination + '" (objectid, globalid, created_user, created_date, last_edited_user, last_edited_date, '+ fields + ') select sde.next_rowid('sde','tblfieldversion_evw'), sde.next_globalid(), created_user, created_date, last_edited_user, last_edited_date, ' + fields + ' from "' + staging + '"'
	sql = 'insert into "' + destination + '" (objectid, globalid, created_user, created_date, last_edited_user, last_edited_date, '+ fields + ') select sde.next_rowid(%s,%s), sde.next_globalid(), created_user, created_date, last_edited_user, last_edited_date, ' + fields + ' from "' + staging + '"'
	errorLog(sql)
	status = eng.execute(sql,"sde",destination)
	errorLog(status)
	if not status:
		errorLog("inside failed status")
		raise Error, eng.error
		errorLog(eng.error)
	status.close()
	#### AFTER WE MOVE DATA - WE NEED TO EXPORT STAGING TABLE TO A FILE FOR BACKUP - THEN REMOVE STAGING TABLE ####
	#### CLEANUP OF STAGING TABLES IS BETTER AS A NIGHTLY CRONJOB
	# for this work we will need to get SQL Fields for each dataframe/table (minus id and timestamp) and then use sqlalchemy to submit dataframe-tab
	# insert into tblToxicityBatchInformation select * from SCCWRP_tblToxicityBatchInformation
	#insert into tblToxicityBatchInformation (ToxBatch,LabCode,Species,Protocol,TestStartDate,Matrix,ActualTestDuration,ActualTestDurationUnits,TargetTestDuration,TargetTestDurationUnits,TestAcceptability,Comments,ReferenceBatch) select ToxBatch,LabCode,Species,Protocol,TestStartDate,Matrix,ActualTestDuration,ActualTestDurationUnits,TargetTestDuration,TargetTestDurationUnits,TestAcceptability,Comments,ReferenceBatch from SCCWRP_tblToxicityBatchInformation
	#insert into tblToxicityResults (StationID,SampleCollectDate,ToxBatch,Matrix,LabCode,Species,Dilution,Treatment,Concentration,ConcentrationUnits,EndPoint,LabRep,Result,ResultUnits,QACode,SampleTypeCode,FieldReplicate,Comments) select StationID,SampleCollectDate,ToxBatch,Matrix,LabCode,Species,Dilution,Treatment,Concentration,ConcentrationUnits,EndPoint,LabRep,Result,ResultUnits,QACode,SampleTypeCode,FieldReplicate,Comments from SCCWRP_tblToxicityResults
	#insert into tblToxicityWQ (StationID,ToxBatch,WQMatrix,Dilution,Treatment,Concentration,ConcentrationUnits,TimePoint,Parameter,Qualifier,Result,ResultUnits,LabRep,LabCode,SampleTypeCode,Comments) select StationID,ToxBatch,WQMatrix,Dilution,Treatment,Concentration,ConcentrationUnits,TimePoint,Parameter,Qualifier, Result,ResultUnits,LabRep,LabCode,SampleTypeCode,Comments from SCCWRP_tblToxicityWQ
	return


staging_upload = Blueprint('staging_upload', __name__)

@staging_upload.route("/staging", methods=["POST"])

def staging():
	errorLog("START STAGING")
	errorLog("Blueprint - Staging")
	message = "Staging: Start upload."
	statusLog(message)
	TIMESTAMP = current_app.timestamp
	MATCH = current_app.match
	errorLog("START STAGING: %s" % TIMESTAMP)
	#stagingFile = request.data
	stagingFile = request.form['submit_file']
	# get timestamp from filename - everthing to left of dot
	TIMESTAMP,extension = stagingFile.split('.')

        # check for existence of summary file
        summary_load_file = '/var/www/smc/logs/%s-toxicity-summary.csv' % TIMESTAMP
	summary_load_file_exists = os.path.isfile(summary_load_file)

	database_to_use = request.form['database'].lower()
	errorLog(database_to_use)
	database_type = request.form['type']
	db = "smcphab"  # postgresql
	dbtype = "postgresql"
	eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
	errorLog("Database to use: %s" % db)
	errorLog("Type of database to connect to: %s" % dbtype)

	inFile = "/var/www/smc/files/" + stagingFile
	df = pd.ExcelFile(inFile, keep_default_na=False, na_values=['NaN'])
	df_tab_names = df.sheet_names
	#print(df_tab_names)
	sqlFields = dcGetTableAndColumns(db,dbtype,eng)
	errorLog(sqlFields)
	tabCounter = 0
	for tab in df_tab_names:
		tab_name = tab
		#tab = df.parse(tab) # use below instead used elsewhere
		tab = pd.read_excel(inFile, keep_default_na=False, na_values=['NaN'], sheetname = tab)
		# dont lowercase for postgresql may need to create a routine to check tab.columns = sql.columns
		#tab.columns = [x.lower() for x in tab.columns]
		match_result, match_fields = dcMatchColumnsToTable(tab_name,tab,sqlFields,tabCounter)
		#errorLog(tab.columns)
		errorLog("match_result: %s, match_fields: %s" % (match_result,match_fields))
		# routine used for getting case sensitive excel file field names
		#tmp_fields = tab.columns.values.tolist()
		#tmp_list = []
		#for t in tmp_fields:
		#	tmp_list.append('"'+str(t)+'"')
		#destination_fields = ','.join(tmp_list) # big issue lowercase or not
		#destination_fields = tab.columns.values.tolist()

		if match_result == True:
			split_match_fields = match_fields[0].split('-')
			errorLog(split_match_fields)
			destination_table = split_match_fields[3]	
			number_of_records = split_match_fields[5]	
			destination_fields = split_match_fields[6].lower() # - lowercasing names doesnt work
			try:
				staging_table = createStagingTable(destination_table,tab,database_to_use,TIMESTAMP)
			except ValueError:
				errorLog("Failed createStagingTable")
			try:
				moveStagingToProduction(eng,staging_table,destination_table,number_of_records,destination_fields)
			except ValueError:
				errorLog("Failed moveStagingToProduction")
		tabCounter = tabCounter + 1
	try:
		# if we have a table match and summary file exists then attempt to load summary into database
		if current_app.match and summary_load_file_exists:
			# LOAD USING ODO
			errorLog("# LOAD USING ODO #")
			staging_summary_name = "staging_agency_tbltoxicitysummaryresults" + "_" + TIMESTAMP
			errorLog(staging_summary_name)
			srcEngine = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
			srcEngine._metadata = MetaData(bind=srcEngine,schema='sde')
			srcEngine._metadata.reflect(srcEngine) # get columns from existing table
			srcTable = Table('tbltoxicitysummaryresults', srcEngine._metadata)
			destEngine = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
			destEngine._metadata = MetaData(bind=destEngine,schema='sde')
			errorLog(destEngine._metadata)
			destTable = Table(staging_summary_name, destEngine._metadata)
			for column in srcTable.columns:
				errorLog("srcTable.columns")
				# get everthing to right of the dot on tbltoxicityresults.objectid
				#if (tail != "id") or (tail != "objectid") or (tail != "shape") or (tail != "gdb_geomattr_data"):
				if str(column) != "tbltoxicitysummaryresults.id":
					errorLog(column)
					destTable.append_column(column.copy())
			destTable.create()
			errorLog("summary_load_file")
			#summary_load_file = '/var/www/smc/logs/%s-toxicity-summary.csv' % TIMESTAMP
			errorLog(summary_load_file)	
			dfsummary = pd.read_csv(summary_load_file)
			dfsummary.columns = [x.lower() for x in dfsummary.columns]
			# drop temporary_row - used for checks
			del dfsummary['tmp_row']
			def getRandomTimeStamp(row):
				row['objectid'] = int(TIMESTAMP) + int(row.name)
				return row
			dfsummary = dfsummary.apply(getRandomTimeStamp, axis=1)
			errorLog(dfsummary['objectid'])
			#postgresql://username:password@hostname:port
			# odo('myfile.*.csv', 'postgresql://hostname::tablename')  # Load CSVs to Postgres
			outcome = dfsummary.to_sql(name=staging_summary_name,con=destEngine,if_exists='append',index=False)
			#t = odo(dfsummary, 'postgresql://sde:dinkum@192.168.1.16:5432/smcphab::tbltoxicitysummaryresults')  # Load CSVs to Postgres
			errorLog(outcome)
	except ValueError:
		errorLog("Failed createStagingSummary")
	try:
		if current_app.match and summary_load_file_exists:
			errorLog("start moveStagingSummary")
			destination_summary_fields = "stationid,latitude,longitude,shape,stationwaterdepth,stationwaterdepthunits,areaweight,coefficientvariance,stratum,lab,sampletypecode,toxbatch,species,concentration,endpoint,units,sqocategory,mean,n,stddev,pctcontrol,pvalue,tstat,sigeffect,qacode,controlvalue"
			moveStagingToProduction(eng,staging_summary_name,'tbltoxicitysummaryresults',0,destination_summary_fields)
	except ValueError:
		errorLog("Failed moveStagingSummary")
	#emailUser()
	errorLog("END STAGING")
	return jsonify(data=stagingFile)
