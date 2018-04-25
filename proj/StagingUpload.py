import os.path
from flask import Blueprint, render_template, request, jsonify
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
from InternalEmail import *
from .ApplicationLog import *

staging_upload = Blueprint('staging_upload', __name__)

@staging_upload.route("/staging", methods=["POST"])

def staging():
	errorLog("Blueprint - Staging")
	message = "Staging: Start upload."
	statusLog(message)
	login = request.form['login']
	errorLog("login: %s" % login)
	agency = request.form['agency']
	errorLog("agency: %s" % agency)
	owner = request.form['owner']
	errorLog("owner: %s" % owner)
	year = request.form['year']
	errorLog("year: %s" % year)
	project = request.form['project']
	errorLog("project: %s" % project)
	submission_type = request.form['submission_type']
	errorLog("submission_type: %s" % submission_type)
	assignment = request.form['assignment']
	errorLog("assignment: %s" % assignment)
	delineate = request.form['delineate']
	errorLog("delineate: %s" % delineate)
	state = 0
	TIMESTAMP=str(session.get('key'))
	errorLog("Processing submission: %s" % TIMESTAMP)

	# load clean file 
	inFile = "/var/www/smc/files/" + TIMESTAMP + "-export.xlsx"
	errorLog(inFile)
	df = pd.ExcelFile(inFile, keep_default_na=False, na_values=['NaN'])
	# tab name will match table in database exactly since it is coming from the export file
       	df_tab_names = df.sheet_names
	errorLog("df_tab_names: %s" % df_tab_names)
	for tab in df_tab_names:
		table_name = tab
		df = pd.read_excel(inFile, keep_default_na=False, na_values=['NaN'], sheetname = tab)
		#destination_fields = df.columns.values.tolist()
		#errorLog("destination_fields: %s" % destination_fields)
		# get number of records - checksum
		df_number_of_rows = len(df.index)

		# drop non essential columns - not necessary since coming from originating file instead of global dataframe
		if 'row' in df:
			df = df.drop('row', 1)
		if 'tmp_row' in df:
			df = df.drop('tmp_row', 1)
		if 'field_errors' in df:
			df = df.drop('field_errors', 1)
		if 'toxicity_errors' in df:
			df = df.drop('toxicity_errors', 1)
		if 'custom_errors' in df:
			df = df.drop('custom_errors', 1)
		errorLog("Show columns to load: %s" % df.columns)
		df_column_names = str(','.join(df.columns))

		# placeholder SCCWRP is for agency variable in future
		staging_table_name = "staging_agency_" + table_name + "_" + TIMESTAMP
		errorLog("Staging table to load to: %s" % staging_table_name)

		# make copy of table to load and fix columns
		try:
			errorLog("Make copy of table: %s" % table_name)
     			src_engine = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
			src_engine._metadata = MetaData(bind=src_engine)
			src_engine._metadata.reflect(src_engine)
			src_table = Table(table_name, src_engine._metadata)
			errorLog(src_table)
			dest_engine = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
			dest_engine._metadata = MetaData(bind=dest_engine,schema='sde')
			dest_tbl = Table(staging_table_name, dest_engine._metadata)
			errorLog(dest_tbl)
			errorLog("End make copy of table...")

			# the fields below are required for all geodatabase feature access tables
			def getRandomTimeStamp(row):
				row['objectid'] = int(TIMESTAMP) + int(row.name)
				return row
			df = df.apply(getRandomTimeStamp, axis=1)
			errorLog(df['objectid'])
			# timestamp to date format - bug fix #4
			timestamp_date = datetime.datetime.fromtimestamp(int(TIMESTAMP)).strftime('%Y-%m-%d %H:%M:%S')
			df['created_user'] = "checker"
			df['created_date'] = timestamp_date
			df['last_edited_user'] = "checker"
			df['last_edited_date'] = timestamp_date

			# new fields from login - added 16apr18
			df['login_email'] = login
			df['login_agency'] = agency
			df['login_owner'] = owner
			df['login_year'] = year
			df['login_project'] = project

			# create columns in staging table based upon originating table
     			for column in src_table.columns:
				errorLog(column)
				dest_tbl.append_column(column.copy())
			# now create staging table
			dest_tbl.create()

			# load dataframe to staging table
			outcome = df.to_sql(name=staging_table_name,con=dest_engine,if_exists='append',index=False)
			errorLog(outcome)
			# use odo instead of pandas - should be faster
			#odo('myfile.*.csv', 'postgresql://hostname::tablename')
			#outcome = odo(df, destEngine)  # Migrate dataframe to Postgres
			#outcome = df.to_sql(name=staging_table_name,con=destEngine,flavor='mysql',if_exists='append',index=True,index_label="id")
			# close engine connections
       			src_engine.dispose()
       			dest_engine.dispose()
		except ValueError:
			message = "Critical Error: Failed to create a copy of table: %s and load to staging." % table_name
			errorLog(message)
			state = 1

		# need to do some checksums also on row count for each
		# dont attempt to load to production unless all dataframes load successfully
		try:
			errorLog("Loading staging to production for table: %s" % table_name)
			# dont run production unless staging succeeded
			if state == 0:
     				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
				# works but below is more precise - sql = 'INSERT INTO %s (SELECT * FROM %s)' % (table_name,staging_table_name)
				sql = 'INSERT INTO "' + table_name + '" (objectid, globalid, created_user, created_date, last_edited_user, last_edited_date, login_email, login_agency, login_owner, login_year, login_project, '+ df_column_names + ') select sde.next_rowid(%s,%s), sde.next_globalid(), created_user, created_date, last_edited_user, last_edited_date, login_email, login_agency, login_owner, login_year, login_project, ' + df_column_names + ' from "' + staging_table_name + '"'
				errorLog(sql)
				# "sde" and table_name below are used to populate next_rowid in sql statement
				status = eng.execute(sql,"sde",table_name)
				errorLog(status)
				if not status:
					errorLog("inside failed status")
					raise Error, eng.error
					errorLog(eng.error)
				status.close()
       				eng.dispose()
		except ValueError:
			errorLog("Failed to create production table: %s" % table_name)

		except ValueError:
			message = "Critical Error: Failed to create production table: %s" % table_name
			errorLog(message)
			state = 1
	# end for
	if state == 0:
        	# set submit in submission tracking table
 		eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
		sql_session = "update submission_tracking_table set submit = 'yes' where sessionkey = '%s'" % TIMESTAMP
		session_results = eng.execute(sql_session)
     		eng.dispose()
	else:
 		eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
		sql_session = "update submission_tracking_table set submit = 'no' where sessionkey = '%s'" % TIMESTAMP
		session_results = eng.execute(sql_session)
     		eng.dispose()

	### REPORT ###
	# pull in sample/field assignment table and use the records in it to toggle submissionstatus column in respective tables 
	# first check to find out which we need to handle sample or field table 
	try:
		# dont run the code below unless every above was successfull and assignment is set
		if assignment and state == 0:
			errorLog("Produce report for: %s" % assignment)
			# Pull information from assignment table and toggle in database
			assignment_file = '/var/www/smc/files/%s-assignment.csv' % TIMESTAMP
			assignment_table = pd.read_csv(assignment_file)
			errorLog(assignment_table)
			state = 0
			# dont run code below if field - removed check submission_type now
			#if 'samplingorganization' in assignment_table:
			#	state = 1
	except ValueError:
		# we were unable to find the assignment or load assignment table from csv
		message = "Critical Error: Failed to load assignment file."
		errorLog(message)
		state = 1

	# once we have the file loaded we can use the records to toggle
	try:
		# dont run the code below unless every above was successfull
		if assignment and state == 0 and submission_type != 'field':
			errorLog("set sample_assignment_table:")
			# put reporting module here and output to template file
			# call assignment table and toggle records submissionstatus = "complete"
			#print result.groupby(['stationid','lab','species']).size().to_frame(name = 'count').reset_index()
			#stationid                lab                    species  count
			#0       0000  City of San Diego     Eohaustorius estuarius     29
			#2   B18-8002  City of San Diego     Eohaustorius estuarius      1
			# may need to update last_edited_user and last_edited_date also
			eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
			for index, row in assignment_table.iterrows():
				#errorLog(row['stationid'],row['lab'],row['species'])
				sql = "update sample_assignment_table set submissionstatus = 'complete', submissiondate = '%s' where stationid = '%s' and lab = '%s' and parameter = '%s'" % (timestamp_date,row['stationid'],row['lab'],row['species'])
				errorLog(sql)
				status = eng.execute(sql)
				errorLog(status)
			eng.dispose()
			status = 0
		if assignment and state == 0 and submission_type == 'field':
			errorLog("set field_submission_table:")
			#assignment_table = occupation.groupby(['stationid','samplingorganization','collectiontype','stationfail','abandoned']).size().to_frame(name = 'count').reset_index()
			# B18-9202,Los Angeles County Sanitation Districts,Grab,None or No Failure,No,1
			# B18-9202,Los Angeles County Sanitation Districts,Trawl 10 Minutes,Other - another reason not listed why site was abandoned,Yes,1
			eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
			for index, row in assignment_table.iterrows():
				if row['collectiontype'] == 'Grab':
					sql = "update field_assignment_table set grabsubmit = 'complete', grabstationfail = '%s', grababandoned = '%s', grabsubmissiondate = '%s' where stationid = '%s' and grabagency = '%s'" % (row['stationfail'],row['abandoned'],timestamp_date,row['stationid'],row['samplingorganization'])
					errorLog(sql)
					status = eng.execute(sql)
					errorLog(status)
				if row['collectiontype'] == 'Trawl 10 Minutes' or row['collectiontype'] == 'Trawl 5 Minutes':
					sql = "update field_assignment_table set trawlsubmit = 'complete', trawlstationfail = '%s', trawlabandoned = '%s', trawlsubmissiondate = '%s' where stationid = '%s' and trawlagency = '%s'" % (row['stationfail'],row['abandoned'],timestamp_date,row['stationid'],row['samplingorganization'])
					errorLog(sql)
					status = eng.execute(sql)
					errorLog(status)
			eng.dispose()
			status = 0
	except ValueError:
		# we failed to connect or toggle records in assignment table
		message = "Critical Error: Failed to connect to database or update records in assignment table."
		errorLog(message)
		state = 1

	try:
		# dont run the code below unless all above was successfull
		if assignment and state == 0 and submission_type != 'field':
			submission_type_uc = submission_type.title()
			eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
			sql = "select stationid,lab,parameter,submissionstatus from sample_assignment_table where lab = '%s' order by submissionstatus desc" % agency
			sql_parameters = "select distinct parameter from sample_assignment_table where datatype = '%s'" % submission_type_uc
			errorLog(sql)
			errorLog(sql_parameters)
			report_results = eng.execute(sql)
			parameter_results = eng.execute(sql_parameters)
			errorLog(report_results)
			errorLog(parameter_results)
			eng.dispose()
			report_results_json = json.dumps([dict(r) for r in report_results])
			parameter_list = json.dumps([r for r, in parameter_results])
			state = 0
		if assignment and state == 0 and submission_type == 'field':
			eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
			sql = "select stationid,grabagency,trawlagency,grabsubmit,trawlsubmit from field_assignment_table where trawlagency = '%s' or grabagency = '%s' order by stationid asc" % (agency,agency)
			errorLog(sql)
			report_results = eng.execute(sql)
			errorLog(report_results)
			eng.dispose()
			report_results_json = json.dumps([dict(r) for r in report_results])
			parameter_list = []
			state = 0
	except ValueError:
		#  we failed to get existing records
		message = "Critical Error: Failed to connect to database and retrieve report records from assignment table."
		errorLog(message)
		state = 1

        # set submit in submission tracking table
        sql_session = "update submission_tracking_table set submit = 'yes' where sessionkey = '%s'" % TIMESTAMP
        session_results = eng.execute(sql_session)
	# user is finished so get rid of session key	
	session.pop('key', None)
	errorLog("END STAGING")
	# check delineate variable and email sccwrp
	if delineate == "no":
		mail_body = "The following user: %s with agency/lab: %s attempted to submit data for owner: %s, project: %s, sampled year: %s, but the csci portion of the checker failed to process un-delineated stations." % (login,agency,owner,project,year)
		errorLog(mail_body)
		status = internal_email("notify","checker@checker.sccwrp.org",["pauls@sccwrp.org"],message,mail_body)
		if status == 1:
			errorLog("failed to email sccwrp")
		else:
			errorLog("emailed sccwrp")
	if project != "SMC":
		mail_body = "The following user: %s with agency/lab: %s attempted to submit data for owner: %s, project: %s, sampled year: %s. The user selected 'Other' for project." % (login,agency,owner,project,year)
		errorLog(mail_body)
		status = internal_email("notify","checker@checker.sccwrp.org",["pauls@sccwrp.org"],message,mail_body)
		if status == 1:
			errorLog("failed to email sccwrp")
		else:
			errorLog("emailed sccwrp")
		
	if assignment and state == 0:
		errorLog(status)
		#return jsonify({'data': render_template('report.html', report=status)})
		#return render_template('report.html', agency=agency, submission_type=submission_type, report=report_results_json) 
		return jsonify(message=message,state=state, agency=agency, submission_type=submission_type, parameters=parameter_list, report=report_results_json)
	else:
		return jsonify(message=message,state=state,data=TIMESTAMP)
