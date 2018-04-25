import os, time, datetime
from flask import Blueprint, request, jsonify, session
import pandas as pd
import json
from sqlalchemy import create_engine, exc
from werkzeug import secure_filename
import xlsxwriter
from .ApplicationLog import *
from MatchFile import match
from CoreChecks import core
from TaxonomyChecks import taxonomy

file_upload = Blueprint('file_upload', __name__)

def allowedFile(filename):
	return '.' in filename and \
		filename.rsplit('.',1)[1] in ['xls','xlsx']

def exportToFile(all_dataframes,TIMESTAMP):
	errorLog("Starting exportToFile routine:")
	try:
		# export existing dataframe to file with tab names as destintation table name (if it has one) - load to database
		export_file = '/var/www/smc/files/%s-export.xlsx' % TIMESTAMP
		export_writer = pd.ExcelWriter(export_file, engine='xlsxwriter')

		# export existing dataframe to file with tab names as destintation table name (if it has one) - return to user
		excel_file = '/var/www/smc/logs/%s-format.xlsx' % TIMESTAMP
		excel_link = 'http://checker.sccwrp.org/smc/logs/%s-format.xlsx' % TIMESTAMP
		excel_writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')
		# creates workbook that includes specified formats
		workbook = excel_writer.book

		for dataframe in all_dataframes.keys():
			errorLog("dataframe:")
			errorLog(dataframe)	
			df_sheet_and_table_name = dataframe.strip().split(" - ")
			errorLog("df_sheet_and_table_name: %s" % df_sheet_and_table_name)
			table_name = str(df_sheet_and_table_name[2])
			errorLog("table_name: %s" % table_name)
			# only export tab name as destination table name if there was a match - otherwise leave as is
			if table_name:
				# drop tmp_row, row, and index for export to excel 
				errorLog("drop tmp_row, row, and index for export to excel:")
				if 'row' in all_dataframes[dataframe]:
					all_dataframes[dataframe].drop(['row'], axis = 1, inplace = True)
				if 'tmp_row' in all_dataframes[dataframe]:
					all_dataframes[dataframe].drop(['tmp_row'], axis = 1, inplace = True)

				# write dataframe to excel worksheet
				errorLog("write dataframe to export writer:")
				all_dataframes[dataframe].to_excel(export_writer, sheet_name=table_name)

				errorLog("write dataframe to save to excel writer:")
				all_dataframes[dataframe].to_excel(excel_writer, sheet_name=table_name, index = False)

				# if there are errors then lets mark them in the returned excel file
				#if 'custom_errors' in all_dataframes[dataframe]:
				errorLog("list all_dataframes columns")
				errorLog(list(all_dataframes[dataframe]))
				if ('errors' in all_dataframes[dataframe]) or ('custom_errors' in all_dataframes[dataframe]):
					# create worksheet object
					worksheet = excel_writer.sheets[table_name]
					# if there are errors or custom_errors process them - there will only be one type (errors or custom_errors)
					if 'errors' in all_dataframes[dataframe]:
                				dfc = all_dataframes[dataframe].loc[~all_dataframes[dataframe]['errors'].isnull()]['errors']
						output_column_name = 'errors'
					if 'custom_errors' in all_dataframes[dataframe]:
                				dfc = all_dataframes[dataframe].loc[~all_dataframes[dataframe]['custom_errors'].isnull()]['custom_errors']
						output_column_name = 'custom_errors'
                			# there can be multiple errors in a row lets make them tuples of dicts
					dfc = dfc.apply(lambda x: eval(x))
					# create color formatting
				        format_red = workbook.add_format({'bg_color': '#FFC7CE','border': 1,'border_color': '#800000','bold': True})
				        format_yellow = workbook.add_format({'bg_color': '#FFFF00','border': 1,'border_color': '#800000','bold': True})
					# loop through error dict
                       			for i in dfc.index:
						errorLog("elements of dfc dict:")
						errorLog(i)
                       				if type(dfc[i])==dict:
							errorLog("dict element:")
							errorLog(dfc[i])
                       					col_name = dfc[i]['column'].lower()
							errorLog("col_name:")
							errorLog(col_name)
							# if column name is empty use errors
							if not col_name:
								#col_name = 'errors'
								col_name = output_column_name
							# if column name has forward slash use errors
							if '/' in col_name:
								#col_name = 'errors'
								col_name = output_column_name
                       					col_index = all_dataframes[dataframe].columns.get_loc(col_name) 
							errorLog("col_index:")
							errorLog(col_index)
							if dfc[i]['error_type'] == 'Custom Warning':
                       						worksheet.conditional_format(i+1,col_index,i+1,col_index,{'type':'no_errors','format':format_yellow})
								worksheet.write_comment(i+1,col_index,dfc[i]['error'])
							else:
                       						worksheet.conditional_format(i+1,col_index,i+1,col_index,{'type':'no_errors','format':format_red})
								worksheet.write_comment(i+1,col_index,dfc[i]['error'])
                       				elif type(dfc[i])==tuple:
							errorLog("tuple element:")
							errorLog(dfc[i])
                       					for j in range(len(dfc[i])):
								#errorLog("j element: %s" % j)
                       						col_name = dfc[i][j]['column'].lower()
								errorLog("col_name:")
								errorLog(col_name)
								# if column name is empty use errors
								if not col_name:
									#col_name = 'errors'
									col_name = output_column_name
								# if column name has forward slash use errors
								if '/' in col_name:
									#col_name = 'errors'
									col_name = output_column_name
                       						col_index = all_dataframes[dataframe].columns.get_loc(col_name)
								errorLog("col_index:")
								errorLog(col_index)
								if dfc[i][j]['error_type'] == 'Custom Warning':
                       							worksheet.conditional_format(i+1,col_index,i+1,col_index,{'type':'no_errors','format':format_yellow})
									worksheet.write_comment(i+1,col_index,dfc[i][j]['error'])
								else:
                       							worksheet.conditional_format(i+1,col_index,i+1,col_index,{'type':'no_errors','format':format_red})
									worksheet.write_comment(i+1,col_index,dfc[i][j]['error'])
						else:
							errorLog("something doesnt match:")
							errorLog(dfc[i])
				else:
					# bug ending up with index numbers as tab sheets - effects upload
					#original_sheet_name = str(df_sheet_and_table_name[0])
					#all_dataframes[dataframe].to_excel(export_writer, sheet_name=original_sheet_name)
					all_dataframes[dataframe].to_excel(export_writer, sheet_name=table_name)
		export_writer.save()
		excel_writer.save()
		state = 0
	except ValueError:
		state = 1
		message = "Critical Error: Failed to export submission to excel file."	
		errorLog(message)
	return state, excel_link

@file_upload.route("/upload", methods=["POST"])
def upload():
	errorLog("Function - upload")
	statusLog("Function - upload")
	errors_dict = {'total': 0, 'mia': 0, 'lookup': 0, 'duplicate': 0, 'custom': 0, 'match': 0}
	# probably need to do something dramatic here if login and agency arent submitted - javascript requires them though
	errorLog("get login:")
	login = request.form['login']
	errorLog(login)
	agency = request.form['agency']
	errorLog(agency)
	owner = request.form['owner']
	errorLog(owner)
	year = request.form['year']
	errorLog(year)
	project = request.form['project']
	errorLog(project)
	if project != "Other":
		# rafi wants project_code left empty if user selects Other - jeff will add the project_code manually
		project_code = "%s_%s_%s" % (owner,project,year)
	else:
		project_code = ""
	login_info = "%s-%s-%s-%s-%s" % (login,agency,owner,year,project)
	errorLog(project_code)
	TIMESTAMP=str(session.get('key'))
	timestamp_date = datetime.datetime.fromtimestamp(int(TIMESTAMP)).strftime('%Y-%m-%d %H:%M:%S')
	errorLog(TIMESTAMP)

        # we need to insert a record for login, agency, sessionkey, and timestamp into submission tracking table
        # connect to database
        # insert record into table
        eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
        sql_session = "insert into submission_tracking_table (objectid, login, agency, sessionkey, time_stamp, created_user, created_date, last_edited_user, last_edited_date) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (TIMESTAMP,login,agency,TIMESTAMP,TIMESTAMP,"checker",timestamp_date,"checker",timestamp_date)
        session_results = eng.execute(sql_session)
        eng.dispose()

	# all values in errors_dict need to be set to 0 at start of application
	for k,v in errors_dict.iteritems():
		errors_dict[k] = 0
	
	try:
		errorLog(request.path)
		#uploaded_file = request.files['file[]']
		uploaded_files = request.files.getlist('files[]')
		for file in uploaded_files:
			if file and allowedFile(file.filename):
				filename = secure_filename(file.filename)
				# we want to save both the file as it was submitted and a timestamp copy
				# the reason for the timestamp copy is so we dont have duplicate submissions
				# get extension
				extension = filename.rsplit('.',1)[1]
				# join extension with timestamp
				originalfilename = filename
				newfilename = TIMESTAMP + "." + extension
				# return timestamp filename
				gettime = int(time.time())
				tfilename = datetime.datetime.fromtimestamp(gettime)
				humanfilename = tfilename.strftime('%Y-%m-%d') + "." + extension	
				modifiedfilename = "original filename: " + filename + " - new filename: " + newfilename + "(" + humanfilename + ")"
				try:
					# save timestamp file first
					file.save(os.path.join('/var/www/smc/files/', newfilename))
					# then original file
					# return timestamp file to web application
					filenames = modifiedfilename
					# return this to internal application
					infile = "/var/www/smc/files/" + newfilename
					message = newfilename
        				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
					sql_session = "update submission_tracking_table set upload = 'yes' where sessionkey = '%s'" % TIMESTAMP
        				session_results = eng.execute(sql_session)
        				eng.dispose()
					state = 0

					all_dataframes, sql_match_tables, match_tables = match(infile,errors_dict)

        				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
					sql_session = "update submission_tracking_table set match = 'yes' where sessionkey = '%s'" % TIMESTAMP
        				session_results = eng.execute(sql_session)
        				eng.dispose()

					if match_tables:
						data_checks, data_checks_redundant, errors_dict = core(all_dataframes,sql_match_tables, errors_dict)
						errorLog(data_checks)
						# move above - after match
						errorLog("sql_match_tables:")
						errorLog(sql_match_tables)
						# dictionary list of required tables by data type
						required_tables_dict = {'chemistry': ['tbl_chembatch','tbl_chemresults'],'toxicity': ['tbl_toxbatch','tbl_toxresults','tbl_toxwq'],'field': ['tbl_stationoccupation','tbl_trawlevent'],'field': ['tbl_stationoccupation','tbl_grabevent'],'field': ['tbl_stationoccupation','tbl_trawlevent','tbl_grabevent'], 'fish': ['tbltrawlfishabundance','tblfishbiomass'],'ocpw': ['tbl_ocpwlab'],'taxonomy': ['tbl_taxonomysampleinfo','tbl_taxonomyresults']}
						match_dataset = "" 	# start empty
						for k,v in required_tables_dict.items():
							if set(sql_match_tables) == set(v):
								message = "Custom: Found exact match: %s, %s" % (k,v)
								match_dataset = k
						errorLog("match_dataset: ")
						errorLog(match_dataset)

						# dont run custom checks if there are core errors - some code in custom checks depends on clean data
						total_count = errors_dict['total']
						errorLog("total error count: %s" % total_count)

						if match_dataset == "toxicity" and total_count == 0:
							assignment_table, custom_checks, custom_redundant_checks, summary_checks, summary_results_link = toxicity(all_dataframes,sql_match_tables,errors_dict)
        						eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict
							# export field or sample assignment dataframe - need to import in StagingUpload.py
							errorLog(assignment_table)

							assignment_file = '/var/www/smc/files/%s-assignment.csv' % TIMESTAMP
							assignment_table.to_csv(assignment_file, sep=',', encoding='utf-8', index=False)

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)

							# used for reporting - either field or sample
							assignment_table = "sample"

							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,summary=summary_checks,summary_file=summary_results_link,errors=errors_count,excel=excel_link,assignment=assignment_table,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset)

						elif match_dataset == "chemistry" and total_count == 0:
							assignment_table, custom_checks, custom_redundant_checks = chemistry(all_dataframes,sql_match_tables,errors_dict)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict
							# export field or sample assignment dataframe - need to import in StagingUpload.py
							errorLog(assignment_table)
							assignment_file = '/var/www/smc/files/%s-assignment.csv' % TIMESTAMP
							assignment_table.to_csv(assignment_file, sep=',', encoding='utf-8', index=False)

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							# used for reporting - either field or sample
							assignment_table = "sample"
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,assignment=assignment_table,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset)
						elif match_dataset == "taxonomy" and total_count == 0:
							assignment_table, custom_checks, custom_redundant_checks, summary_checks, summary_results_link, message = taxonomy(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)
        						eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict
							# export field or sample assignment dataframe - need to import in StagingUpload.py
							#errorLog(assignment_table)
							#assignment_file = '/var/www/smc/files/%s-assignment.csv' % TIMESTAMP
							#assignment_table.to_csv(assignment_file, sep=',', encoding='utf-8', index=False)
							#assignment_table = ""

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							# used for reporting - either field or sample
							assignment_table = ""
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,summary=summary_checks,summary_file=summary_results_link,errors=errors_count,excel=excel_link,assignment=assignment_table,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset)
						elif match_dataset == "field" and total_count == 0:
							assignment_table, custom_checks, custom_redundant_checks = field(all_dataframes,sql_match_tables,errors_dict)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict
							# export field or sample assignment dataframe - need to import in StagingUpload.py
							errorLog(assignment_table)
							assignment_file = '/var/www/smc/files/%s-assignment.csv' % TIMESTAMP
							assignment_table.to_csv(assignment_file, sep=',', encoding='utf-8', index=False)

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							# used for reporting - either field or sample
							assignment_table = "field"
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,assignment=assignment_table,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset)
						else:
							# we may want to create a submission option here find a way to export dataframe to file - with matching table names as tab names
							errorLog("submitted data didnt match a set of data (like toxicity, chemistry, etc...)")
							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
							# disable for testing
							#status = ""
							#excel_link = ""

							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename)
					else:
						# unable to match tables
						errorLog("match_tables: %s" % match_tables)
						errorLog("sql_match_tables: %s" % sql_match_tables)
						errors_count = json.dumps(errors_dict)
						return jsonify(message=message,state=state,table_match=match_tables,errors=errors_count)
						

				except IOError:
					message = "Critical Error: Failed to save file to upload directory!"
					state = 1
		errorLog(message)
	except ValueError:
		state = 1
		message = "Critical Error: Failed to upload file."	
		errorLog(message)
		jsonify(message=message,state=state)
	#return jsonify(message=message,state=state,timestamp=TIMESTAMP,original_file=originalfilename,modified_file=newfilename)
	#errors_count = json.dumps(errors_dict)
	#return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,summary=summary_checks,summary_file=summary_results_link,errors=errors_count)
