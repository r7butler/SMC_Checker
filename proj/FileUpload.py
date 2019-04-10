import os, time, datetime
from flask import Blueprint, request, jsonify, session
import pandas as pd
import json
from sqlalchemy import create_engine, exc
from werkzeug import secure_filename
import xlsxwriter
import folium
import pandas_access as mdb
from datetime import datetime, timedelta
from .ApplicationLog import *
from MatchFile import match
from CoreChecks import core
from AlgaeChecks import algae
from ChemistryChecks import chemistry
from ChannelEngineeringChecks import channelengineering
from HydromodChecks import hydromod
from SiteEvaluationChecks import siteevaluation
from TaxonomyChecks import taxonomy
from ToxicityChecks import toxicity
from ChannelEngineeringChecks import channelengineering
from HydromodChecks import hydromod
from SiteEvaluationChecks import siteevaluation
from ChemistryChecks import chemistry
# PHABMetrics R package must be installed.
#from PHABChecks import phab
#from PYHABMetrics import python_phabmetrics
from TimeSeriesChecks import timeseries
#from lib.upload_file import uploadfile # added by Jordan 2/28/2019

file_upload = Blueprint('file_upload', __name__)

def allowedFile(filename):
	return '.' in filename and \
		filename.rsplit('.',1)[1] in ['xls','xlsx','mdb']

def exportToFile(all_dataframes,TIMESTAMP):
	errorLog("Starting exportToFile routine:")
	try:
		# export existing dataframe to file with tab names as destintation table name (if it has one) - load to database
		export_file = '/var/www/smc/files/%s-export.xlsx' % TIMESTAMP
                export_writer = pd.ExcelWriter(export_file, engine='xlsxwriter',options={'strings_to_formulas': False})

		# export existing dataframe to file with tab names as destintation table name (if it has one) - return to user
		excel_file = '/var/www/smc/logs/%s-format.xlsx' % TIMESTAMP
                excel_link = 'http://smcchecker.sccwrp.org/smc/logs/%s-format.xlsx' % TIMESTAMP
                excel_writer = pd.ExcelWriter(excel_file, engine='xlsxwriter',options={'strings_to_formulas': False})
		# creates workbook that includes specified formats
		workbook = excel_writer.book

		for dataframe in all_dataframes.keys():
			df_sheet_and_table_name = dataframe.strip().split(" - ")
			errorLog("df_sheet_and_table_name: %s" % df_sheet_and_table_name)
			table_name = str(df_sheet_and_table_name[2])
			errorLog("table_name: %s" % table_name)
			# only export tab name as destination table name if there was a match - otherwise leave as is
			if table_name:
				# drop necessary fields for export to excel 
				errorLog("drop tmp_row, row, index, and calculated fields for export to excel:")
				
				if 'analyteclass' in all_dataframes[dataframe]:
					all_dataframes[dataframe].drop(['analyteclass'], axis = 1, inplace = True)
				if 'percentrecovery' in all_dataframes[dataframe]:
					all_dataframes[dataframe].drop(['percentrecovery'], axis = 1, inplace = True)
				if 'row' in all_dataframes[dataframe]:
					all_dataframes[dataframe].drop(['row'], axis = 1, inplace = True)
				if 'tmp_row' in all_dataframes[dataframe]:
					all_dataframes[dataframe].drop(['tmp_row'], axis = 1, inplace = True)
				
				# write dataframe to excel worksheet
				errorLog("write dataframe to export writer:")
				errorLog(all_dataframes[dataframe])
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
							if dfc[i]['error_type'] == 'Undefined Warning':
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
								if dfc[i][j]['error_type'] == 'Undefined Warning':
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

def createMap(list_of_stations,timestamp):
	errorLog("map function")
	errorLog(timestamp)
	errorLog(list_of_stations)
	#map1 = folium.Map(location=[45.5, -73.61], width="100%", height="100%")
	#map1.save('/var/www/smc/logs/map.html')
	eng = create_engine('postgresql://smcread:1969$Harbor@192.168.1.17:5432/smc')
	sql = "select stationid,latitude,longitude from lu_stations where stationid in (%s)" % ''.join(list_of_stations)
	errorLog(sql)
	#sql_results = pd.read_sql_query(sql,eng)
	sql_results = eng.execute(sql)
	rows = sql_results.cursor.fetchall()
	eng.dispose()
	errorLog(sql_results)
	m = folium.Map(
	    location=[33.0000, -118.0000],
	    zoom_start=7,
	    tiles='Stamen Terrain'
	)
	tip = 'test'
	for r in rows:
		tooltip = r[0]
		folium.Marker(
		    location=[r[1], r[2]],
		    popup='%s' % r[0]
		).add_to(m)
	map_file = '/var/www/smc/logs/%s-map.html' % timestamp
	errorLog(map_file)
	map_url = 'http://smcchecker.sccwrp.org/smc/logs/%s-map.html' % timestamp
	errorLog(map_url)

	m.save(map_file)
	return map_url

@file_upload.route("/upload", methods=["POST"])
def upload():
	errorLog("Function - upload")
	statusLog("Function - upload")
	errors_dict = {'total': 0, 'mia': 0, 'lookup': 0, 'duplicate': 0, 'custom': 0, 'match': 0, 'warnings': 0}
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
	message = ""

        # we need to insert a record for login, agency, sessionkey, and timestamp into submission tracking table
        # connect to database
        # insert record into table
        eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
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
				mime_type = file.content_type
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
                                errorLog(modifiedfilename)
				errorLog("================================= about to try to upload")
                                try:
					# save timestamp file first
                                        statusLog("Uploading file")
                                        uploaded_file_path = os.path.join('/var/www/smc/files/', newfilename)
					file.save(uploaded_file_path)
					# get file size after saving (Added by Jordan 2/28/2019)
                                        file_size = os.path.getsize(uploaded_file_path)
                                        errorLog(file_size)
                                        # then original file
					# return timestamp file to web application
					filenames = modifiedfilename
					# return this to internal application
					infile = "/var/www/smc/files/" + newfilename
					
                                        message = newfilename
        				eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
					sql_session = "update submission_tracking_table set upload = 'yes' where sessionkey = '%s'" % TIMESTAMP
        				session_results = eng.execute(sql_session)
        				eng.dispose()
					state = 0

                                        # check if user submits an mdb file instead of excel
                                        errorLog("check if user submits an mdb file instead of excel")
                                        if extension == "mdb":
                                            all_dataframes, sql_match_tables, match_tables, pmetrics_long, stations_for_IPI, unique_stations = python_phabmetrics(infile, login_info)
                                            

                                            
                                        else:
					    all_dataframes, sql_match_tables, match_tables = match(infile,errors_dict)
					    eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
					    sql_session = "update submission_tracking_table set match = 'yes' where sessionkey = '%s'" % TIMESTAMP
        				    session_results = eng.execute(sql_session)
        				    eng.dispose()

					if match_tables:
						errorLog("entering match_tables")
						errorLog(match_tables)
                                                # get split key to get matching table
                				for dataframe in all_dataframes.keys():
                        				errorLog("dataframe:")
                        				errorLog(dataframe)
						for m in match_tables:
							# name of dataframe
							errorLog("name of dataframe:")
							errorLog(m[0])
                                                	split_match_fields = m[0].split('-')
                                                        # you wont be able to split unless the table is matched
                                                        # length should be 7 fields
                                                        errorLog("length")
                                                        errorLog(len(split_match_fields))
                                                        if len(split_match_fields) >= 4:
                                                            # field three is matched table
                                                            match_tables_index = split_match_fields[0]
                                                            match_tables_sheet = split_match_fields[1]
                                                            match_tables_name = split_match_fields[3]
                                                            match_tables_key = match_tables_index + ' - ' + match_tables_sheet + ' - ' + match_tables_name
                                                            errorLog(match_tables_key)

                                                            # code specific to taxonomy sampleinfo and results - fill empties with -88 and 1/1/1980 added 25apr18
                                                            if match_tables_name == 'tbl_taxonomysampleinfo':
                                                                    # for future - call database and get integer, decimal, and date fields that are required
                                                                    # replace empty integer and decimal and date values with nulls
                                                                    errorLog(all_dataframes[match_tables_key]['replicatename'])
                                                                    all_dataframes[match_tables_key].loc[:, ['replicatename','numberjars','percentsamplecounted','targetorganismcount','actualorganismcount','extraorganismcount','qcorganismcount','discardedorganismcount']] = all_dataframes[match_tables_key].loc[:, ['replicatename','numberjars','percentsamplecounted','targetorganismcount','actualorganismcount','extraorganismcount','qcorganismcount','discardedorganismcount']].fillna(-88)
                                                                    for column_name in ['replicatename','numberjars','targetorganismcount','actualorganismcount','extraorganismcount','qcorganismcount','discardedorganismcount']:
                                                                            all_dataframes[match_tables_key][column_name] = all_dataframes[match_tables_key][column_name].astype('int')
                                                                    set_date = datetime.datetime.strptime("01/01/1950","%m/%d/%Y").strftime('%m/%d/%Y')
                                                                    all_dataframes[match_tables_key]['replicatecollectiondate'] = all_dataframes[match_tables_key]['replicatecollectiondate'].fillna(set_date)
                                                                    # replicatecollectiondate to datetime field
                                                                    all_dataframes[match_tables_key]['replicatecollectiondate'] = pd.to_datetime(all_dataframes[match_tables_key]['replicatecollectiondate'])

                                                            if match_tables_name == 'tbl_taxonomyresults':
                                                                    set_date = datetime.datetime.strptime("01/01/1950","%m/%d/%Y").strftime('%m/%d/%Y')
                                                                    all_dataframes[match_tables_key]['enterdate'] = all_dataframes[match_tables_key]['enterdate'].fillna(set_date)
                                                                    # enterdate field to datetime field
                                                                    all_dataframes[match_tables_key]['enterdate'] = pd.to_datetime(all_dataframes[match_tables_key]['enterdate'])
                                                                    all_dataframes[match_tables_key]['resqualcode'] = all_dataframes[match_tables_key]['resqualcode'].apply(lambda x: "'=" if x == "=" else x)
                                                                    all_dataframes[match_tables_key]['resqualcode'] = all_dataframes[match_tables_key]['resqualcode'].astype('str')
                                                            # Bug fix: Force datatype of Result field to string for algae data. -Jordan 2/19/2019
                                                            if match_tables_name == 'tbl_algae':
                                                                    # Critical: If actualorganismcount, baresult, and result are empty fill them with -88. This should be done before core checks.
                                                                    all_dataframes[match_tables_key]['actualorganismcount'].fillna(-88,inplace=True)
                                                                    all_dataframes[match_tables_key]['baresult'].fillna(-88,inplace=True)
                                                                    all_dataframes[match_tables_key]['result'].fillna(-88,inplace=True)
                                                                    all_dataframes[match_tables_key]['result'] = all_dataframes[match_tables_key]['result'].astype(str)

                                                            if match_tables_name == 'tbl_phab':
                                                                    all_dataframes, sql_match_tables, match_tables, pmetrics_long, stations_for_IPI, unique_stations = python_phabmetrics(infile, login_info, excel = True)
						
                                                            if match_tables_name == 'tbl_channelengineering':
                                                                all_dataframes[match_tables_key]['determination'] = all_dataframes[match_tables_key].determination.apply(lambda x: 'Site visit' if str(x).lower() == 'site visit' else x)
                                                                    
                                                
                                                
                                                data_checks, data_checks_redundant, errors_dict = core(all_dataframes,sql_match_tables, errors_dict)
                                                errorLog('data_checks:')
						errorLog(data_checks)

						# move above - after match
						errorLog("sql_match_tables:")
						errorLog(sql_match_tables)
						# dictionary list of required tables by data type - made into lists instead of strings 15mar18 - supports variations
                                                required_tables_dict = {'chemistry': [['tbl_chemistrybatch','tbl_chemistryresults'],['tbl_chemresults','tbl_chembatch']],'toxicity': [['tbl_toxbatch','tbl_toxresults','tbl_toxwq'],['tbl_toxresults','tbl_toxbatch','tbl_toxwq'],['tbl_toxwq','tbl_toxresults','tbl_toxbatch'],['tbl_toxwq','tbl_toxbatch','tbl_toxresults'],['tbl_toxbatch','tbl_toxwq','tbl_toxresults']], 'field': [['tbl_stationoccupation','tbl_trawlevent'],['tbl_trawlevent','tbl_stationoccupation'],['tbl_stationoccupation','tbl_grabevent'],['tbl_grabevent','tbl_stationoccupation'],['tbl_stationoccupation','tbl_trawlevent','tbl_grabevent'],['tbl_trawlevent','tbl_grabevent','tbl_stationoccupation']], 'taxonomy': [['tbl_taxonomysampleinfo','tbl_taxonomyresults'],['tbl_taxonomyresults','tbl_taxonomysampleinfo']],'invert': [['tbl_trawlinvertebrateabundance','tbl_trawlinvertebratebiomass'],['tbl_trawlinvertebratebiomass','tbl_trawlinvertebrateabundance']],'ocpw': [['tbl_ocpwlab']],'debris': [['tbl_trawldebris']],'ptsensor': [['tbl_ptsensorresults']],'infauna': [['tbl_infaunalabundance_initial'],['tbl_infaunalabundance_qareanalysis']],'channelengineering': [['tbl_channelengineering']], 'hydromod': [['tbl_hydromod']], 'siteevaluation': [['tbl_siteeval', 'tbl_siteeval']], 'phab': [['tbl_phab']]}
						match_dataset = "" 	# start empty
						errorLog("required_tables_dict:")
						errorLog(required_tables_dict)
                                                errorLog("sql_match_tables:")
                                                errorLog(sql_match_tables)
						for k,v in required_tables_dict.items():
                                                        errorLog("locate item in required_tables_dict:")
                                                        errorLog(v)
							if sql_match_tables in v:
								message = "Custom: Found exact match: %s, %s" % (k,v[v.index(sql_match_tables)])
								errorLog(message)
								match_dataset = k
						errorLog("match_dataset: ")
						errorLog(match_dataset)

						# dont run custom checks if there are core errors - some code in custom checks depends on clean data
						total_count = errors_dict['total']
						errorLog("total error count: %s" % total_count)

						if match_dataset == "taxonomy" and total_count == 0:
							assignment_table, custom_checks, custom_redundant_checks, summary_checks, summary_results_link, message, unique_stations = taxonomy(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)
							errorLog("list of unique_stations:")
							errorLog(unique_stations)
							map_url = createMap(unique_stations,TIMESTAMP)
							errorLog("map_url")
							errorLog(map_url)
        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							# used for reporting - either field or sample
							assignment_table = ""
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,summary=summary_checks,summary_file=summary_results_link,errors=errors_count,excel=excel_link,assignment=assignment_table,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset,map=map_url)

                                                elif match_dataset == "phab" and total_count == 0:
							all_dataframes, ipi_output, assignment_table, custom_checks, custom_redundant_checks, summary_checks, summary_results_link, message = phab(all_dataframes,pmetrics_long,stations_for_IPI,sql_match_tables,errors_dict,project_code,login_info)
							errorLog("list of unique_stations:\n")
							errorLog(unique_stations)
                                                        
                                                        errorLog("calling the createMap function")
							map_url = createMap(unique_stations,TIMESTAMP)
							errorLog("map_url\n")
							errorLog(map_url)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
                                                        eng.dispose()
                                                        
                                                        errorLog("creating json from the errors dictionary\n")
							errors_count = json.dumps(errors_dict)	# dump error count dict
                                                        errorLog(errors_count)

                                                        '''
                                                        # Add PHABMetrics and IPI scores to the all_dataframes variable
                                                        all_dataframes['1 - tblPHabMetrics - tbl_phabmetrics'] = phabmetrics_output 
                                                        
                                                        # We aren't concerned with IPI just yet. It's almost ready though
                                                        all_dataframes['2 - tblIPIScores - tbl_ipi_scores'] = IPI_output
                                                        '''

							# create excel files
                                                        errorLog("calling exportToFile function")
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
                                                        errorLog("exportToFile function completed successfully")
                                                        errorLog("Here is the status variable returned from the exportToFile function:\n")
                                                        errorLog(status)
                                                        errorLog("\nHere is the excel_link returned from exportToFile function:\n")
                                                        errorLog(excel_link)
                                                        errorLog("\n")
                                                        

                                                        errorLog("writing ipi dataframe to csv")
                                                        ipipath = '/var/www/smc/logs/%s.ipi.csv' % TIMESTAMP
                                                        ipi_output.to_csv(ipipath, sep=',', mode='a', encoding='utf-8', index=False)
                                                        
                                                        
                                                        # This next line is to make the IPI tab show up on the UI
                                                        match_tables[0] = match_tables[0] + "-ipi"

							# used for reporting - either field or sample
							assignment_table = ""

                                                        errorLog("Total running time: " + str((float(time.time()) - float(TIMESTAMP)) / 60) + " minutes.")

							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,summary=summary_checks,summary_file=summary_results_link,errors=errors_count,excel=excel_link,assignment=assignment_table,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset,map=map_url)

						elif match_dataset == "algae" and total_count == 0:
							custom_checks, custom_redundant_checks, message, unique_stations = algae(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)

							errorLog("list of unique_stations:")
							errorLog(unique_stations)
							map_url = createMap(unique_stations,TIMESTAMP)
							errorLog("map_url")
                                                        errorLog(map_url)

                                                        eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
                                                        sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset, TIMESTAMP)
                                                        session_results = eng.execute(sql_session)
                                                        eng.dispose()

                                                        errors_count = json.dumps(errors_dict) # dump error count dict

                                                        # create excel files
                                                        status, excel_link = exportToFile(all_dataframes, TIMESTAMP)
                                                        
                                                        return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset,map=map_url)
                                                        

						elif match_dataset == "fish" and total_count == 0:
							custom_checks, custom_redundant_checks = fish(all_dataframes,sql_match_tables,errors_dict)

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset,map=map_url)
						elif match_dataset == "chemistry" and total_count == 0:
							custom_checks, custom_redundant_checks, message, unique_stations = chemistry(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)

							errorLog("list of unique_stations:")
							errorLog(unique_stations)
							map_url = createMap(unique_stations,TIMESTAMP)
							errorLog("map_url")
							errorLog(map_url)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset,map=map_url)
						elif match_dataset == "channelengineering" and total_count == 0:
							custom_checks, custom_redundant_checks, message, unique_stations = channelengineering(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)
							errorLog("list of unique_stations:")
							errorLog(unique_stations)
							map_url = createMap(unique_stations,TIMESTAMP)
							errorLog("map_url")
							errorLog(map_url)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset,map=map_url)
						elif match_dataset == "hydromod" and total_count == 0:
							custom_checks, custom_redundant_checks, message, unique_stations = hydromod(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)

							errorLog("list of unique_stations:")
							errorLog(unique_stations)
							map_url = createMap(unique_stations,TIMESTAMP)
							errorLog("map_url")
							errorLog(map_url)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset,map=map_url)
						elif match_dataset == "siteevaluation" and total_count == 0:
							custom_checks, custom_redundant_checks, message, unique_stations = siteevaluation(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)

							errorLog("list of unique_stations:")
							errorLog(unique_stations)
							map_url = createMap(unique_stations,TIMESTAMP)
							errorLog("map_url")
							errorLog(map_url)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset,map=map_url)
                                                elif match_dataset == "toxicity" and total_count == 0:
							custom_checks, custom_redundant_checks, message, unique_stations = toxicity(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)

							errorLog("list of unique_stations:")
							errorLog(unique_stations)
							map_url = createMap(unique_stations,TIMESTAMP)
							errorLog("map_url")
							errorLog(map_url)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset,map=map_url)
						elif match_dataset == "timeseries" and total_count == 0:
							custom_checks, custom_redundant_checks, message = timeseries(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset)
						
                                                
						elif match_dataset == "channelengineering" and total_count == 0:
							custom_checks, custom_redundant_checks = channelengineering(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset)
						
                                                elif match_dataset == "hydromod" and total_count == 0:
							custom_checks, custom_redundant_checks = hydromod(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset)
                                                    
                                                elif match_dataset == "siteevaluation" and total_count == 0:
							custom_checks, custom_redundant_checks = siteevaluation(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset)
                                                elif match_dataset == "chemistry" and total_count == 0:
							custom_checks, custom_redundant_checks = chemistry(all_dataframes,sql_match_tables,errors_dict,project_code,login_info)

        						eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
							sql_session = "update submission_tracking_table set extended_checks = 'yes', extended_checks_type = '%s' where sessionkey = '%s'" % (match_dataset,TIMESTAMP)
        						session_results = eng.execute(sql_session)
        						eng.dispose()

							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
                                                        errorLog("exportToFile")
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)
	                                                errorLog(" after export ")
							return jsonify(message=message,state=state,table_match=match_tables, business=data_checks,redundant=data_checks_redundant,custom=custom_checks,redundant_custom=custom_redundant_checks,errors=errors_count,excel=excel_link,original_file=originalfilename,modified_file=newfilename,datatype=match_dataset)
                                                else:
							# we may want to create a submission option here find a way to export dataframe to file - with matching table names as tab names
							errorLog("submitted data didnt match a set of data (like toxicity, chemistry, etc...)")
							# check to see if one of the sheets submitted match one of the values in required_tables_dict
							# if that is the case then the user needs to submit the required number of sheets
							for key, value in required_tables_dict.items():
								for v in value:
									overlap = set(sql_match_tables) & set(v)
									if overlap:
                                                                                # user submitted correct data but there are errors
                                                                                errorLog(overlap)
										#message = "missing required tables"
                                                                        else:
                                                                                errorLog(v)
							errors_count = json.dumps(errors_dict)	# dump error count dict

							# create excel files
							status, excel_link = exportToFile(all_dataframes,TIMESTAMP)

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







# ---------------------------------------- #

# CODE ARCHIVE

# this used to be right below where it says "if extension == 'mdb'"
'''        
### THIS MAY NEED TO BECOME A SEPARATE ROUTE
errorLog("LOAD DATA TO DATABASE")
sccwrp_engine = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')

#### LOAD FIELD
# get last objectid
phab_sql = "SELECT MAX(objectid) from tbl_phab;"
try:
        last_phab_objid = sccwrp_engine.execute(phab_sql).fetchall()[0][0]
        field_query['objectid'] = field_query.index + last_phab_objid + 1
except:
        field_query['objectid'] = field_query.index + 1
        # submit to phab
        #status = field_query.to_sql('tbl_phab', sccwrp_engine, if_exists='append', index=False)

        #### LOAD HABITAT
        # get last objectid
        phab_sql = "SELECT MAX(objectid) from tbl_phab;"
        last_phab_objid = sccwrp_engine.execute(phab_sql).fetchall()[0][0]
        habitat_query['objectid'] = habitat_query.index + last_phab_objid + 1
        # submit to phab
        #status = habitat_query.to_sql('tbl_phab', sccwrp_engine, if_exists='append', index=False)
'''

