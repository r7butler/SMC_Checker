from proj import app
from functools import wraps
from flask import send_from_directory, render_template, request, redirect, Response, jsonify, json, current_app
from werkzeug import secure_filename
from sqlalchemy import create_engine, text
from sqlalchemy import exc
import urllib, json
import pandas as pd
import numpy as np
import time, datetime
from datetime import datetime
import psycopg2
from pandas import DataFrame
import folium
import xlsxwriter

def support_jsonp(f):
        """Wraps JSONified output for JSONP"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            callback = request.args.get('callback', False)
            if callback:
                content = str(callback) + '(' + str(f(*args,**kwargs).data) + ')'
                return current_app.response_class(content, mimetype='application/javascript')
            else:
                return f(*args, **kwargs)
        return decorated_function

@app.route('/')
def index():
	eng = create_engine('postgresql://smcread:1969$Harbor@192.168.1.17:5432/smc')
	agency_sql = "select agencycode,agencyname from lu_agency order by agencyname asc"
	agency_result = eng.execute(agency_sql)
	owner_sql = "select agencycode,agencyname from lu_dataowner order by agencyname asc"
	owner_result = eng.execute(owner_sql)
	#list_of_agencies = [r for r, in agency_result]
	dict_of_agencies = [dict(r) for r in agency_result]
	dict_of_owners = [dict(r) for r in owner_result]
	#list_of_agencies = ["MYTEST","MYTEST2"]
	#list_of_owners = ["OWNER1","OWNER2"]
	return render_template('index.html', agencies=dict_of_agencies, owners=dict_of_owners)

@app.route('/clear')
def clear():
        eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
        statement = text("""DELETE FROM tbl_taxonomysampleinfo""")
        eng.execute(statement)
        statement2 = text("""DELETE FROM tbl_taxonomyresults""")
        eng.execute(statement2)
        #statement3 = text("""DELETE FROM tmp_cscicore WHERE stationcode = 'SMC01097'""")
        #eng.execute(statement3)
        return "taxonomy beta clear finished"

@app.route('/export', methods=['GET'])
@support_jsonp
def export():
	print("function to export organizations data to excel")
	if request.args.get("callback"):
		action = request.args.get("callback", False)
		if action == "Select All":
			action = "get all"
		if action == "Ventura":
			action = "ventura"
		# variables
		gettime = int(time.time())
		TIMESTAMP = str(gettime)
                print TIMESTAMP

		export_file = '/var/www/smc/logs/%s-export.xlsx' % TIMESTAMP
		export_link = 'http://smcchecker.sccwrp.org/smc/logs/%s-export.xlsx' % TIMESTAMP
		export_writer = pd.ExcelWriter(export_file, engine='xlsxwriter')
		eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
                '''
                sql1_statement = "SELECT t2.stationid, t2.county, t2.smcshed, stationcode, sampledate, agencycode, replicate, sampleid, benthiccollectioncomments, grabsize, percentsamplecounted, totalgrids, gridsanalyzed, gridsvolumeanalyzed, targetorganismcount, actualorganismcount, extraorganismcount, qcorganismcount, discardedorganismcount, benthiclabeffortcomments, finalid, lifestagecode, distinctcode, baresult, resqualcode, qacode, taxonomicqualifier, personnelcode_labeffort, personnelcode_results, labsampleid, locationcode, samplecomments, collectionmethodcode, effortqacode, record_origin, origin_lastupdatedate, record_publish FROM taxonomy t1 INNER JOIN lu_stations t2 ON t1.stationcode = t2.stationid WHERE collectionmethodcode like '%%BMI%%' AND record_publish = 'true'"
                sql1 = eng.execute(sql1_statement)
		taxonomy = DataFrame(sql1.fetchall())
		if len(taxonomy) > 0:
			taxonomy.columns = sql1.keys()
			taxonomy.columns = [x.lower() for x in taxonomy.columns]
			taxonomy.to_excel(export_writer, sheet_name='taxonomy', index = False)
                sql2_statement = "SELECT t2.stationid,t2.county,t2.smcshed,stationcode,sampleid,sampledate,samplemonth,sampleday,sampleyear,collectionmethodcode,fieldreplicate,databasecode,count,number_of_mmi_iterations,number_of_oe_iterations,pcnt_ambiguous_individuals,pcnt_ambiguous_taxa,e,mean_o,oovere,oovere_percentile,mmi,mmi_percentile,csci,csci_percentile,scoredate,scorenotes,cleaned,rand,processed_by,record_origin,origin_lastupdatedate,record_publish FROM csci_core t1 INNER JOIN lu_stations t2 ON t1.stationcode = t2.stationid WHERE collectionmethodcode like '%%BMI%%' AND record_publish = 'true'"
                sql2 = eng.execute(sql2_statement)
	        core = DataFrame(sql2.fetchall())
		if len(core) > 0:
			core.columns = sql2.keys()
			core.columns = [x.lower() for x in core.columns]
			core.to_excel(export_writer, sheet_name='csci_core', index = False)
                sql3_statement = "SELECT t2.stationid,t2.county,t2.smcshed,stationcode,pgroup1, pgroup2, pgroup3, pgroup4, pgroup5,pgroup6, pgroup7, pgroup8, pgroup9, pgroup10, pgroup11, processed_by,record_origin,origin_lastupdatedate,record_publish FROM csci_suppl1_grps t1 INNER JOIN lu_stations t2 ON t1.stationcode = t2.stationid" 
                sql3 = eng.execute(sql3_statement)
		grps = DataFrame(sql3.fetchall())
		if len(grps) > 0:
			grps.columns = sql3.keys()
			grps.columns = [x.lower() for x in grps.columns]
			grps.to_excel(export_writer, sheet_name='csci_suppl1_grps', index = False)
                sql4_statement = "SELECT t2.stationid,t2.county,t2.smcshed,stationcode,sampleid, mmi_score, clinger_percenttaxa,clinger_percenttaxa_predicted, clinger_percenttaxa_score, coleoptera_percenttaxa,coleoptera_percenttaxa_predict, coleoptera_percenttaxa_score, taxonomic_richness, taxonomic_richness_predicted, taxonomic_richness_score, ept_percenttaxa, ept_percenttaxa_predicted, ept_percenttaxa_score, shredder_taxa, shredder_taxa_predicted, shredder_taxa_score, intolerant_percent, intolerant_percent_predicted, intolerant_percent_score,processed_by,record_origin,origin_lastupdatedate,record_publish FROM csci_suppl1_mmi t1 INNER JOIN lu_stations t2 ON t1.stationcode = t2.stationid WHERE record_publish = 'true'"
                sql4 = eng.execute(sql4_statement)
		s1mmi = DataFrame(sql4.fetchall())
		if len(s1mmi) > 0:
			s1mmi.columns = sql4.keys()
			s1mmi.columns = [x.lower() for x in s1mmi.columns]
			s1mmi.to_excel(export_writer, sheet_name='csci_suppl1_mmi', index = False)
                sql5_statement = "SELECT t2.stationid,t2.county,t2.smcshed,stationcode,sampleid, metric, iteration, value, predicted_value, score, processed_by,record_origin,origin_lastupdatedate,record_publish FROM csci_suppl2_mmi t1 INNER JOIN lu_stations t2 ON t1.stationcode = t2.stationid WHERE record_publish = 'true'"
                sql5 = eng.execute(sql5_statement)
		s2mmi = DataFrame(sql5.fetchall())
		if len(s2mmi) > 0:
			s2mmi.columns = sql5.keys()
			s2mmi.columns = [x.lower() for x in s2mmi.columns]
			s2mmi.to_excel(export_writer, sheet_name='csci_suppl2_mmi', index = False)
                sql6_statement = "SELECT t2.stationid,t2.county,t2.smcshed,stationcode,sampleid, otu, captureprob, meanobserved, processed_by,record_origin,origin_lastupdatedate,record_publish FROM csci_suppl1_oe t1 INNER JOIN lu_stations t2 ON t1.stationcode = t2.stationid WHERE record_publish = 'true'"
                sql6 = eng.execute(sql6_statement)
		s1oe = DataFrame(sql6.fetchall())
		if len(s1oe) > 0:
			s1oe.columns = sql6.keys()
			s1oe.columns = [x.lower() for x in s1oe.columns]
			s1oe.to_excel(export_writer, sheet_name='csci_suppl1_oe', index = False)
                sql7_statement = "SELECT t2.stationid,t2.county,t2.smcshed,stationcode,sampleid, otu, captureprob, iteration1, iteration2, iteration3, iteration4, iteration5, iteration6, iteration7, iteration8, iteration9, iteration10, iteration11, iteration12, iteration13, iteration14, iteration15, iteration16, iteration17, iteration18, iteration19, iteration20, processed_by,record_origin,origin_lastupdatedate,record_publish FROM csci_suppl2_oe t1 INNER JOIN lu_stations t2 ON t1.stationcode = t2.stationid WHERE record_publish = 'true'"
                sql7 = eng.execute(sql7_statement)
		s2oe = DataFrame(sql7.fetchall())
		if len(s2oe) > 0:
			s2oe.columns = sql7.keys()
			s2oe.columns = [x.lower() for x in s2oe.columns]
			s2oe.to_excel(export_writer, sheet_name='csci_suppl2_oe', index = False)
                sql8 = eng.execute("SELECT t2.stationid,t2.county,t2.smcshed,stationcode,sampledate,sampletypecode,matrixname,record_origin,origin_lastupdatedate,record_publish FROM swamp_chemistry t1 INNER JOIN lu_stations t2 ON t1.stationcode = t2.stationid WHERE record_publish = 'true'")
		chemistry = DataFrame(sql8.fetchall())
		if len(chemistry) > 0:
			chemistry.columns = sql8.keys()
			chemistry.columns = [x.lower() for x in chemistry.columns]
			chemistry.to_excel(export_writer, sheet_name='chemistry', index = False)
                '''
		eng.dispose()
		export_writer.save()
		print export_link
	else:
		export_link = "empty"
   	response = jsonify({'code': 200,'link': export_link})
        return response

@app.route('/logs/<path:path>')
def send_log(path):
	print("log route")
	print(path)
	#send_from_directory("/some/path/to/static", "my_image_file.jpg")
	return send_from_directory('/var/www/smc/logs/', path)

@app.route('/mail', methods=['GET'])
def mail():
	action = ""
	if request.args.get("action"):
		action = request.args.get("action")
	# if action = critical then email sccwrp im with log file or at least link to log file and timestamp
	# if action = success then email committee and sccwrp im
	msg = Message("test flask email",
                  sender="admin@smcchecker.sccwrp.org",
                  #recipients=["pauls@sccwrp.org"])
                  recipients=["smcim-tox@sccwrp.org"])
	msg.body = "testing flask email - action = " + str(action)
	with flask_app.app_context():
		mail.send(msg)
	return "success"

@app.route('/map')
def map():
	print "start map"
	# basic working
	#map1 = folium.Map(location=[45.5, -73.61], width="100%", height="100%")
	#map1.save('/var/www/smc/logs/map.html')
	action = ""
	if request.args.get("action"):
		action = request.args.get("action")
	'''
	submit stations
	return station, lat, long to map
	{
	    "sites": [
		{ "stationid":"smc123", "latlon":[ "33.123", "-118.33" ] },
		{ "stationid":"smc234", "latlon":[ "33.234", "-118.34" ] }
	    ]
	 }
	'''
	m = folium.Map(
	    location=[33.0000, -117.0000],
	    zoom_start=12,
	    tiles='Stamen Terrain'
	)
	folium.Marker(
	    location=[33.9308, -117.5939],
	    popup='801FC1089',
	    icon=folium.Icon(icon='cloud')
	).add_to(m)

	m.save('/var/www/smc/logs/map.html')
	# neew to fix cache issue
	return redirect("/smc/logs/map.html", code=302)
	#return action

@app.route('/escraper', methods=['GET'])
def escraper():
        print("start error scraper")
        message = request.args.get("message")
        print message
        # unfortunately readonly user doesnt have access to information_schema
        # below should be more sanitized
        # https://stackoverflow.com/questions/39196462/how-to-use-variable-for-sqlite-table-name?rq=1
        # check to make sure table exists before proceeding
        try:
                # get all fields first
                eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/bight2018') # postgresql
                if request.args.get("message"):
                        message = request.args.get("message")
                        sql_results = eng.execute("select datatype as DataType, errortype as ErrorType, columns as Columns, message as Error, description as Description, addresses as Addresses from error_table where message = '%s'" % message)
                        
                else:
                        message = "All Error Messages"
                        sql_results = eng.execute("select datatype as DataType, errortype as ErrorType, columns as Columns, message as Error, description as Description, addresses as Addresses from error_table")
                        
                scraper_results = DataFrame(sql_results.fetchall())
                scraper_results.columns = sql_results.keys()
                # for bight we dont want system columns
                if 'created_user' in scraper_results:
                        scraper_results = scraper_results.drop('created_user', 1)
                if 'created_date' in scraper_results:
                        scraper_results = scraper_results.drop('created_date', 1)
                if 'last_edited_user' in scraper_results:
                        scraper_results = scraper_results.drop('last_edited_user', 1)
                if 'last_edited_date' in scraper_results:
                        scraper_results = scraper_results.drop('last_edited_date', 1)
                if 'objectid' in scraper_results:
                        scraper_results = scraper_results.drop('objectid', 1)
                if 'gdb_geomattr_data' in scraper_results:
                        scraper_results = scraper_results.drop('gdb_geomattr_data', 1)
                if 'globalid' in scraper_results:
                        scraper_results = scraper_results.drop('globalid', 1)
                if 'shape' in scraper_results:
                        scraper_results = scraper_results.drop('shape', 1)
                # turn dataframe into dictionary object
                scraper_json = scraper_results.to_dict('records')
                eng.dispose()
                # give jinga the listname, primary key (to highlight row), and fields/rows
                return render_template('escraper.html', message=message, scraper=scraper_json)

                # if sql error just return empty
                
        except Exception as err:
                print err
                return "empty"

@app.route('/scraper', methods=['GET'])
def scraper():
	print("start scraper")
	if request.args.get("action"):
		action = request.args.get("action")
		message = str(action)
		if request.args.get("layer"):
			layer = request.args.get("layer")
			# layer should start with lu - if not return empty - this tool is only for lookup lists
			if layer.startswith("lu_"):
				# unfortunately readonly user doesnt have access to information_schema
				#eng = create_engine('postgresql://smcread:1969$Harbor@192.168.1.17:5432/smc')
				eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
				# below should be more sanitized
				# https://stackoverflow.com/questions/39196462/how-to-use-variable-for-sqlite-table-name?rq=1
				# check to make sure table exists before proceeding
				# get primary key for lookup list
				sql_primary = "SELECT DISTINCT(kcu.column_name) FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'PRIMARY KEY' AND tc.table_name='%s'" % layer
				try:
					
                                        primary_key_result = eng.execute(sql_primary)
					# there should be only one primary key
					primary_key = primary_key_result.fetchone()
					print "primary_key: %s" % primary_key
					try:
						# get all fields first                      
                                                sql_results = eng.execute("select * from %s order by %s asc" % (layer,primary_key[0]))
                                                scraper_results = DataFrame(sql_results.fetchall())
                                                scraper_results.columns = sql_results.keys()
                                                # for bight we dont want system columns
                                                if 'created_user' in scraper_results:
                                                    scraper_results = scraper_results.drop('created_user', 1)
                                                if 'created_date' in scraper_results:
                                                    scraper_results = scraper_results.drop('created_date', 1)
                                                if 'last_edited_user' in scraper_results:
                                                    scraper_results = scraper_results.drop('last_edited_user', 1)
                                                if 'last_edited_date' in scraper_results:
                                                    scraper_results = scraper_results.drop('last_edited_date', 1)
                                                if 'objectid' in scraper_results:
                                                    scraper_results = scraper_results.drop('objectid', 1)
                                                if 'gdb_geomattr_data' in scraper_results:
                                                    scraper_results = scraper_results.drop('gdb_geomattr_data', 1)
                                                if 'globalid' in scraper_results:
                                                    scraper_results = scraper_results.drop('globalid', 1)
                                                if 'shape' in scraper_results:
                                                    scraper_results = scraper_results.drop('shape', 1)
                                                # turn dataframe into dictionary object
                                                scraper_json = scraper_results.to_dict('records')
                                                # give jinga the listname, primary key (to highlight row), and fields/rows
                                                return render_template('scraper.html', list=layer, primary=primary_key[0], scraper=scraper_json)
                                        # if sql error just return empty
					except Exception as err:
						return "empty"
				except Exception as err:
					return "empty"
				eng.dispose()
			else:
				return "empty"

@app.route('/status', methods=['GET'])
def status():
	if request.args.get("t"):
		timestamp = request.args.get("t")
		print(timestamp)
		status_file = "/var/www/smc/logs/" + timestamp + "-status.txt"
		status_log = open(status_file, 'r')
		status_read = status_log.read()
    		response = jsonify({'code': 200,'message': str(status_read),'timestamp': timestamp})
    		response.status_code = 200
		return response

@app.route('/track')
def track():
	print("start track")
	eng = create_engine('postgresql://smcread:1969$Harbor@192.168.1.17:5432/smc')
	sql_session = "select login, agency, sessionkey, upload, match, mia, lookup, duplicates, extended_checks, extended_checks_type, submit, created_user, created_date from submission_tracking_table order by created_date"
        print(sql_session)
        session_results = eng.execute(sql_session)
	print(session_results)
        eng.dispose()
	#session_json = json.dumps([dict(r) for r in session_results])
	session_json = [dict(r) for r in session_results]
	return render_template('track.html', session=session_json)

@app.route('/uploadtest')
def upload_test():
    print "upload_test"
    return render_template('upload.html')

@app.route('/uploader', methods = ['GET','POST'])
def upload_test_uploader():
    print "upload_test_uploader"
    if request.method == 'POST':
        f = request.files['file']
        filename = secure_filename(f.filename)
        f.save('/var/upload/%s' % filename)
        print filename
        #f.save(secure_filename(f.filename))
        return 'file uploaded successfully'

def errorApp():
	print("error app")
	return render_template('error.html')

@app.errorhandler(Exception)
def default_error_handler(error):
	print("Checker application came across an error...")
	print(str(error))
    	response = jsonify({'code': 500,'message': str(error)})
    	response.status_code = 500
	# need to add code here to email SCCWRP staff about error
	return response
