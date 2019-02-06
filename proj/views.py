from proj import app
from functools import wraps
from flask import send_from_directory, render_template, request, redirect, Response, jsonify, json, current_app
from werkzeug import secure_filename
from sqlalchemy import create_engine, text
from sqlalchemy import exc
from sentry_sdk import capture_message
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
        statement3 = text("""DELETE FROM tbl_channelengineering""")
        eng.execute(statement3)
        statement4 = text("""DELETE FROM tbl_toxicitybatch""")
        eng.execute(statement4)
        statement5 = text("""DELETE FROM tbl_toxicityresults""")
        eng.execute(statement5)
        statement6 = text("""DELETE FROM tbl_toxicitysummary""")
        eng.execute(statement6)
        statement7 = text("""DELETE FROM tbl_algae""")
        eng.execute(statement7)
        statement8 = text("""DELETE FROM tbl_siteeval""")
        eng.execute(statement8)
        statement9 = text("""DELETE FROM tbl_hydromod""")
        eng.execute(statement9)
        return "algae, channelengineering, hydromod, siteevaluation, taxonomy, toxicity beta clear finished"

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

@app.route('/scraper', methods=['GET'])
def scraper():
	print("start scraper")
	if request.args.get("action"):
		action = request.args.get("action")
		message = str(action)
		if request.args.get("layer"):
			layer = request.args.get("layer")
			# layer should start with lu - if not return empty - this tool is only for lookup lists
                        print "layer"
			if layer.startswith(("lu_","vw_")):
                                print layer
				# unfortunately readonly user doesnt have access to information_schema
				#eng = create_engine('postgresql://smcread:1969$Harbor@192.168.1.17:5432/smc')
				eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') # postgresql
				# below should be more sanitized
				# https://stackoverflow.com/questions/39196462/how-to-use-variable-for-sqlite-table-name?rq=1
				# check to make sure table exists before proceeding
				# get primary key for lookup list
				sql_primary = "SELECT DISTINCT(kcu.column_name) FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'PRIMARY KEY' AND tc.table_name='%s'" % layer
				try:
                                        print "primary key"
					primary_key_result = eng.execute(sql_primary)
					# there should be only one primary key
					primary_key = primary_key_result.fetchone()
					#print "primary_key: %s" % primary_key
					try:
					        # get all fields first
                                                if primary_key:
                                                    sql_results = eng.execute("select * from %s order by %s asc" % (layer,primary_key[0]))
                                                else:
                                                    primary_key = ["none"]
                                                    sql_results = eng.execute("select * from %s" % (layer))
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
                                                print "render"
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
	#sql_session = "select login, agency, sessionkey, upload, match, mia, lookup, duplicates, extended_checks, extended_checks_type, submit, created_user, created_date from submission_tracking_table order by created_date"
	sql_session = "select login, agency, t1.sessionkey, upload, match, mia, lookup, duplicates, extended_checks, extended_checks_type, submit, tablename, checksum, created_user, created_date from submission_tracking_table t1 left join submission_tracking_checksum t2 on t1.sessionkey = t2.sessionkey"
        print(sql_session)
        session_results = eng.execute(sql_session)
	print(session_results)
        eng.dispose()
	#session_json = json.dumps([dict(r) for r in session_results])
	session_json = [dict(r) for r in session_results]
	return render_template('track.html', session=session_json)

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
        capture_message(str(error))
    	response = jsonify({'code': 500,'message': str(error)})
    	response.status_code = 500
	# need to add code here to email SCCWRP staff about error
	return response
