from proj import app
from flask import send_from_directory, render_template, request, redirect, Response, jsonify, json
from sqlalchemy import create_engine, text
from sqlalchemy import exc
import urllib, json
import pandas as pd
import numpy as np
import psycopg2
from pandas import DataFrame
import folium

@app.route('/')
def index():
	eng = create_engine('postgresql://smcread:1969$Harbor@192.168.1.16:5432/smcphab')
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

@app.route('/agency', methods=['GET'])
def agency():
	print("start retrieve agency")
	agency = ""
	return "<option>TEST</option>"
	#if request.args.get("name"):
	#	agency = request.args.get("name")
	#	return "<option>TEST</option>"
	#else:
	#	return "empty"

@app.route('/clear')
def clear():
	eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
	statement = text("""DELETE FROM tbl_taxonomysampleinfo""")
	eng.execute(statement)
	statement2 = text("""DELETE FROM tbl_taxonomyresults""")
	eng.execute(statement2)
	statement3 = text("""DELETE FROM tmp_cscicore WHERE stationcode = 'SMC01097'""")
	eng.execute(statement3)
	return "taxonomy clear finished"

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
                  sender="admin@checker.sccwrp.org",
                  #recipients=["pauls@sccwrp.org"])
                  recipients=["smcim-tox@sccwrp.org"])
	msg.body = "testing flask email - action = " + str(action)
	with flask_app.app_context():
		mail.send(msg)
	return "success"

@app.route('/map')
def map():
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
			if layer.startswith("lu_"):
				# unfortunately readonly user doesnt have access to information_schema
				#eng = create_engine('postgresql://smcread:1969$Harbor@192.168.1.16:5432/smcphab')
				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
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
						# for smc we only want columns with code or description in the name
						show_cols = [col for col in scraper_results.columns if 'code' in col or 'description' in col or 'unitname' in col]
						scraper_results = scraper_results[show_cols]
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

@app.route('/sandbox')
def sandbox():
	# sandbox to test pieces of code independently
	inFile = '/var/www/smc/files/1503862398.xlsx'
	df = pd.ExcelFile(inFile, keep_default_na=False, na_values=['NaN'])
      	df_tab_names = df.sheet_names
	for sheet in df_tab_names:
		tab_name = sheet
		print(tab_name)
		tab = df.parse(sheet)
		print("finished")
	#return "sandbox"
	return str(df_tab_names)

@app.route('/sandbox-ui')
def sandboxui():
	# sandbox to test various parts of user interface
	return render_template('test.html')

@app.route('/track')
def track():
	print("start track")
	eng = create_engine('postgresql://b18read:1969$Harbor@192.168.1.16:5432/smcphab')
	sql_session = "select login, agency, sessionkey, upload, match, mia, lookup, duplicates, extended_checks, extended_checks_type, submit, created_user, created_date from submission_tracking_table order by created_date"
        print(sql_session)
        session_results = eng.execute(sql_session)
	print(session_results)
        eng.dispose()
	#session_json = json.dumps([dict(r) for r in session_results])
	session_json = [dict(r) for r in session_results]
	return render_template('track.html', session=session_json)

@app.route('/report')
def report():
	print("start report")
	eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab') # postgresql
	# field - sql = "select stationid,grabagency,trawlagency,grabsubmit,trawlsubmit from field_assignment_table where trawlagency = 'Los Angeles County Sanitation Districts' or grabagency = 'Los Angeles County Sanitation Districts' order by stationid asc"
	sql = "select stationid,lab,parameter,submissionstatus from sample_assignment_table where lab = 'Nautilus Environmental' order by submissionstatus desc"
        print(sql)
        results = eng.execute(sql)
	print(results)
        eng.dispose()
	report_results = [dict(r) for r in results]
	return render_template('report.html', agency='Nautilus Environmental', submission_type='toxicity', report=report_results)

def errorApp():
	print("error app")
	return render_template('error.html')

@app.route('/myjson')
def myjson():
   	testList = []
     	testDict = {}
     	count = 0
	testString1 = '{"row": "0", {"value": [{"column": "teststartdate","error_type":"Data Type","error":"tj"}]}}'
	testString2 = '{"row": "1", {"value": [{"column": "teststartdate","error_type":"Data Type","error":"tm"}]}}'
	testList.append(testString1)
	testList.append(testString2)
     	for t in testList:
		testDict[count] = t
		count = count + 1
	return render_template(
		'json.html',
		title='JSON Test Page',
     		result=json.dumps(testDict))
     		#result=json.dumps({"0":[{"row":"0"},{"value": [{"column": "teststartdate","error_type":"Data Type","error":"['must be of integer type']/16/Jul/2013"}]}],"1":[{"row":"1"},{"value": [{"column": "teststartdate","error_type":"Data Type","error":"['must be of integer type']/16/Jul/2014"}]}]}, sort_keys = False, indent = 2))

@app.errorhandler(Exception)
def default_error_handler(error):
	print("Checker application came across an error...")
	print(str(error))
    	response = jsonify({'code': 500,'message': str(error)})
    	response.status_code = 500
	# need to add code here to email SCCWRP staff about error
	return response
