from proj import app
from flask import send_from_directory, render_template, request, jsonify, json
from sqlalchemy import create_engine, text
import urllib, json
import pandas as pd
import numpy as np

@app.route('/')
def index():
	return render_template('index.html')

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

@app.route('/scraper', methods=['GET'])
def scraper():
	print("start scraper")
	if request.args.get("action"):
		action = request.args.get("action")
		message = str(action)
		if request.args.get("layer"):
			layer = request.args.get("layer")
			# filter actions
			# help actions
			help_layer = "smc" + str(layer)
			run_url = "https://gis.sccwrp.org/arcgis/rest/services/{0}/FeatureServer/0/query?where=1=1&returnGeometry=false&outFields=*&f=json".format(help_layer)
			#run_url = "https://gis.sccwrp.org/arcgis/rest/services/{0}/FeatureServer/0/query?where=1=1&outFields=agency,code&returnGeometry=false&f=json".format(help_layer)
			print(run_url)
	url_response = urllib.urlopen(run_url)
	url_json = json.loads(url_response.read())
	fields = url_json['fields']
	field_names = []
	for f in url_json['fields']:
		if(f['name'] not in ["globalid","objectid"]):
			print(f['name'])
			field_names.append(f['name'])
	print fields
	# returns features in detail 
	#print url_json['features'][0]['attributes']
	#for i in url_json['features']:
	#	print i
	#message = message + str(layer)
    	#response = jsonify({'code': 200,'message': message})
    	#response = jsonify({'code': 200,'message': url_json})
    	#response.status_code = 200
	#return str(url_json)
	#return url_json["features"]
	return render_template('scraper.html', helplayer=layer, fieldnames=field_names, data=url_json['features'])


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
