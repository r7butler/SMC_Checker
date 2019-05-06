from proj import app
from flask import send_from_directory, render_template, request, jsonify, json, Markup
from sqlalchemy import create_engine, text
import urllib, json
import pandas as pd
import numpy as np
import time, datetime
import pytz
from datetime import datetime
from pandas import DataFrame
import xlsxwriter
from ApplicationLog import *


@app.route('/')
def index():
	eng = create_engine('postgresql://b18read:1969$Harbor@192.168.1.16:5432/bight2018')
	agency_sql = "select agency from lu_agencies order by agency asc"
	agency_result = eng.execute(agency_sql)
	dict_of_agencies = [dict(r) for r in agency_result]
	return render_template('index.html', agencies=dict_of_agencies)


@app.route('/export')
def export():
	print("function to export organizations data to excel")
	if request.args.get("agency"):
		agency = request.args.get("agency")
		if agency == "ANCHOR":
			agency = "Anchor QEA"
		if agency == "ABC":
			agency = "Aquatic Bioassay and Consulting Laboratories"
		if agency == "AMEC":
			agency = "AMEC, Foster, & Wheeler / WOOD"
		if agency == "CSD":
			agency = "City of San Diego"
		if agency == "LACSD":
			agency = "Los Angeles County Sanitation Districts"
		if agency == "MBC":
			agency = "Marine Biological Consulting"
		if agency == "OCSD":
			agency = "Orange County Sanitation Districts"
		if agency == "CLAEMD":
			agency = "City of Los Angeles Environmental Monitoring Division"
		if agency == "CLAWPD":
			agency = "City of Los Angeles Watershed Protection Division"
		if agency == "SCCWRP":
			agency = "Southern California Coastal Water Research Project"
		if agency == "SPAWAR":
			agency = "Space and Naval Warfare Systems Command"
		if agency == "WESTON":
			agency = "Weston Solutions"
		print "agency: %s" % agency
		# variables
		gettime = int(time.time())
		TIMESTAMP = str(gettime)

		export_file = '/var/www/checker/logs/%s-export.xlsx' % TIMESTAMP
		export_link = '192.168.1.26/checker/logs/%s-export.xlsx' % TIMESTAMP
		export_writer = pd.ExcelWriter(export_file, engine='xlsxwriter')
		eng = create_engine('postgresql://b18read:1969$Harbor@192.168.1.16:5432/bight2018')

		# call database to get occupation data
		sql1 = eng.execute("SELECT stationid,date(occupationdate) as occupationdate,to_char((occupationdate-interval'7 hours'),'HH24:MI:SS') as occupationtime,timezone as occupationtimezone,samplingorganization,collectiontype,vessel,navigationtype as navtype,salinity,salinityunits, weather,windspeed,windspeedunits,winddirection,swellheight,swellheightunits,swellperiod,swelldirection,seastate,stationfail,abandoned,stationdepth as occupationdepth,stationdepthunits as occupationdepthunits,occupationlat as occupationlatitude,occupationlon as occupationlongitude,datum as occupationdatum,stationcomments as comments FROM mobile_occupation_trawl_evw where samplingorganization = '%s' UNION SELECT stationid,date(occupationdate) as occupationdate,to_char((occupationdate-interval'7 hours'),'HH24:MI:SS') as occupationtime,timezone as occupationtimezone,samplingorganization,collectiontype,vessel,navigationtype as navtype,salinity,salinityunits,weather,windspeed,windspeedunits,winddirection,swellheight,swellheightunits,swellperiod,swelldirection,seastate,stationfail,abandoned,stationdepth as occupationdepth,stationdepthunits as occupationdepthunits,occupationlat as occupationlatitude,occupationlon as occupationlongitude,datum as occupationdatum,stationcomments as comments FROM mobile_occupation_grab_evw where samplingorganization = '%s'" % (agency,agency))
		
		occupation = DataFrame(sql1.fetchall())
		if len(occupation) > 0:
			occupation.columns = sql1.keys()
			occupation.columns = [x.lower() for x in occupation.columns]
			occupation.to_excel(export_writer, sheet_name='occupation', index = False)
			print occupation

		# call database to get trawl data
		sql2 = eng.execute("SELECT trawlstationid as stationid,date(trawloverdate) as sampledate,trawlsamplingorganization as samplingorganization,trawlgear as gear,trawlnumber,trawldatum as datum, (trawloverdate-interval'7 hours')::time as overtime, trawlovery as overlatitude, trawloverx as overlongitude,(trawlstartdate-interval'7 hours')::time as starttime, trawlstarty as startlatitude, trawlstartx as startlongitude,trawlstartdepth as startdepth, trawldepthunits as depthunits, trawlwireout as wireout,(trawlenddate-interval'7 hours')::time as endtime, trawlendy as endlatitude, trawlendx as endlongitude,trawlenddepth as enddepth, (trawldeckdate-interval'7 hours')::time as decktime, trawldecky as decklatitude, trawldeckx as decklongitude, trawlfail, ptsensor, ptsensormanufacturer, ptsensorserialnumber,netonbottomtemp as onbottomtemp, netonbottomtime as onbottomtime, trawlcomments as comments FROM mobile_trawl_evw where trawlsamplingorganization = '%s'" % (agency))
		trawl = DataFrame(sql2.fetchall())
		if len(trawl) > 0:
			trawl.columns = sql2.keys()
			trawl.columns = [x.lower() for x in trawl.columns]
			trawl.overtime=trawl.overtime.apply(lambda x: x.strftime('%H:%M:%S'))
			trawl.starttime=trawl.starttime.apply(lambda x: x.strftime('%H:%M:%S'))
			trawl.endtime = trawl.endtime.apply(lambda x: x.strftime('%H:%M:%S'))
			trawl.decktime = trawl.decktime.apply(lambda x: x.strftime('%H:%M:%S'))
			trawl.to_excel(export_writer, sheet_name='trawl', index = False)
			print trawl

		# call database to get grab data
		sql3 = eng.execute("SELECT grabstationid as stationid,date(grabdate) as sampledate, to_char((grabdate-interval'7 hours'), 'HH24:MI:SS') as sampletime,grabnumber as grabeventnumber,grabsamplingorganization as samplingorganization,grabgear as gear, grabx as latitude,graby as longitude, grabdatum as datum, grabstationwaterdepth as stationwaterdepth,grabstationwaterdepthunits as stationwaterdepthunits, grabpenetration as penetration, grabpenetrationunits as penetrationunits, grabsedimentcomposition as composition, grabsedimentcolor as color, grabsedimentodor as odor, grabshellhash as shellhash, benthicinfauna, sedimentchemistry, grainsize, toxicity, grabfail, debris as debrisdetected, grabcomments as comments FROM mobile_grab_evw where grabsamplingorganization = '%s'"  % (agency))
		grab = DataFrame(sql3.fetchall())
		if len(grab) > 0:
			grab.columns = sql3.keys()
			grab.columns = [x.lower() for x in grab.columns]
			grab.to_excel(export_writer, sheet_name='grab', index = False)
			print grab

		eng.dispose()
		export_writer.save()
		print export_link
	else:
		export_link = "empty"
	return render_template('export.html', link=export_link, agency=agency,time=time.time())

@app.route('/logs/<path:path>')
def send_log(path):
	print("log route")
	print(path)
	#send_from_directory("/some/path/to/static", "my_image_file.jpg")
	return send_from_directory('/var/www/checker/logs/', path)

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
                  recipients=["bightim-tox@sccwrp.org"])
	msg.body = "testing flask email - action = " + str(action)
	with flask_app.app_context():
		mail.send(msg)
	return "success"

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
				eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/bight2018') # postgresql
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
		status_file = "/var/www/checker/logs/" + timestamp + "-status.txt"
		status_log = open(status_file, 'r')
		status_read = status_log.read()
    		response = jsonify({'code': 200,'message': str(status_read),'timestamp': timestamp})
    		response.status_code = 200
		return response

@app.route('/sandbox')
def sandbox():
	# sandbox to test pieces of code independently
	inFile = '/var/www/checker/files/1503862398.xlsx'
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
	eng = create_engine('postgresql://b18read:1969$Harbor@192.168.1.16:5432/bight2018')
	sql_session = "select login, agency, sessionkey, upload, match, mia, lookup, duplicates, extended_checks, extended_checks_type, submit, created_user, created_date from submission_tracking_table order by created_date desc"
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
	eng = create_engine('postgresql://b18read:1969$Harbor@192.168.1.16:5432/bight2018')
	agencies = eng.execute("SELECT oldbight13code,agency FROM lu_agencies;")
	ag = DataFrame(agencies.fetchall()); ag.columns = agencies.keys()
	
	# request agency
	if request.args.get("agency"):
		agency = request.args.get("agency")
		agency = ag[(ag.oldbight13code == agency)|(ag.agency == agency)]['agency'].item()
		print("agency: %s" % agency)
	
	# request datatype
	if request.args.get("datatype"):
		datatype = request.args.get("datatype")
		print "datatype: %s" % datatype
	'''
	# create list of abandoned grab stations
	grab_agencies = eng.execute("SELECT stationid, grababandoned FROM field_assignment_table WHERE grabagency = '%s' AND grababandoned = 'Yes';" % agency)
	ga = DataFrame(grab_agencies.fetchall())
	if not ga.empty:
		ga.columns = grab_agencies.keys()
	
	# create list of abandoned trawl stations
	trawl_agencies = eng.execute("SELECT stationid, trawlabandoned FROM field_assignment_table WHERE trawlagency = '%s' AND trawlabandoned = 'Yes';" % agency)
	ta = DataFrame(trawl_agencies.fetchall())
	if not ta.empty:
		ta.columns = trawl_agencies.keys()
	
	# merge lists of abandoned stations
	if not ga.empty and not ta.empty:
		abandoned_stations = ga.merge(ta, on = ['stationid'], how = 'outer')
	if not ga.empty and ta.empty:
		abandoned_stations = ga
	if not ta.empty and ga.empty:
		abandoned_stations = ta
	'''
	# begin building report
	if datatype != "Field":
		# Dataframe for submission status
		sql = "select stationid,parameter,submissionstatus from sample_assignment_table where lab = '%s' and datatype = '%s'" % (agency,datatype)
		print(sql)
		sql_results = eng.execute(sql)
		report = DataFrame(sql_results.fetchall())
		report.columns = sql_results.keys()
		# convert empty strings to 'missing' in submissionstatus field
		report['submissionstatus'] = report['submissionstatus'].replace('','missing')
		# remove any records that have station on abandoned_stations list
		#if not abandoned_stations.empty:
		#	report.loc[report.stationid.isin(abandoned_stations.stationid.tolist()),'submissionstatus'] = 'abandoned'
		
		# Add columns of unique parameter to report. Remove parameter,submissionstatus columns
		for par in report.parameter.unique().tolist():
			report[par] = report.submissionstatus.where(report.parameter == par)
			report[par] = report[par].apply(lambda x: '' if x is None else x)
		report.drop(['parameter','submissionstatus'],axis=1,inplace=True)
		print(report)
		
		# melt the dataframe on stationids
		report = report.groupby('stationid').apply(lambda x: x.ffill().iloc[-1, 1:]).reset_index()
		
	else:
		
		field_assgn_sql = "SELECT stationid, grab, trawl, grabagency, trawlagency, grabsubmit, trawlsubmit, grabstationfail, trawlstationfail, grababandoned, trawlabandoned FROM field_assignment_table WHERE grabagency = '%s' OR trawlagency = '%s';" % (agency,agency)
		fld_assgn_tbl = eng.execute(field_assgn_sql)
		fat = DataFrame(fld_assgn_tbl.fetchall())
		fat.columns = fld_assgn_tbl.keys()

		# occupation records for associated agency
		sql_occ = "SELECT stationid, samplingorganization, collectiontype, stationfail, abandoned FROM tbl_stationoccupation WHERE samplingorganization = '%s';" % agency
		sql_occ_results = eng.execute(sql_occ)
		occ_report = DataFrame(sql_occ_results.fetchall())
		occ_report.columns = sql_occ_results.keys()


		# GRAB RECORDS
		grab_assignments = fat[fat.grab.str.lower() == 'yes']
		grab_occupations = occ_report[occ_report.collectiontype.str.lower() == 'grab']
		if not grab_assignments.empty:
		    # build new field that shows whether or not occupation data was submitted
		    grab_assignments['occupation'] = grab_assignments.apply(lambda x: 'missing' if x.stationid not in grab_occupations.stationid.tolist() else '', axis =1)
		    grab_assignments['occupation'] = grab_assignments.apply(lambda x: 'abandoned' if x.stationid in occ_report[occ_report.abandoned == 'Yes'].stationid.tolist() else x.grabsubmit, axis=1)
		    # drop irrelevant fields
		    grab_assignments.drop(['grab','trawl','grabagency','trawlagency','trawlsubmit','grabstationfail','trawlstationfail','grababandoned','trawlabandoned'],axis=1,inplace=True)
		    print(grab_assignments)



		# TRAWL RECORDS
		trawl_assignments = fat[fat.trawl.str.lower() == 'yes']
		trawl_occupations = occ_report[occ_report.collectiontype.str.lower().str.contains('trawl')]
		if not trawl_assignments.empty:
		    # build new field that shows whether or not occupation data was submitted
		    trawl_assignments['occupation'] = trawl_assignments.apply(lambda x: 'missing' if x.stationid not in trawl_occupations.stationid.tolist() else '', axis =1)
		    trawl_assignments['occupation'] = trawl_assignments.apply(lambda x: 'abandoned' if x.stationid in occ_report[occ_report.abandoned == 'Yes'].stationid.tolist() else x.trawlsubmit, axis=1)
		    # drop irrelevant fields
		    trawl_assignments.drop(['grab','trawl','grabagency','trawlagency','grabsubmit','grabstationfail','trawlstationfail','grababandoned','trawlabandoned'],axis=1,inplace=True)
		    print(trawl_assignments)



		# BUILD FINAL REPORT
		cols = ['stationid','occupation','trawlsubmit','grabsubmit']
		if not grab_assignments.empty and not trawl_assignments.empty:
		    report = grab_assignments.merge(trawl_assignments, on = 'stationid', how = 'outer')
		    report['occupation'] = report.occupation_x.fillna(report.occupation_y)
		    report.drop(['occupation_x','occupation_y'],axis=1,inplace=True)
		    # reorder columns
		    report = report[['stationid','occupation','trawlsubmit','grabsubmit']]
		if not grab_assignments.empty and trawl_assignments.empty:
		    report = grab_assignments
		    report = report[['stationid','occupation','grabsubmit']]
		if not trawl_assignments.empty and grab_assignments.empty:
		    report = trawl_assignments
		    report = report[['stationid','occupation','trawlsubmit']]
		'''
		# field assignment data. Note: Here I assume that every agency will have at least a trawl or a grab assignment
		field_assgn_sql = "SELECT stationid, grab, trawl, grabagency, trawlagency, grabsubmit, trawlsubmit, grabstationfail, trawlstationfail, grababandoned, trawlabandoned FROM field_assignment_table WHERE grabagency = '%s' OR trawlagency = '%s';" % agency
		fld_assgn_tbl = eng.execute(field_assgn_sql)
		fat = DataFrame(fld_assgn_tbl.fetchall())

		# occupation data associated to agency
		sql_station_occ = "SELECT stationid, samplingorganization, collectiontype, stationfail, abandoned FROM tbl_stationoccupation WHERE samplingorganization = '%s';" % agency
		
		sql_occ_results = eng.execute(sql_occ)
		occ_report = DataFrame(sql_occ_results.fetchall())
		print("occupation report:")
		print(occ_report)
		if len(occ_report.index) > 0:
			occ_report.columns = sql_occ_results.keys()
		# trawl data associated to agency
		sql_trawl = "select stationid,trawlagency,trawlsubmit from field_assignment_table where trawlagency = '%s'" % agency
		print(sql_trawl)
		sql_trawl_results = eng.execute(sql_trawl)
		trawl_report = DataFrame(sql_trawl_results.fetchall())
		print("trawl report:")
		print(trawl_report)
		if len(trawl_report.index) > 0:
			trawl_report.columns = sql_trawl_results.keys()
			trawl_report.drop(['trawlagency'],axis=1, inplace=True)
			trawl_report['trawlsubmit'] = trawl_report['trawlsubmit'].replace('','missing')
			if not ta.empty:	
				trawl_report.loc[trawl_report.stationid.isin(ta.stationid.tolist()),'trawlsubmit'] = 'abandoned'

		# grab data associated to agency
		sql_grab  = "select stationid,grabagency,grabsubmit from field_assignment_table where grabagency = '%s'" % agency
		print(sql_grab)
		sql_grab_results = eng.execute(sql_grab)
		grab_report = DataFrame(sql_grab_results.fetchall())
		print("grab report:")
		print(grab_report)
		if len(grab_report.index) > 0:
			grab_report.columns = sql_grab_results.keys()
			grab_report.drop(['grabagency'],axis=1,inplace=True)
			grab_report['grabsubmit'] = grab_report['grabsubmit'].replace('','missing')
			if not ga.empty:
				grab_report.loc[grab_report.stationid.isin(ga.stationid.tolist()),'grabsubmit'] = 'abandoned'
		
		# build final report. If necessary, merge trawl and grab reports
		if not trawl_report.empty and not grab_report.empty:
			report = trawl_report.merge(grab_report, on = ['stationid'], how = 'outer')
		if not trawl_report.empty and grab_report.empty:
			report = trawl_report
		if not grab_report.empty and trawl_report.empty:
			report = grab_report
		'''

		# only display B18 stations
		report = report[report['stationid'].str.contains('B18')]

		# order report based on value after 'B18-'
		report['sort'] = report['stationid'].str.extract('((?<=-).*$)', expand = False).astype(int)
		report.sort_values('sort',inplace=True)
		report = report.drop('sort',axis=1)
	
	# converts pandas dataframe to html, removes indices and treats NaNs as empty strings
	report_html = report.to_html(index=False,na_rep='')
	return render_template('report-test.html', agency=agency, data=report_html)


#Benthic InfaunalAbundance discrepancy report for David Gillet
@app.route('/discrep')
def discrep():
		
	# Connect to database
	eng = create_engine("postgresql://sde:dinkum@192.168.1.16:5432/bight2018")
	
	initial_sql = "SELECT taxon as original_species, abundance as original_abundance, voucher as original_voucher, stationid FROM tmp_infaunalabundance_initial;"
	initial = pd.read_sql(initial_sql, eng)

	qc_sql = "SELECT taxon as qc_species, abundance as qc_abundance, stationid FROM tmp_infaunalabundance_reid;"
	qc = pd.read_sql(qc_sql, eng)
	
	# Now if there haven't been changes to the database there's no need to run the discrepancy report.
	current_initial = pd.read_csv("/var/www/checker/proj/discrepancy_report/initial.csv")
	current_qc = pd.read_csv("/var/www/checker/proj/discrepancy_report/qc.csv")
	
	errorLog("current initial:")
	errorLog(current_initial.head())
	errorLog("The head of the initial data:")
	errorLog(initial.head(7))
	errorLog("current QC:")
	errorLog(current_qc.head())
	errorLog("The head of the QC Data:")
	errorLog(qc.head())

	errorLog("initial equals current initial:")
	errorLog(initial.equals(current_initial))
	errorLog("qc equals current qc:")
	errorLog(qc.equals(current_qc))
	
	#if (initial.equals(current_initial)) and (qc.equals(current_qc)):		
	if True:
		discrep = pd.read_csv("/var/www/checker/proj/discrepancy_report/discrep.csv")
		initial_html_file = open("/var/www/checker/proj/discrepancy_report/initial_html.txt", 'r')
		initial_html = Markup(initial_html_file.read())
		initial_html_file.close()

		qc_html_file = open("/var/www/checker/proj/discrepancy_report/qc_html.txt", 'r')
		qc_html = Markup(qc_html_file.read())
		qc_html_file.close()
		
		discrep_html_file = open("/var/www/checker/proj/discrepancy_report/discrep_html.txt", 'r')
		discrep_html = Markup(discrep_html_file.read())
		discrep_html_file.close()
	
		return render_template("discrepancy_report.html", discrep_html=discrep_html,qc_html=qc_html,initial_html=initial_html,initial=initial,qc=qc,discrep=discrep)
	

	# Now we define the function that will be used to build the report
	def discrepancy_check(row):
	    'A function to be applied to the "output" dataframe row by row'
	    if ((pd.isnull(row['original_species'])) or (row['original_species'] == '')):
		row['match_or_nomatch'] = 'Not Match'
		row['type'] = 'ID'
	    else:
		if ((pd.isnull(row['qc_species'])) or (row['qc_species'] == '')):
		    if row['original_abundance'] == row['original_voucher']:
			row['match_or_nomatch'] = 'Match'
			row['type'] = ''
		    else:
			row['match_or_nomatch'] = 'Not Match'
			row['type'] = 'ID'
		else:
		    if (row['original_voucher'] != -88):
			if row['qc_abundance'] == row['original_abundance'] - row['original_voucher']:
			    row['match_or_nomatch'] = 'Match'
			    row['type'] = ''
			else:
			    row['match_or_nomatch'] = 'Not Match'
			    row['type'] = 'Count'
		    else:
			if row['qc_abundance'] == row['original_abundance']:
			    row['match_or_nomatch'] = 'Match'
			    row['type'] = ''
			else:
			    row['match_or_nomatch'] = 'Not Match'
			    row['type'] = 'Count'
	    return pd.Series([row['match_or_nomatch'], row['type']])	
	
	# A function that will be used to put the assigned and submitting agencies as columns in the dataframes
	# didn't want to write the same code 3 times, so I wrote this function
	def append_agencies(dataframe):
		agencies_sql = text(
			"SELECT "
				"sample_assignment_table.stationid as stationid,"
				"tbl_infaunalabundance_initial.lab as submitting_agency, "
				"sample_assignment_table.lab as assigned_agency	"
			"FROM tbl_infaunalabundance_initial JOIN sample_assignment_table "
			"ON tbl_infaunalabundance_initial.stationid = sample_assignment_table.stationid "
			"WHERE sample_assignment_table.datatype = 'Infauna';"
		)
		
		agencies = pd.read_sql(agencies_sql, eng)
		newdf = pd.merge(dataframe, agencies, on = 'stationid', how = 'left').drop_duplicates()
		return newdf
        
	# Now we build the html for the summary table, to display on the front end. ##
        def html_table(dataframe, name='generic-datatable', verbose_classes=False):
                dataframe_html = '<table id="' + name + '" class="datatable"><thead><tr class="table-header">'
                dataframe_html += '<colgroup>'
		errorLog("building colgroups")
		for column in dataframe.columns:
		    dataframe_html += '<col class="' + str(column) + '">'
                dataframe_html += '</colgroup>'
		
		errorLog("Making table headers")
                for col in dataframe.columns:
                        dataframe_html += '<th>' + col + '</th>'
                dataframe_html += '</tr></thead>'
                dataframe_html += '<tbody>'
		errorLog("Making table rows")
                for i in range(len(dataframe)):
			errorLog(len(dataframe))
			errorLog(name)
                        row = dataframe.iloc[i]
			errorLog(row)
                        colname_value_pairs = zip(row.keys(), row.values)
			errorLog("Row %s" % i)
                        
			dataframe_html += '<tr class="'
                        if i % 2 == 0:
                                dataframe_html += 'row-even'
                        else:
                                dataframe_html += 'row-odd'
			
			if verbose_classes:
				dataframe_html += ' '
				for pair in colname_value_pairs:
					dataframe_html += 'c-%s--v-%s ' % (pair[0], pair[1])
				dataframe_html += '">'
			else:
				dataframe_html += '">'
	

			errorLog("writing table cells")	
                        for pair in colname_value_pairs:
                                dataframe_html += '<td class="col-' + str(pair[0]) + '">'
                                dataframe_html += str(pair[1])
                                dataframe_html += '</td>'
			
			
                        dataframe_html += '</tr>'

                dataframe_html += '</tbody></table>'
                return dataframe_html

		
	
	errorLog("Begin Discrepancy Report routine.")	

	# discrep is short for discrepancy report
	errorLog("Merging initial and qc dataframes")
	discrep = pd.merge(initial, qc, left_on = ["stationid", "original_species"], right_on = ['stationid', 'qc_species'], how = 'outer')
	errorLog("Merging  initial and qc dataframes: DONE")
	errorLog("The head of the merged dataframe:")
	errorLog(discrep.head())

	errorLog("Re-ordering the columns:")
	discrep = discrep[['stationid', 'original_species', 'original_abundance', 'original_voucher', 'qc_species', 'qc_abundance']]
	errorLog(discrep.head())
	
	errorLog("Running discrepancy report:")
	discrep[['match_or_nomatch', 'type']] = discrep.apply(discrepancy_check, axis=1)
	errorLog("Running discrepancy report: DONE")
	errorLog(discrep.head())

	# We need to pull the assigned agency and the agency that submitted each record
	# From the actual initial table as well as the sample assignment table
	

	
	errorLog("Begin sorting columns")
	discrep.sort_values(['stationid', 'match_or_nomatch', 'original_species', 'qc_species'], ascending = [True, False, True, True], inplace = True)
	errorLog("Sorting columns: DONE")
	errorLog(discrep.head())
	
	initial = append_agencies(initial)
	qc = append_agencies(qc)
	discrep = append_agencies(discrep)	

		
	errorLog("Building the Discrepancy Report table")
	initial_html = html_table(initial, name='initial', verbose_classes=True)	
	qc_html = html_table(qc, name='qc', verbose_classes=True)	
	discrep_html = html_table(discrep, name='discrep', verbose_classes=True)	
	errorLog("Building the Discrepancy Report table: DONE")	
	
	initial_html = Markup(initial_html)
	qc_html = Markup(qc_html)
	discrep_html = Markup(discrep_html)
	
	'''
	errorLog("converting to html")	
	initial_html = initial.to_html()	
	qc_html = qc.to_html()
	discrep_html = discrep.to_html()	
	errorLog("converting to html: DONE")
	'''
	
	initial_html_file = open("/var/www/checker/proj/discrepancy_report/initial_html.txt", 'w')
	initial_html_file.write(initial_html)
	initial_html_file.close()
	
	qc_html_file = open("/var/www/checker/proj/discrepancy_report/qc_html.txt", 'w')
	qc_html_file.write(qc_html)
	qc_html_file.close()
	
	discrep_html_file = open("/var/www/checker/proj/discrepancy_report/discrep_html.txt", 'w')
	discrep_html_file.write(discrep_html)
	discrep_html_file.close()
	
	#TIMESTAMP = time.time()
	errorLog("creating the excel writer")
	report_writer = pd.ExcelWriter('/var/www/checker/logs/discrepancy_report.xlsx', engine = 'xlsxwriter')
	errorLog("creating the excel writer: DONE")
	
	errorLog("writing the initial dataframe to the excel workbook:")
	initial.to_excel(report_writer, sheet_name="Original_Data", index = False)
	initial.to_csv("/var/www/checker/proj/discrepancy_report/initial.csv", index = False)
	errorLog("writing the initial dataframe to the excel workbook: DONE")

	errorLog("writing the QC_Data dataframe to the excel workbook:")
	qc.to_excel(report_writer, sheet_name="QC_Data", index = False)
	qc.to_csv("/var/www/checker/proj/discrepancy_report/qc.csv", index = False)
	errorLog("writing the QC_Data dataframe to the excel workbook: DONE")

	errorLog("writing the discrepancy report dataframe to the excel workbook:")
	discrep.to_excel(report_writer, sheet_name="Discrepancy_Report", index = False)	
	discrep.to_csv("/var/www/checker/proj/discrepancy_report/discrep.csv", index = False)
	errorLog("writing the discrepancy report dataframe to the excel workbook: DONE")
	
	errorLog("Saving the workbook")
	report_writer.save()	
	errorLog("Saving the workbook: DONE")


	return render_template("discrepancy_report.html", discrep_html=discrep_html,qc_html=qc_html,initial_html=initial_html,initial=initial,qc=qc,discrep=discrep)

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

@app.route("/errors")
def errorspage():
	return render_template('testing.html')
