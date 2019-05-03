from proj import app
from functools import wraps
from flask import send_from_directory, render_template, request, redirect, Response, jsonify, json, current_app, Markup
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
from zipfile import ZipFile
from collections import OrderedDict, defaultdict


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
        return "algae, hydromod, siteevaluation, toxicity beta clear finished"

@app.route('/export', methods=['GET'])
@support_jsonp
def export():
        # function to build query from url string and return result as an excel file or zip file if requesting all data
        print "start export"
        admin_engine = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') 
        query_engine = create_engine('postgresql://smcread:1969$Harbor@192.168.1.17:5432/smc')
        # sql injection check one
        def cleanstring(instring):
            # unacceptable characters from input
            special_characters = '''!-[]{};:'"\,<>./?@#$^&*~'''

            # remove punctuation from the string
            outstring = ""
            for char in instring:
                if char not in special_characters:
                    outstring = outstring + char
            return outstring

        # sql injection check two
        def exists_table(local_engine, local_table_name):
            # check lookups
            lsql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_CATALOG=%s"
            lquery = local_engine.execute(lsql, ("smc"))
            lresult = lquery.fetchall()
            lresult = [r for r, in lresult]
            result = lresult 
            if local_table_name in result:
                print "found matching table"
                return 1
            else:
                print "no matching table return empty"
                return 0

	#if request.args.get("action"):
        gettime = int(time.time())
        TIMESTAMP = str(gettime)
        export_file = '/var/www/smc/logs/%s-export.csv' % TIMESTAMP
        export_link = 'http://smcchecker.sccwrp.org/smc/logs/%s-export.csv' % TIMESTAMP

        # sql injection check three
        #valid_tables = ['algae','csci_core', 'csci_suppl1_grps', 'csci_suppl1_mmi', 'csci_suppl1_oe', 'csci_suppl2_mmi', 'csci_suppl2_oe', 'chemistry','channelengineering','cramplants', 'crammetricscores','cramstressors','hydromodresults','phab','phabmetrics','taxonomy','siteeval','timeserieseffortcheck','timeserieseffortdetails','timeseriesresults','toxicitybatch','toxicityresults','toxicitysummary']
        valid_tables = {'algae': 'tbl_algae', 'csci_core': 'csci_core', 'csci_suppl1': 'csci_suppl1_grps', 'csci_suppl1_grps': 'csci_suppl1_grps', 'csci_suppl1_mmi': 'csci_suppl1_mmi', 'csci_suppl1_oe': 'csci_suppl1_oe', 'csci_suppl2': 'csci_suppl2_grps', 'csci_suppl2_grps': 'csci_suppl2_grps', 'csci_suppl2_mmi': 'csci_suppl2_mmi', 'csci_suppl2_oe': 'csci_suppl2_oe', 'cramplants': 'tblcramplants', 'cramstressors': 'tblcramstressors', 'crammetricscores': 'tblcrammetricscores', 'cramindexandattributescores': 'tblcramindexandattributescores', 'channelengineering': 'tmp_channelengineering','hydromod': 'tmp_hydromodresults', 'siteeval': 'tmp_siteeval', 'taxonomy': 'taxonomy', 'chemistry': 'chemistry', 'phab': 'tmp_phab', 'toxicitybatchresults': 'tmp_toxicitybatch', 'toxicitysummary': 'tmp_toxicitysummary'}
        if request.args.get("callback"):
	    test = request.args.get("callback", False)
            print test
        if request.args.get("action"):
            action = request.args.get("action", False)
            print action
            cleanstring(action)
            print action 
        if request.args.get("query"):
            query = request.args.get("query", False)
            print query
            # incoming json string 
            jsonstring = json.loads(query)

        if action == "multiple":
            outlink = 'http://smcchecker.sccwrp.org/smc/logs/%s-export.zip' % (TIMESTAMP)
            zipfile = '/var/www/smc/logs/%s-export.zip' % (TIMESTAMP)
            with ZipFile(zipfile,'w') as zip:
                for item in jsonstring:
                    table_name = jsonstring[item]['table']
                    table_name = table_name.replace("-", "_")
                    # check table_name for prevention of sql injection
                    cleanstring(table_name)
                    print table_name
                    table = valid_tables[table_name]
                    print table
                    check = exists_table(admin_engine, table)
                    if table_name in valid_tables and check == 1:
                        sql = jsonstring[item]['sql']
                        # check sql string - clean it of any special characters
                        cleanstring(sql)
                        outputfilename = '%s-export.csv' % (table_name)
                        outputfile = '/var/www/smc/logs/%s' % (outputfilename)
                        isql = text(sql)
                        print isql
                        rsql = query_engine.execute(isql)
	                df = DataFrame(rsql.fetchall())
	                if len(df) > 0:
                            df.columns = rsql.keys()
                            df.columns = [x.lower() for x in df.columns]
                            print df
                            df.to_csv(outputfile,header=True, index=False, encoding='utf-8')
                            print "outputfile"
                            print outputfile
                            print "outputfilename"
                            print outputfilename
                            zip.write(outputfile,outputfilename) 
                    # if we dont pass validation then something is wrong - just error out
                    else:
                        response = jsonify({'code': 200,'link': export_link})
                        return response

        if action == "single":
            for item in jsonstring:
                table_name = jsonstring[item]['table']
                outlink = 'http://smcchecker.sccwrp.org/smc/logs/%s-%s-export.csv' % (TIMESTAMP,table_name)
                csvfile = '/var/www/smc/logs/%s-%s-export.csv' % (TIMESTAMP,table_name)
                table_name = table_name.replace("-", "_")
                # check table_name for prevention of sql injection
                cleanstring(table_name)
                table = valid_tables[table_name]
                print table
                check = exists_table(admin_engine, table)
                if table_name in valid_tables and check == 1:
                    sql = jsonstring[item]['sql']
                    # check sql string - clean it of any special characters
                    cleanstring(sql)
                    outputfilename = '%s-%s-export.csv' % (TIMESTAMP,table_name)
                    outputfile = '/var/www/smc/logs/%s' % (outputfilename)
                    isql = text(sql)
                    rsql = query_engine.execute(isql)
                    df = DataFrame(rsql.fetchall())
                    if len(df) > 0:
                        df.columns = rsql.keys()
                        df.columns = [x.lower() for x in df.columns]
                        df.to_csv(outputfile,header=True, index=False, encoding='utf-8')
                    else:
                        response = jsonify({'code': 200,'link': export_link})
                        return response

        export_link = outlink
        admin_engine.dispose()
        query_engine.dispose()
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

# for the scraper
import unicodedata as ucd
def html_table(dataframe, name='generic-datatable', verbose_classes=False):
        dataframe_html = '<table id="' + name + '" class="datatable"><thead><tr class="table-header">'
        dataframe_html += '<colgroup>'
        dataframe.fillna('')
        print "building colgroups"
        for column in dataframe.columns:
            dataframe_html += '<col class="' + str(column) + '">'
        dataframe_html += '</colgroup>'
        
        print "Making table headers"
        for col in dataframe.columns:
                dataframe_html += '<th>' + col + '</th>'
        dataframe_html += '</tr></thead>'
        dataframe_html += '<tbody>'
        print "Making table rows"
        for i in range(len(dataframe)):
                print len(dataframe)
                print name
                row = dataframe.iloc[i]
                #print row
                colname_value_pairs = zip(row.keys(), row.values)
                print "Row %s" % i
                
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


                print "writing table cells"
                for pair in colname_value_pairs:
                        #print ucd.normalize('NFKD', pair[0]).encode('ascii','ignore')
                        #print ucd.normalize('NFKD', pair[1]).encode('ascii','ignore')
                        #dataframe_html += '<td class="col-' + str(pair[0]) + '">'
                        #dataframe_html += str(pair[1])
                        try:
                            dataframe_html += '<td class="col-' + ucd.normalize('NFKD', pair[0]).encode('ascii','ignore') + '">'
                            dataframe_html += ucd.normalize('NFKD', pair[1]).encode('ascii','ignore')
                            dataframe_html += '</td>'
                        except Exception as err:
                            print err
                            dataframe_html += '<td></td>'
                
                dataframe_html += '</tr>'

        dataframe_html += '</tbody></table>'
        return dataframe_html


@app.route('/scraper', methods=['GET'])
def scraper():
	print("start scraper")
        # code has been rewritten to prevent sql injection by first checking that the input the user has submitted doesnt have any special characters (except underscores)
        # second we check to make sure that the input matches a table in the database
	# unfortunately readonly user doesnt have access to information_schema
        # admin engine is used to query the information schema
        # query engine should be used for all other queries - read only
	admin_engine = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc') 
	query_engine = create_engine('postgresql://smcread:1969$Harbor@192.168.1.17:5432/smc')
        def cleanstring(instring):
            # unacceptable characters from input
            special_characters = '''!-[]{};:'"\,<>./?@#$^&*~'''

            # remove punctuation from the string
            outstring = ""
            for char in instring:
                if char not in special_characters:
                    outstring = outstring + char
            return outstring

        def exists_table(local_engine, local_table_name):
            # check lookups
            lsql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_CATALOG=%s AND TABLE_NAME LIKE %s"
            lquery = local_engine.execute(lsql, ("smc","lu_%%"))
            lresult = lquery.fetchall()
            lresult = [r for r, in lresult]
            # check views
            vsql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME LIKE %s"
            vquery = local_engine.execute(vsql, ("vw_%%"))
            vresult = vquery.fetchall()
            vresult = [r for r, in vresult]
            # combine lookups and views
            result = lresult + vresult
            if local_table_name in result:
                print "found matching table"
                return 1
            else:
                print "no matching table return empty"
                return 0

	if request.args.get("action"):
		action = request.args.get("action")
		message = str(action)
		if request.args.get("layer"):
			layer = request.args.get("layer")
                        # clean name of input to avoid sql injection
                        layer = cleanstring(layer)
                        # check that the table name exists in the system catalog - to avoid sql injection
                        check = exists_table(admin_engine, layer)
			if (layer.startswith(("lu_","vw_")) and (check == 1)):
				# completed sanitizing above
				# https://stackoverflow.com/questions/39196462/how-to-use-variable-for-sqlite-table-name?rq=1
				# get primary key for lookup list
				sql_primary = "SELECT DISTINCT(kcu.column_name) FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'PRIMARY KEY' AND tc.table_name=%s"
				try:
                                        print "primary key"
                                        # need to use admin connection to query information_schema
					primary_key_result = admin_engine.execute(sql_primary, (layer,))
					# there should be only one primary key
					primary_key = primary_key_result.fetchone()
					print "primary_key: %s" % primary_key
					try:
					        # get all fields first
                                                if primary_key:
                                                    # layer/table has been checked for sql injection so this should be ok - otherwise cant call dynamic table
                                                    sql_secondary = "select * from %s order by %s" % (layer,primary_key[0])
                                                    sql_results = query_engine.execute(sql_secondary)
                                                else:
                                                    primary_key = ["none"]
                                                    sql_secondary = "select * from %s" % (layer)
                                                    sql_results = query_engine.execute(sql_secondary)
                                                scraper_results = DataFrame(sql_results.fetchall())
                                                print scraper_results
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
                                               
                                                print layer
                                                if layer == 'lu_organism':
                                                    organism_sql = "SELECT finalid, commonname, phylum, superclass, lu_organism.class, subclass, superorder, lu_organism.order_, suborder, superfamily, lu_organism.family, subfamily, tribe, genuscomplex, genus, speciescomplex, species, subspecies, variety, taxonomicauthority, finalidauthority, lu_organism.source FROM lu_organism ORDER BY finalid;"
                                                    scraper_results = pd.read_sql(organism_sql, query_engine)
                                                    print scraper_results.head(7)
                                                    #scraper_json = scraper_results.to_dict('records')
                                                    #scraper_html = Markup(html_table(scraper_results, name='mytable'))
                                                    #print scraper_json
                                                    #scraper_html = Markup(ucd.normalize('NKFD', str(scraper_results.to_html())).encode('ascii','ignore'))
                                                    
                                                    scraper_html = scraper_results.to_html(index=False, na_rep='')

                                                    return render_template('organism.html', list=layer, primary='finalid',scraper=scraper_html)
                                                    
                                                    


                                                return render_template('scraper.html', list=layer, primary=primary_key[0], scraper=scraper_json)
                                        # if sql error just return empty 
					except Exception as err:
                                                print err
						return "empty"
				except Exception as err:
					return "empty"
				admin_engine.dispose()
				query_engine.dispose()
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
