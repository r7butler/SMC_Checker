import os, time, datetime
from flask import Blueprint, request, jsonify, current_app, session
from werkzeug import secure_filename
#from flask.ext.mail import Mail, Message
from flask_mail import Mail, Message
import pandas as pd
from sqlalchemy import create_engine, text
from pandas import DataFrame
import datetime
import xlsxwriter
from .ApplicationLog import *


notification_email = Blueprint('notification_email', __name__)

## directly send email to user regarding successful submission
def notify(agency,login,checksum,type,TIMESTAMP):
	errorLog("final_email routine:")
	timestamp_date = datetime.datetime.fromtimestamp(int(TIMESTAMP)).strftime('%Y-%m-%d %H:%M:%S')
	errorLog(TIMESTAMP)
        required_tables_dict = {'algae': ['tbl_algae'], 'channelengineering': ['tbl_channelengineering'],'hydromod': ['tbl_hydromod'], 'siteevaluation': ['tbl_siteeval'], 'taxonomy': ['tbl_taxonomysampleinfo','tbl_taxonomyresults'], 'chemistry': ['tbl_chemistrybatch','tbl_chemistryresults'], 'toxicity': ['tbl_toxicitybatch','tbl_toxicityresults','tbl_toxicitysummary'],'hydromod': ['tbl_hydromod']}
	message_body = "SCCWRP has received a successful %s submission from %s for %s. For future - the user submitted [row count] and [row count] were successfully entered into the database." % (str(type),str(login),str(agency))
	# use report to build message body of email 
	if type in required_tables_dict:
		errorLog(required_tables_dict[type])
		eng = create_engine('postgresql://smcread:1969$Harbor@192.168.1.17:5432/smc')
		message_body = "SCCWRP has received a successful %s submission from %s for %s: SubmissionID - %s\r\n" % (str(type),str(login),str(agency),str(TIMESTAMP))
		for v in required_tables_dict[type]:
			errorLog(v)
			table_name = str(v)
			table = "\r\n ---- %s ----\r\n" % table_name
			total = ""
			group_list = ""
			# checksum on submitted data vs received
			sql_total = "select count(*) from %s where created_date = '%s'" % (table_name,str(timestamp_date))
			errorLog(sql_total)
			sql_total_results = eng.execute(sql_total)
			for s in sql_total_results:
				errorLog(s.count)
				total = total + "\t checksum -  total submitted: " + str(checksum[table_name]) + ", total received: " + str(s.count)
			sql_group = "select created_date,count(*) from %s where login_agency = '%s' group by created_date" % (table_name,str(agency))
			errorLog(sql_group)
			sql_group_results = eng.execute(sql_group)
			for s in sql_group_results:
				errorLog(s)
				group_list = group_list + "\r\n\t submisssions - \r\n\t\t date -- records \r\n\t\t " + str(s.created_date) + " -- " + str(s.count) + "\r\n"
				errorLog(group_list)
                        message_body = message_body + str(table) + str(total) + " - " + str(group_list) + "\r\n"
                # report code location - not used yet in smc
                #if report_link:
                #        report_url = '\r\n ---- Click here for a link to your report: %s ----' % report_link
                #else:
                report_url = ""
                message_body = message_body + str(report_url)
                errorLog(message_body)
		eng.dispose()

	mail = Mail(current_app)
	if type == "algae":
		msg = Message("%s smc - successful data load" % (str(type)),
       		sender="admin@smcchecker.sccwrp.org",
       		recipients=["smc-im@sccwrp.org","%s" % str(login)])
	if type == "channelengineering":
		msg = Message("%s smc - successful data load" % (str(type)),
       		sender="admin@smcchecker.sccwrp.org",
       		recipients=["smc-im@sccwrp.org","%s" % str(login)])
	elif type == "chemistry":
		msg = Message("%s smc - successful data load" % (str(type)),
       		sender="admin@smcchecker.sccwrp.org",
       		recipients=["smc-im@sccwrp.org","%s" % str(login)])
	elif type == "hydromod":
		msg = Message("%s smc - successful data load" % (str(type)),
       		sender="admin@smcchecker.sccwrp.org",
       		recipients=["smc-im@sccwrp.org","%s" % str(login)])
	elif type == "siteevaluation":
		msg = Message("%s smc - successful data load" % (str(type)),
       		sender="admin@smcchecker.sccwrp.org",
       		recipients=["smc-im@sccwrp.org","%s" % str(login)])
	elif type == "taxonomy":
		msg = Message("%s smc - successful data load" % (str(type)),
       		sender="admin@smcchecker.sccwrp.org",
       		recipients=["smc-im@sccwrp.org","%s" % str(login)])
	elif type == "toxicity":
		msg = Message("%s smc - successful data load" % (str(type)),
       		sender="admin@smcchecker.sccwrp.org",
       		recipients=["smc-im@sccwrp.org","%s" % str(login)])
	else:
		msg = Message("%s smc - successful data load" % (str(type)),
       		sender="admin@smcchecker.sccwrp.org",
               	recipients=["smc-im@sccwrp.org","%s" % str(login)])

	msg.body = message_body
	mail.send(msg)
	return "success"

#### CREATE GENERIC FUNCTION
### FOLLOWING VARIALBES: recipients(bight18-monitor,bight18-im,bight18-im-tox, etc...)
#### NEED TIMESTAMP or log file from ApplicationLog 
def send_email():
	errorLog("send_email")
	# if action = success then email committee and sccwrp im
	action = ""
	if request.args.get("action"):
		action = request.args.get("action")
	agency_return = ""
	if request.args.get("agency"):
		agency_return = request.args.get("agency")
	login_return = ""
	if request.args.get("login"):
		login_return = request.args.get("login")
	message_return = ""
	if request.args.get("message"):
		message_return = request.args.get("message")
	type_of_data = ""
	if request.args.get("type"):
		type_of_data = request.args.get("type")
        if request.args.get("timestamp"):
                TIMESTAMP = request.args.get("timestamp")
	mail = Mail(current_app)
	if action == "critical":
		msg = Message("smc - application failure",
                	sender="admin@smcchecker.sccwrp.org",
                  	recipients=["smc-imdev@sccwrp.org"])
		msg.body = "critical body of email - action = " + str(action) + " - " + str(message_return) + " - " + str(login_return)
        # code below should be handled above according to each specific datatype - Paul Smith 4/25/19
	if action == "success":
		msg = Message("smc - successful data load",
               		sender="admin@smcchecker.sccwrp.org",
               		recipients=["smc-im@sccwrp.org"])
		msg.body = "SCCWRP has received a successful %s submission from %s for %s. For future - the user submitted [row count] and [row count] were successfully entered into the database." % (str(type_of_data),str(login_return),str(agency_return))
	mail.send(msg)
	return "success"

def monitor():
	# NEED TO CREATE ATTACHEMENT FUNCTION TO GET LOG FILE AND SEND
	# OTHER ITEMS: WHO IS SUBMITTING/DATASET
	mail = Mail(current_app)
	msg = Message("test flask email",
                  sender="checker@sccwrp.org",
                  recipients=["smc-monitor@sccwrp.org"])
	msg.body = "testing flask email"
	mail.send(msg)
	return "success"


@notification_email.route("/mail", methods=["GET"])
def mail():
	# get action:
	# monitor -> send log file to sccwrp im
	# final -> notify sccwrp of final submission -> requires to send to both sccwrp, user submitting, and maybe committee chair
	errorLog("Function - mail")
	try:
		message = send_email()
		state = 0
	except ValueError:
		state = 1
		message = "Critical Error: Failed to email."	
		errorLog(message)
	return jsonify(message=message,state=state)
