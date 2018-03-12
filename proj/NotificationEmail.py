import os, time, datetime
from flask import Blueprint, request, jsonify, current_app 
from werkzeug import secure_filename
from flask.ext.mail import Mail, Message
from .ApplicationLog import *


notification_email = Blueprint('notification_email', __name__)

#### CREATE GENERIC FUNCTION
### FOLLOWING VARIALBES: recipients(smc-monitor,smc-im,smc-im-tox, etc...)
#### NEED TIMESTAMP or log file from ApplicationLog 
def test_email():
	# if action = critical then email sccwrp im with log file or at least link to log file and timestamp
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
	mail = Mail(current_app)
	if action == "critical":
		msg = Message("smc - application failure",
                	sender="admin@checker.sccwrp.org",
                  	recipients=["pauls@sccwrp.org"])
		msg.body = "critical body of email - action = " + str(action) + " - " + str(message_return) + " - " + str(login_return)
	if action == "success":
		# if type_of_data = toxicity then email
		# send_to = "b18im-tox" - toxicity committee
		if type_of_data == "toxicity":
			msg = Message("toxicity smc - successful data load",
                  		sender="admin@checker.sccwrp.org",
                  		recipients=["b18im-tox@sccwrp.org"])
		elif type_of_data == "field":
			msg = Message("field smc - successful data load",
                  		sender="admin@checker.sccwrp.org",
                  		recipients=["b18im-field@sccwrp.org"])
		else:
			msg = Message("smc - successful data load",
                  		sender="admin@checker.sccwrp.org",
                  		recipients=["pauls@sccwrp.org"])
		#msg.body = "successful body of email - action = " + str(action) + " - " + str(type_of_data) + " - " + str(message_return) + " - " + str(login_return)
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
		message = test_email()
		state = 0
	except ValueError:
		state = 1
		message = "Critical Error: Failed to email."	
		errorLog(message)
	return jsonify(message=message,state=state)
