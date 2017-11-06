import os, time, datetime
from flask import Blueprint, request, jsonify, current_app 
from werkzeug import secure_filename
from flask.ext.mail import Mail, Message
from .ApplicationLog import *


notification_email = Blueprint('notification_email', __name__)

#### CREATE GENERIC FUNCTION
### FOLLOWING VARIALBES: recipients(bight18-monitor,bight18-im,bight18-im-tox, etc...)
#### NEED TIMESTAMP or log file from ApplicationLog 
def test_email():
	# if action = critical then email sccwrp im with log file or at least link to log file and timestamp
	# if action = success then email committee and sccwrp im
	action = ""
	if request.args.get("action"):
		action = request.args.get("action")
	type_of_data = ""
	if request.args.get("type"):
		type_of_data = request.args.get("type")
	mail = Mail(current_app)
	if action == "critical":
		msg = Message("checker - application failure",
                	sender="admin@checker.sccwrp.org",
                  	recipients=["pauls@sccwrp.org"])
		msg.body = "critical body of email - action = " + str(action)
	if action == "success":
		# if type_of_data = toxicity then email
		# send_to = "b18im-tox" - toxicity committee
		if type_of_data == "toxicity":
			msg = Message("toxicity checker - successful data load",
                  		sender="admin@checker.sccwrp.org",
                  		recipients=["bight18im-tox@sccwrp.org"])
		else:
			msg = Message("checker - successful data load",
                  		sender="admin@checker.sccwrp.org",
                  		recipients=["pauls@sccwrp.org"])
		msg.body = "successful body of email - action = " + str(action) + " - " + str(type_of_data)
	mail.send(msg)
	return "success"

def monitor():
	# NEED TO CREATE ATTACHEMENT FUNCTION TO GET LOG FILE AND SEND
	# OTHER ITEMS: WHO IS SUBMITTING/DATASET
	mail = Mail(current_app)
	msg = Message("test flask email",
                  sender="checker@sccwrp.org",
                  recipients=["bight18-monitor@sccwrp.org"])
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
