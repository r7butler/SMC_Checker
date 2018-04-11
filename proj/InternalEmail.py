import os, time, datetime
from flask import Blueprint, request, jsonify, current_app 
from werkzeug import secure_filename
from flask.ext.mail import Mail, Message
from .ApplicationLog import *


internal_email = Blueprint('internal_email', __name__)

def internal_email(action,sendfrom,sendto,subject,body):
	state = 0
	# action = error,issue,submission - this list will expand over time
	errorLog("Function - internal mail")
	errorLog(action)
	try:
		mail = Mail(current_app)
		msg = Message(subject,
                	sender=sendfrom,
                  	recipients=sendto)
		msg.body = body
		mail.send(msg)
		message = "Email sent successfully."	
		state = 0
	except ValueError:
		state = 1
		message = "Critical Error: Failed to email."	
		errorLog(message)
	return state
