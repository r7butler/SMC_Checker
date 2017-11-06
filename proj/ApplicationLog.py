from __future__ import print_function
from flask import Blueprint, jsonify, render_template, current_app
import sys, logging, time, datetime

logging.basicConfig(stream=sys.stderr)

application_log = Blueprint('application_log', __name__)

errors_dict = {'total': 0, 'mia': 0, 'lookup': 0, 'duplicate': 0, 'custom': 0, 'match': 0}
log_file = ""

def errorLog(message):
	TIMESTAMP = current_app.timestamp
	#TIMESTAMP = current_app.timestamp
	log_file = '/var/www/checker/logs/%s.log' % TIMESTAMP
	try:
		#print("log_file: %s" % log_file)
		user_log = open(log_file, 'a')
		# print debug messages to apache log and user debug file
		print(message)
		print(message, file = user_log)
		user_log.close()
		return 1
	except IOError:
		print("Critical Error: Failed to open log file")
		return 0

def errorsCount(error_key):
	errors_dict[error_key] = errors_dict[error_key] + 1
	errors_dict['total'] = errors_dict['total'] + 1
	errorLog("errorsCount: %s now %s" % (error_key,errors_dict[error_key]))
	return

def errorsCount(error_key):
	errors_dict[error_key] = errors_dict[error_key] + 1
	errors_dict['total'] = errors_dict['total'] + 1
	errorLog("errorsCount: %s now %s" % (error_key,errors_dict[error_key]))
	return

def statusLog(message):
	TIMESTAMP = current_app.timestamp
	status_file = '/var/www/checker/logs/%s-status.txt' % TIMESTAMP
	try:
		status_log = open(status_file, 'w')
		# print debug messages to apache log and user debug file
		print(message, file = status_log)
		status_log.close()
		return 1
	except IOError:
		print("Critical Error: Failed to open status file")
		return 0

@application_log.route("/application", methods=["POST"])
def application():
	gettime = int(time.time())
	TIMESTAMP = str(gettime)
	current_app.timestamp = TIMESTAMP
	log_file = '/var/www/checker/logs/%s.log' % TIMESTAMP
	status_file = '/var/www/checker/logs/%s-status.txt' % TIMESTAMP
	current_app.status_file = status_file
	www_log_file = 'http://prochecker.sccwrp.org/checker/logs/%s.log' % TIMESTAMP
	www_status_file = 'http://prochecker.sccwrp.org/checker/logs/%s-status.txt' % TIMESTAMP
	message = "start application"
	user_log = open(log_file, 'a')
	status_log = open(status_file, 'a')
	errorLog("first write to errorLog")
	statusLog("first write to statusLog")
	return jsonify(log_file=www_log_file,status_file=www_status_file,timestamp=TIMESTAMP)
