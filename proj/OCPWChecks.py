from flask import Blueprint, request, jsonify
import json
from .ApplicationLog import *

ocpw_checks = Blueprint('ocpw_checks', __name__)

@ocpw_checks.route("/ocpw", methods=["POST"])
def ocpw():
	errorLog("Function - ocpw")
	# first we need to find out what type of custom checks we are doing
	# toxicity requires three matching tabs - batch,result,wq
	# if there arent three then bounce
	# try: call ToxicityChecks
	# try: call FishChecks
	try:
		# get filenames from fileupload routine
		message = "Start ocpw checks..."	
		errorLog(message)
		state = 0
	except ValueError:
		message = "Critical Error: Failed to run ocpw checks"	
		errorLog(message)
		state = 1
	return jsonify(message=message,state=state)
