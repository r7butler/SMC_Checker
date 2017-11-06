from flask import Blueprint, request, jsonify
import json
from .ApplicationLog import *

fish_checks = Blueprint('fish_checks', __name__)

@fish_checks.route("/fish", methods=["POST"])
def fish():
	print("Function - fish")
	# first we need to find out what type of custom checks we are doing
	# toxicity requires three matching tabs - batch,result,wq
	# if there arent three then bounce
	# try: call ToxicityChecks
	# try: call FishChecks
	try:
		# get filenames from fileupload routine
		message = "Start fish checks..."	
		errorLog(message)
		state = 0
	except ValueError:
		message = "Critical Error: Failed to run fish checks"	
		errorLog(message)
		state = 1
	return jsonify(message=message,state=state)
