from flask import Blueprint, request, jsonify
import json
from .ApplicationLog import *

finish_app = Blueprint('finish_app', __name__)

@finish_app.route("/finish", methods=["POST"])
def finish():
	print("Function - finish")
	try:
		# get filenames from fileupload routine
		message = "Start finish..."	
		errorLog(message)
		state = 0
	except ValueError:
		message = "Critical Error: Failed to run finish"	
		errorLog(message)
		state = 1
	errors_count = json.dumps(errors_dict)
	return jsonify(errors=errors_count,message=message,state=state)
