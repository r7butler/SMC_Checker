from flask import Blueprint, request, jsonify
from .ApplicationLog import *

custom_checks = Blueprint('custom_checks', __name__)

@custom_checks.route("/custom", methods=["POST"])
def custom():
	print("Function - custom")
	# first we need to find out what type of custom checks we are doing
	# toxicity requires three matching tabs - batch,result,wq
	# if there arent three then bounce
	# try: call ToxicityChecks
	# try: call FishChecks
	# first we need to get the global list of matching tables that user has submitted 
	message = ""
	match = ""
	sql_match_tables = current_app.sql_match_tables
	errorLog("Custom: sql_match_tables: %s" % sql_match_tables)
	# dictionary list of required tables by data type
	required_tables_dict = {'toxicity': ['tbl_toxbatch','tbl_toxresults','tbl_toxwq'],'fish': ['tbltrawlfishabundance','tblfishbiomass'],'csci': ['tbl_taxonomysampleinfo','tbl_taxonomyresults']}
	for k,v in required_tables_dict.items():
		if set(sql_match_tables) == set(v):
			message = "Custom: Found exact match: %s, %s" % (k,v)
			match = k
			current_app.match = k
	try:
		if match:
			errorLog(message)
			state = 0
		else:
			message = "Critical Error: You lack the required excel tabs to run custom checks: %s" % (sql_match_tables)
			errorLog(message)
			state = 1
	except ValueError:
		message = "Critical Error: Failed to run CustomChecks routine."	
		errorLog(message)
		state = 1
	return jsonify(message=message,state=state,match=match)
