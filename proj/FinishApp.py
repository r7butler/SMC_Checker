from flask import Blueprint, request, jsonify
import pandas as pd
import json
import xlsxwriter
from .ApplicationLog import *

finish_app = Blueprint('finish_app', __name__)

@finish_app.route("/finish", methods=["POST"])
def finish():
	errorLog("Function - finish")
	TIMESTAMP = current_app.timestamp
	excel_file = '/var/www/smc/logs/%s-format.xlsx' % TIMESTAMP
	excel_link = 'http://checker.sccwrp.org/smc/logs/%s-format.xlsx' % TIMESTAMP
	all_dataframes = current_app.all_dataframes
	errorLog(all_dataframes.keys())
	try:
		# get filenames from fileupload routine
		message = "Start finish..."	
		errorLog(message)


		# Create a Pandas Excel writer using XlsxWriter as the engine.
		writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')
		#workbook = writer.book	
		for dataframe in all_dataframes.keys():
			df_sheet_and_table_name = dataframe.strip().split(" - ")
			table_name = str(df_sheet_and_table_name[2])
			errorLog("Loading: %s" % table_name)
			statusLog("Loading: %s" % table_name)

			# Create a Pandas dataframe from the data.
			df = all_dataframes[dataframe]
			if 'errors' in df:
				errorLog(df['errors'])
			#df = pd.DataFrame({'Data': [10, 20, 30, 20, 15, 30, 45]})

			# create worksheet in writer object
			df.to_excel(writer, sheet_name=table_name)
			# now create worksheet object
			worksheet = writer.sheets[table_name]
			errorLog("worksheet")

			# Convert the dataframe to an XlsxWriter Excel object.
			# Apply a conditional format to the cell range.
			worksheet.conditional_format('O2:O8', {'type': '3_color_scale'})
			errorLog("df.to_excel")

		# Close the Pandas Excel writer and output the Excel file.
		writer.save()

		state = 0
	except ValueError:
		message = "Critical Error: Failed to run finish"	
		data=message
		errorLog(message)
		state = 1
	errors_count = json.dumps(errors_dict)
	return jsonify(errors=errors_count,message=message,state=state,excel=excel_link)
