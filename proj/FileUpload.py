import os, time, datetime
from flask import Blueprint, request, jsonify, current_app 
from werkzeug import secure_filename
from .ApplicationLog import *

file_upload = Blueprint('file_upload', __name__)

def allowedFile(filename):
	return '.' in filename and \
		filename.rsplit('.',1)[1] in current_app.config['ALLOWED_EXTENSIONS']

@file_upload.route("/upload", methods=["POST"])
def upload():
	errorLog("Function - upload")
	statusLog("Function - upload")
	current_app.config['UPLOAD_FOLDER'] = '/var/www/checker/files'
	current_app.config['ALLOWED_EXTENSIONS'] = set(['csv','xls','xlsx'])
	TIMESTAMP = current_app.timestamp

	# set user variables - otherwise global variables will run endlessly
	#www_log_file = 'http://data.sccwrp.org/checker/logs/%s.log' % TIMESTAMP
	#current_app.log_file = '/var/www/checker/logs/%s.log' % TIMESTAMP

	#errors_dict = {'total': 0, 'mia': 0, 'lookup': 0, 'duplicate': 0, 'logic': 0, 'custom': 0, 'match': 0}
	# all values in errors_dict need to be set to 0 at start of application
	for k,v in errors_dict.iteritems():
		errors_dict[k] = 0
	
	try:
		errorLog(request.path)
		#uploaded_file = request.files['file[]']
		uploaded_files = request.files.getlist('files[]')
		for file in uploaded_files:
			if file and allowedFile(file.filename):
				filename = secure_filename(file.filename)
				# we want to save both the file as it was submitted and a timestamp copy
				# the reason for the timestamp copy is so we dont have duplicate submissions
				# get extension
				extension = filename.rsplit('.',1)[1]
				# join extension with timestamp
				originalfilename = filename
				newfilename = TIMESTAMP + "." + extension
				newcsv = TIMESTAMP + ".csv"
				# return timestamp filename
				gettime = int(time.time())
				tfilename = datetime.datetime.fromtimestamp(gettime)
				humanfilename = tfilename.strftime('%Y-%m-%d') + "." + extension	
				modifiedfilename = "original filename: " + filename + " - new filename: " + newfilename + "(" + humanfilename + ")"
				try:
					# save timestamp file first
					file.save(os.path.join(current_app.config['UPLOAD_FOLDER'], newfilename))
					# then original file
					file.save(os.path.join(current_app.config['UPLOAD_FOLDER'], filename))
					# return timestamp file to web application
					filenames = modifiedfilename
					# return this to internal application
					current_app.infile = "/var/www/checker/files/" + newfilename
					message = newfilename
					state = 0
				except IOError:
					message = "Critical Error: Failed to save file to upload directory!"
					state = 1
		errorLog(message)
	except ValueError:
		state = 1
		message = "Critical Error: Failed to upload file."	
		errorLog(message)
	return jsonify(message=message,state=state,timestamp=TIMESTAMP,original_file=originalfilename,modified_file=newfilename)
