import os, sys, time, datetime, xlrd, urllib, json, collections
#import os, sys, time, datetime, xlrd, unicodecsv, urllib, json, collections
from flask import Flask, url_for, jsonify, request, make_response, session
from flask_session import Session
from flask_cors import CORS, cross_origin
from sqlalchemy import create_engine
#import numpy as np
#import pandas as pd
from ApplicationLog import application_log
from FileUpload import file_upload
from MatchFile import match_file
from CoreChecks import core_checks
from CustomChecks import custom_checks
from TaxonomyChecks import taxonomy_checks
from FinishApp import finish_app
from StagingUpload import staging_upload
from NotificationEmail import notification_email

app = Flask(__name__, static_url_path='/static')
app.debug = True # remove for production

CORS(app)
# does your application require uploaded filenames to be modified to timestamps or left as is
app.config['CORS_HEADERS'] = 'Content-Type'
app.secret_key = 'any random string'
app.infile = ""
# list of database fields that should not be queried on - removed status could be a problem 9sep17 - added trawl calculated fields
app.system_fields = ["id", "objectid", "globalid", "gdb_geomattr_data", "shape", "record_timestamp", "timestamp","errors","lastchangedate","toxbatchrecordid","toxicityresultsrecordid","trawloverdistance","trawldeckdistance","trawldistance","trawlovertime","trawldecktime","trawltimetobottom","trawltime","trawldistancetonominaltarget","picture_url", "coordinates", "device_type", "qcount","created_user","created_date","last_edited_user","last_edited_date","gdb_from_date","gdb_to_date","gdb_archive_oid"]
# set the database connection string, database, and type of database we are going to point our application at
app.eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
app.db = "smcphab"
app.dbtype = "postgresql"
app.status_file = ""
app.timestamp = ""
app.match = ""
app.all_dataframes = collections.OrderedDict()
app.sql_match_tables = []

app.register_blueprint(application_log)
app.register_blueprint(file_upload)
app.register_blueprint(match_file)
app.register_blueprint(core_checks)
app.register_blueprint(custom_checks)
app.register_blueprint(taxonomy_checks)
app.register_blueprint(finish_app)
app.register_blueprint(staging_upload)
app.register_blueprint(notification_email)

import views
