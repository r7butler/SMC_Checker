from flask import Blueprint, request, jsonify, session
import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine, exc, Table, Column, Integer, Unicode, MetaData, String, Text, update, and_, select, func, types
from sqlalchemy.sql import text
from pandas import DataFrame
from .ApplicationLog import *
import rpy2
import rpy2.robjects as robjects
from rpy2.robjects.packages import SignatureTranslatedAnonymousPackage
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
import pandas_access as mdb
import re
from datetime import datetime
from PhabConvert import phabconvert

PHABMetrics = importr("PHABMetrics")
phabmetrics = PHABMetrics.phabmetrics
PHAB = importr("PHAB")
ipi = PHAB.IPI
r = robjects.r

as_numeric = r['as.numeric']
as_integer = r['as.integer']
as_character = r['as.character']
as_POSIXct = r['as.POSIXct']

def addErrorToList(error_column, row, error_to_add,df):
	df.ix[int(row), 'row'] = str(row)
	if error_column in df.columns:
		# check if cell value is empty (nan) 
		if(pd.isnull(df.ix[int(row), error_column])):
			# no data exists in cell so add error
	      		df.ix[int(row), error_column] = error_to_add
			errorLog("addErrorToList New Row: %s, Error To Add: %s" % (int(row),error_to_add))
		else:
			# a previous error was recorded so append to it
			# even though there may be data lets check to make sure it is not empty
			if str(df.ix[int(row), error_column]):
				#print("There is already a previous error recorded: %s" % str(df.ix[int(row), error_column]))
				df.ix[int(row), error_column] = str(df.ix[int(row), error_column]) + "," + error_to_add
				errorLog("addErrorToList Existing Row: %s, Error To Add: %s" % (int(row),error_to_add))
			else:
				#print("No error is recorded: %s" % str(df.ix[int(row), error_column]))
	      			df.ix[int(row), error_column] = error_to_add
				errorLog("addErrorToList Row: %s, Error To Add: %s" % (int(row),error_to_add))
	else:
		df.ix[int(row), error_column] = error_to_add
		errorLog("addErrorToList Add New Column and New Row: %s, Error To Add: %s" % (int(row),error_to_add))
	return df

def getCustomErrors(df,name,warn_or_error):
	errorLog("start getCustomErrors")
	errorLog(name)
	# get name of dataframe
	tab_value = name.strip().split(" - ")
	tab = tab_value[0]
	# get tab number of dataframe batch = 0, result = 1, wq = 2
	#errorLog(tab[0])
	#errorLog(list(df))
	#  clear dataframe of rows that have no errors
	dfjson = df
        dfjson = dfjson[pd.notnull(dfjson[warn_or_error])]
	#errorLog(dfjson)
        #dfjson = dfjson[pd.notnull(dfjson['toxicity_errors'])]
        # must re-index dataframe - set to 0 after removing rows
	# not necessary with custom errors only regular
	#dfjson.reset_index(drop=True,inplace=True) 
	tmp_dict = {}
	count = 0
	# Critical for custom checks we look at row instead of tmp_row
	# something must be wrong with code row seems to work with toxicity and summary checks, but tmp_row fails to work properly with summary (duplicates)
	for index, row in dfjson.iterrows():
		# delete - errorLog("count: %s - row: %s value: %s" % (count,row['tmp_row'],row['custom_errors']))
		# delete - tmp_dict[tab] = '[{"count":"%s","row":"%s","value":[%s]}]' % (count,row['tmp_row'],row['custom_errors'])
		tabcount = tab + "-" + str(count)
		tmp_dict[tabcount] = '[{"row":"%s","value":[%s]}]' % (row['row'],row[warn_or_error])
		errorLog("row: %s, value: %s" % (row['row'],row[warn_or_error]))
		# delete - tmp_dict[tabcount] = '[{"row":"%s","value":[%s]}]' % (row['tmp_row'],row['custom_errors'])
		count = count + 1
	errorLog("end getCustomErrors")
	return tmp_dict

def getCustomRedundantErrors(df,name,check):
	errorLog("start getCustomRedundantErrors")
	#errorLog("check: %s" % check)
	# get name of dataframe
	tab_value = name.strip().split(" - ")
	# get tab number of dataframe batch = 0, result = 1, wq = 2
	tab = tab_value[0]
	tmp_dict = {}
	count = 0
	for error_message,group in df.groupby(check):
		# only return errors if there are more one (redundant)
		#errorLog("grouped rows count: %s" % len(group.row))
		if len(group.row) > 1:
			row_fix = []
			for r in group.row:
				row_fix.append(str(int(r) + 2))
			rows = ', '.join(row_fix)
			errorLog('[{"rows":"%s","value":[%s]}]' % (rows,error_message))
			tabcount = tab + "-" + str(count)
			tmp_dict[tabcount] = '[{"rows":"%s","value":[%s]}]' % (rows,error_message)
		count = count + 1
	errorLog("end getCustomRedundantErrors")
	return tmp_dict

def dcValueAgainstMultipleValues(eng,dbtable,dbfield,df,field):
        # codes_df: dataframe of valid codes according to database
        codes = eng.execute("select " + dbfield + " from " + dbtable +";")
        codes_df = pd.DataFrame(codes.fetchall())
        codes_df.columns = codes.keys()
        
        # subcodes: submitted codes for specified field
        subcodes = df[[field,'tmp_row']]
	subcodes[field].replace(r'^$',np.nan,regex=True,inplace = True)
        # check submitted data for at least one code
        #nan_rows = subcodes.loc[subcodes[field]==''].tmp_row.tolist()
	nan_rows = subcodes.loc[subcodes[field].isnull()].tmp_row.tolist()
	
        # check submitted data for invalid codes
	db_list = codes_df[dbfield].apply(lambda row: "".join(row.split()).lower()).tolist()
        #subcodes['check'] = subcodes[field][subcodes[field] != ""].apply(lambda row: set("".join(row.split()).lower().split(',')).issubset(db_list))
        subcodes['check'] = subcodes[field].dropna().apply(lambda row: set("".join(row.split()).lower().split(',')).issubset(db_list))
	invalid_codes = df.loc[subcodes.check == False].tmp_row.tolist()
	return nan_rows, invalid_codes, subcodes


def python_phabmetrics(infile, login_info, excel = False):
        '''This function processes PHAB Metrics and does preprocessing for IPI'''

	errorLog("Function - python_phabmetrics")
	message = "Getting ready to process PHAB Metrics."
	statusLog("Getting ready to process PHAB Metrics")
	errorLog(message)
        
	TIMESTAMP=str(session.get('key'))

        if excel == False:
            original_rawdata = phabconvert(infile, login_info)
        else:
            original_rawdata = pd.read_excel(infile, sheet_name = 0)
        
        # original rawdata is to be saved and returned in all_dataframes
        # 'rawdata' below is going to be processed
        rawdata = original_rawdata

            
        ##### PRE-PROCESSINGS  FOR R PHABMETRICS
        errorLog("Pre-Processing Data for PHAB Metrics...")
        statusLog("Pre-Processing Data for PHAB Metrics")


        # Camel-case field names for integration with PHAB Metrics
        errorLog("renaming the columns to camel case")
        rawdata = rawdata.rename(index=str, columns = {'stationcode':'StationCode','sampledate':'SampleDate','replicate':'Replicate','locationcode':'LocationCode','analytename':'AnalyteName','unitname':'UnitName','variableresult':'VariableResult','fractionname':'FractionName','result':'Result','resqualcode':'ResQualCode','qacode':'QACode'})

        # add id field for integration with PHAB Metrics
        errorLog("adding id field for integration with phabmetrics function")
        rawdata['id'] = rawdata.StationCode

        # collect approprate data from the tables
        rawdata = rawdata[['StationCode', 'SampleDate', 'Replicate', 'LocationCode', 'AnalyteName', 'UnitName', 'VariableResult', 'FractionName', 'Result', 'ResQualCode', 'QACode', 'id']]
        
        


        # Convert the rawdata DataFrame to an rpy2.robjects 'data.frame'
        errorLog('converting the rawdata to the R format so that it can be put into the phabmetrics function')
        r_rawdata = pandas2ri.py2ri(rawdata)

        # We must ensure that rpy2 knows the data types of each column in "R World"
        r_rawdata[0] = as_character(r_rawdata[0])
        r_rawdata[1] = as_character(r_rawdata[1])
        r_rawdata[2] = as_integer(r_rawdata[2])
        r_rawdata[3] = as_character(r_rawdata[3])
        r_rawdata[4] = as_character(r_rawdata[4])
        r_rawdata[5] = as_character(r_rawdata[5])
        r_rawdata[6] = as_character(r_rawdata[6])
        r_rawdata[7] = as_character(r_rawdata[7])
        r_rawdata[8] = as_numeric(r_rawdata[8])
        r_rawdata[9] = as_character(r_rawdata[9])
        r_rawdata[10] = as_character(r_rawdata[10])
        r_rawdata[11] = as_character(r_rawdata[11])
        
        
        errorLog("The R version of the raw phab data:\n")
        robjects.globalenv['rawdata'] = r_rawdata
        errorLog(r('head(rawdata)'))


        #!!--- process PHABMetrics ---!!#
        errorLog("phabmetrics function is running")
        statusLog("Processing PHab Metrics")
        pmetrics_wide = phabmetrics(r_rawdata)
        errorLog("phabmetrics function completed successfully")
        statusLog("Done processing PHab Metrics")

        # get the output from phabmetrics function back into a pandas DataFrame
        errorLog("converting the output back to python format")
        pmetrics_wide = pandas2ri.ri2py(pmetrics_wide)
        
        errorLog("Here is the head of the output of the phabmetrics function")
        errorLog(pmetrics_wide.head())
        
        
        # Below is the code to process IPI.
        
        #-----------------------------------------------------------------------------------#
        # Now getting ready to process ipi scores                                           #
        #                                                                                   #
        # This is the part where we seaparate stations with GIS Data and those that don't   #
        #-----------------------------------------------------------------------------------#
        errorLog("Getting ready to process IPI")
        statusLog("Getting ready to process IPI")
        
        # ------------------------------------------------------------------------------------------------- #
                                                                                                            
        # Convert the output from the phabmetrics function into the necessary format for the IPI function   #
        # Below, we will attempt to accomplish this with the pandas library                                 #
                                                                                                            
        # ------------------------------------------------------------------------------------------------- #
        errorLog("Converting output from phabmetrics function to long format")

        # pmetrics_wide is the raw ouput from phabmetrics function
        # remember that the stationcode column in that dataframe is a concatenation of StationCode and SampleDate

        # These next few lines of code parse the 'StationCode' column, 
        # which is actually a concatenation of StationCode and SampleDate with an underscore in between
        # I am also lowercasing the column names to help it work more fluidly with the tables coming from the database
        errorLog("Parsing StationCode column")
        pmetrics_wide['stationcode'] = pmetrics_wide['StationCode'].apply(lambda x: x.split('_'))
        errorLog(pmetrics_wide[['StationCode', 'stationcode']].head())
        pmetrics_wide.drop('StationCode', axis = 1, inplace = True)

        # since we did a .split("_") on every item in that column, each item in that column is now a list
        # The first item in that list is the StationCode, and the second item is the SampleDate
        errorLog("Splitting StationCode column into StationCode and SampleDate")
        errorLog("Creating sampledate column")
        pmetrics_wide['sampledate'] = [x[1] for x in pmetrics_wide.stationcode]
        errorLog("Creating stationcode column")
        pmetrics_wide['stationcode'] = [x[0] for x in pmetrics_wide.stationcode]
        errorLog(pmetrics_wide[['stationcode', 'sampledate']].head())

        # Now get the SampleDates as string objects in the correct format for IPI
        errorLog("Converting sampledate column from datetime to strings")
        pmetrics_wide['sampledate'] = pmetrics_wide['sampledate'].apply(lambda x: datetime.strptime(x.split()[0], "%Y-%m-%d").strftime("%m/%d/%Y"))
        errorLog(pmetrics_wide.sampledate.head())
       
        # Now we have to rename the columns to CamelCase.
        pmetrics_wide.rename(columns={'stationcode': 'StationCode', 'sampledate': 'SampleDate'}, inplace = True)

        
        errorLog("Converting the output of the phabmetrics function to long format")
        # It may be helpful to keep in mind that the 'StationCode' Column in the phabmetrics output is actually a concatenation of the stationcode and sampledate
        # also, here, phabmetrics_wide is the raw output from the phabmetrics function

        # Get lists of column names that have 'result' and 'count' respectively
        resultcolumns = [col for col in pmetrics_wide.columns if 'result' in col]
        countcolumns = [col for col in pmetrics_wide.columns if 'count' in col]

        # get the long format of the result value and count value of the variable
        resultdf = pd.melt(pmetrics_wide, id_vars = ['StationCode', 'SampleDate'], value_vars = resultcolumns, var_name = 'Variable', value_name = 'Result')
        errorLog(resultdf.head())
        countdf = pd.melt(pmetrics_wide, id_vars = ['StationCode', 'SampleDate'], value_vars = countcolumns, var_name = 'Variable', value_name = 'Count_Calc')
        errorLog(countdf.head())

        # strip the '.result' and '.count' from the variable columns of the resultdf and countdf
        resultdf['Variable'] = resultdf['Variable'].apply(lambda x: re.sub("\.result", "", x))
        countdf['Variable'] = countdf['Variable'].apply(lambda x: re.sub("\.count", "", x))

        # merge the result DataFrame with the count DataFrame
        pmetrics_long = pd.merge(resultdf, countdf, on = ['StationCode', 'SampleDate', 'Variable'], how = 'outer')

        # rename it to pmetrics
        # commenting this out because it may be using too much memory
        #pmetrics = pmetrics_long
        errorLog("Done converting output of phabmetrics function")            
        

        errorLog("Here is the head of the phabmetrics output in long format:")
        errorLog(pmetrics_long.head())
        
        
        # Now we generate the report and write it to a csv
        errorLog("writing phabmetrics dataframe to csv")
        pmetricpath = '/var/www/smc/logs/%s.phabmetrics.csv' % TIMESTAMP
        # Later to be served up to the user for download
        pmetrics_long.to_csv(pmetricpath, sep=',', mode='a', encoding='utf-8', index=False)
        




        
        # -----  Separate stations with GIS Data from those that don't ----- #
        errorLog("Separating stations with GIS Data from those that don't")

        # original submitted stations
        errorLog("Getting list of unique stations that were submitted")
        list_of_original_unique_stations = pd.unique(pmetrics_long['StationCode'])

        errorLog("List of original unique stations:")
        errorLog(list_of_original_unique_stations)


        # this is for the sql statement
        errorLog("Preparing the SQL statement for query")
        unique_original_stations = ', '.join("'" + s + "'" for s in list_of_original_unique_stations)

        # Connect to database
        errorLog("Connecting to the database")
        eng = create_engine("postgresql://sde:dinkum@192.168.1.17:5432/smc")

        # get necessary columns from the crosswalk table
        # Stations that do not have GIS Data will not appear in this table
        errorLog("Collecting GIS codes of the stations that were submitted")
        xwalk_execution = eng.execute("SELECT stationcode, giscode FROM lu_newgisstationcodexwalk WHERE stationcode IN (%s);" % unique_original_stations)
        xwalk = pd.DataFrame(xwalk_execution.fetchall())
        xwalk.columns = xwalk_execution.keys()

        errorLog("Here is the xwalk dataframe:")
        errorLog(xwalk)


        # Get a list of giscodes of our submitted stations, that is, the stations that have GIS Data
        gis_stations_giscodes = list(xwalk.giscode)

        # this is just for the SQL statement
        gis_stations_giscodes_str = ', '.join(["'" + s + "'" for s in gis_stations_giscodes])
        
        
        # SQL statement to get GIS Data for our stations table
        errorLog("Collecting GIS Data")
        stationsql = text("SELECT "
                            "tbl_newgismetrics.stationcode AS giscode, "
                            "tbl_newgismetrics.max_elev, tbl_newgismetrics.area_sqkm, "
                            "tbl_newgismetrics.elev_range, tbl_newgismetrics.meanp_ws, "
                            "tbl_newgismetrics.new_long, tbl_newgismetrics.site_elev, "
                            "tbl_newgismetrics.kfct_ave, tbl_newgismetrics.new_lat, "
                            "tbl_newgismetrics.minp_ws, tbl_newgismetrics.ppt_00_09 "
                        "FROM tbl_newgismetrics "
                        "WHERE tbl_newgismetrics.stationcode IN (%s);" % gis_stations_giscodes_str)

        # Build the stations table
        stations_execution = eng.execute(stationsql)
        stations = pd.DataFrame(stations_execution.fetchall())
        stations.columns = stations_execution.keys()

        # add the true stationcode column (stationcode here does not refer to giscode)
        stations = pd.merge(xwalk, stations, on = 'giscode')
        
        # In stations dataframe: drop 'giscode' column.
        stations.drop('giscode', axis = 1, inplace = True)
        stations.rename(columns={'stationcode': 'StationCode'}, inplace = True)

        # get the missing stations (stations without GIS Data)
        errorLog("Creating list of stations with missing GIS Data")
        missing_stations = set(list_of_original_unique_stations) - set(stations.StationCode)
        errorLog("Stations missing GIS Data:")
        errorLog(missing_stations)
        
        # in pmetrics_wide table: get rid of rows that have stations with no GIS Data
        errorLog("dropping stations that are missing GIS Data from the phabmetrics dataframe")
        pmetrics_long = pmetrics_long[~pmetrics_long.StationCode.isin(missing_stations)]
        errorLog("pmetrics_long AFTER dropping stations with missing GIS Data")


        # In stations DataFrame: rename all the rest of the columns to match what it looks like on the github SCCWRP/PHAB site
        errorLog("Renaming columns of the stations dataframe")
        stations.rename(columns={'max_elev': 'MAX_ELEV', 'area_sqkm': 'AREA_SQKM', 'elev_range': 'ELEV_RANGE', 'meanp_ws': 'MEANP_WS', 'new_long': 'New_Long', 'site_elev': 'SITE_ELEV', 'kfct_ave': 'KFCT_AVE', 'new_lat': 'New_Lat', 'minp_ws': 'MINP_WS', 'ppt_00_09': 'PPT_00_09'}, inplace = True)



        
        # These queries are here for the sake of building the match_tables variable
        phabtable = pd.read_sql("SELECT * FROM tbl_phab LIMIT 1", eng)
        phabcolumnsmatched = len(phabtable.columns) - 1
        phabmetricstable = pd.read_sql("SELECT * FROM tmp_phabmetrics LIMIT 1", eng)
        phabmetricscolumnsmatched = len(phabmetricstable.columns) - 1


        all_dataframes = {'0 - tblPhab - tbl_phab': original_rawdata}
        sql_match_tables = ['tbl_phab']
        match_tables = ["1-tblPhab-2-tbl_phab-True-" + str(phabcolumnsmatched) + "-" + ','.join(phabtable.columns) + "-phab"]

        return all_dataframes, sql_match_tables, match_tables, pmetrics_long, stations, unique_original_stations



def python_IPI(pmetrics_long, stations):
    

    # ---------------------------------------------- #

    # Here is the part where we actually process IPI #

    # ---------------------------------------------- #

    errorLog("Bringing the stations dataframe to R world")            
    # convert stations DataFrame to an rpy2.robjects 'data.frame' and store it in the rpy2 robjects global environment
    r_stations = pandas2ri.py2ri(stations)
    rpy2.robjects.globalenv['stations'] = r_stations

    # convert pmetrics_long DataFrame to an rpy2.robjects 'data.frame' and store it in the rpy2 robjects global enironment
    errorLog("Bringing phabmetrics output back into R world in the long format")
    r_pmetrics = pandas2ri.py2ri(pmetrics_long)
    rpy2.robjects.globalenv['pmetrics'] = r_pmetrics

    errorLog("Here is the head of the phabmetrics DataFrame in R world:")
    errorLog(r('head(pmetrics)'))

    # Here we convert the datatypes to what they need to be in the "R World"
    errorLog("Converting Datatypes of the stations dataframe")
    errorLog("Converting StationCode column")
    r_stations[0] = as_character(r_stations[0]) # StationCode
    errorLog("Converting MAX_ELEV column")
    r_stations[1] = as_integer(r_stations[1])   # MAX_ELEV
    errorLog("Converting AREA_SQKM column")
    r_stations[2] = as_numeric(r_stations[2])   # AREA_SQKM
    errorLog("Converting ELEV_RANGE column")
    r_stations[3] = as_numeric(r_stations[3])   # ELEV_RANGE
    errorLog("Converting MEANP_WS column")
    r_stations[4] = as_numeric(r_stations[4])   # MEANP_WS
    errorLog("Converting New_Long column")
    r_stations[5] = as_numeric(r_stations[5])   # New_Long
    errorLog("Converting SITE_ELEV column")
    r_stations[6] = as_numeric(r_stations[6])   # SITE_ELEV
    errorLog("Converting KFC_AVE column")
    r_stations[7] = as_numeric(r_stations[7])   # KFCT_AVE
    errorLog("Converting New_Lat column")
    r_stations[8] = as_numeric(r_stations[8])   # New_Lat
    errorLog("Converting MINP_WS column")
    r_stations[9] = as_numeric(r_stations[9])   # MINP_WS
    errorLog("Converting PPT_00_09 column")
    r_stations[10] = as_numeric(r_stations[10]) # PPT_00_09

    errorLog("Converting Datatypes of the phabmetrics output dataframe")
    errorLog("Converting StationCode column")
    r_pmetrics[0] = as_character(r_pmetrics[0]) # StationCode
    errorLog("Converting SampleDate column")
    r_pmetrics[1] = as_character(r_pmetrics[1]) # SampleDate
    errorLog("Converting Variable column")
    r_pmetrics[2] = as_character(r_pmetrics[2]) # Variable
    errorLog("Converting Result column")
    r_pmetrics[3] = as_numeric(r_pmetrics[3])   # Result
    errorLog("Converting Count_Calc column")
    r_pmetrics[4] = as_integer(r_pmetrics[4])   # Count_Calc

    errorLog("stations:")
    errorLog(r('head(stations)'))
    errorLog('\nphabmetrics:\n')
    errorLog(r('head(pmetrics)'))
    
    # Couldn't figure out the bug to fix IPI...
    
    # process IPI
    errorLog("Processing IPI")
    statusLog("Processing IPI")
    ipi_output = ipi(r_stations, r_pmetrics)
    errorLog("Done processing IPI")
    statusLog("Done processing IPI")
    
    errorLog("Bringing the IPI output back into python world")
    IPI_output = pandas2ri.ri2py(ipi_output)

    return IPI_output






phab_checks = Blueprint('phab_checks', __name__)

@phab_checks.route("/phab", methods=["POST"])

def phab(all_dataframes,pmetrics_long,stations,sql_match_tables,errors_dict,project_code,login_info):
	errorLog("Function - phab")
	message = "Custom PHAB: Start checks."
	statusLog("Starting PHAB Checks")
	errorLog(message)
	errorLog("project code: %s" % project_code)

        errorLog(login_info)
	login_info = login_info.strip().split("-")
	login = str(login_info[0])
	agency = str(login_info[1])
	owner = str(login_info[2])
	year = str(login_info[3])
	project = str(login_info[4])

        
        assignment_table = ""
        custom_checks = ""
        summary_checks = ""
        summary_results_link = ""
        custom_redundant_checks = ""
        custom_errors = []
        custom_warnings = []
        custom_redundant_errors = []
        custom_redundant_warnings = []

	TIMESTAMP=str(session.get('key'))
	errorsCount(errors_dict,'custom')
        

        try:
            
            # Run Custom Checks here
            
            if custom_errors == []:
                # Run IPI Here
                ipi_output = python_IPI(pmetrics_long, stations)
                message = "IPI ran successfully"

            
            
            for dataframe in all_dataframes.keys():
                    if 'custom_errors' in all_dataframes[dataframe]:
                            custom_errors.append(getCustomErrors(all_dataframes[dataframe],dataframe,'custom_errors'))
                            custom_redundant_errors.append(getCustomRedundantErrors(all_dataframes[dataframe],dataframe,"custom_errors"))
                    if 'custom_warnings' in all_dataframes[dataframe]:
                            errorLog("custom_warnings")
                            custom_errors.append(getCustomErrors(all_dataframes[dataframe],dataframe,'custom_warnings'))
                            errorLog(custom_warnings)
                            custom_redundant_errors.append(getCustomRedundantErrors(all_dataframes[dataframe],dataframe,"custom_warnings"))
            custom_checks = json.dumps(custom_errors, ensure_ascii=True)
            custom_redundant_checks = json.dumps(custom_redundant_errors, ensure_ascii=True)
            ## END RETRIEVE ERRORS ##
            # get filenames from fileupload routine
            errorLog(message)
            #assignment_table = result.groupby(['stationid','lab','analyteclass']).size().to_frame(name = 'count').reset_index()
            # lets reassign the analyteclass field name to species so the assignment query will run properly - check StagingUpload.py for details
            #assignment_table = assignment_table.rename(columns={'analyteclass': 'species'})
            summary_results_link = TIMESTAMP
            statusLog("Finalizing Report")



            return all_dataframes, ipi_output, assignment_table, custom_checks, custom_redundant_checks, summary_checks, summary_results_link, message

        except Exception as e:
            message = "Critical Error: Failed to run phab checks"	
            errorLog(message)
            errorLog(e)
            state = 1
            return jsonify(message=message,state=state)
















# ----------------------- #
#       CODE ARCHIVE
# ----------------------- #


# Block of code below is commented out because it may be useful later
# this was the way we had the thing running before to process PHabmetrics, where we had field and habitat separate until the last minute
# Now we are doing it a bit different, where we have that huge 92 column dataframe nad extract certain fields from it for the sake if running phabmetrics
'''
##### PRE-PROCESSINGS  FOR R PHABMETRICS
errorLog("Pre-Processing Data for PHAB Metrics...")

# Camel-case field names for integration with PHAB Metrics
habitat_query = habitat_query.rename(index=str, columns = {'stationcode':'StationCode','sampledate':'SampleDate','replicate':'Replicate','locationcode':'LocationCode','analytename':'AnalyteName','unitname':'UnitName','variableresult':'VariableResult','fractionname':'FractionName','result':'Result','resqualcode':'ResQualCode','qacode':'QACode'})

# add id field for integration with PHAB Metrics
habitat_query['id'] = habitat_query.StationCode

# Camel-case field_query fields for integration with PHAB Metrics
field_query = field_query.rename(index=str, columns = {'stationcode':'StationCode','sampledate':'SampleDate','replicate':'Replicate','locationcode':'LocationCode','analytename':'AnalyteName','unitname':'UnitName','variableresult':'VariableResult','fractionname':'FractionName','result':'Result','resqualcode':'ResQualCode','qacode':'QACode'})

# add id field for integration with PHAB Metrics
field_query['id'] = field_query.StationCode

# collect approprate data from the tables
habitat_rawdata = habitat_query[['StationCode', 'SampleDate', 'Replicate', 'LocationCode', 'AnalyteName', 'UnitName', 'VariableResult', 'FractionName', 'Result', 'ResQualCode', 'QACode', 'id']]
field_rawdata = field_query[['StationCode', 'SampleDate', 'Replicate', 'LocationCode', 'AnalyteName', 'UnitName', 'FractionName', 'Result', 'ResQualCode', 'QACode', 'id']]

# Merge habitat and field data to get a complete table ready to input to phabmetrics function
rawdata = habitat_rawdata.merge(field_rawdata, on = ['StationCode', 'SampleDate', 'Replicate', 'LocationCode', 'AnalyteName', 'UnitName', 'FractionName', 'Result', 'ResQualCode', 'QACode', 'id'], how='outer')

# Change value Length Reach to Length, Reach (must be done to continue)
rawdata.AnalyteName = rawdata['AnalyteName'].apply(lambda x: "Length, Reach" if x == "Length Reach" else x)
errorLog("Finished Pre-Processing Data for PHAB Metrics.")
##### END PRE-PROCESSING FOR R PHABMETRICS
'''
# Commented out for now. We will try to convert datatypes exclusively in the R world and see if that works.
'''
errorLog("Filling NA values with approriate values")
rawdata.Result.fillna(-88, inplace = True)

errorLog(rawdata)
errorLog(rawdata.Result)
errorLog("converting data types")
# Data type conversions, this part may not be necessary, but if its not broken, I don't want to try to fix it
errorLog("converting Result")
rawdata.Result = rawdata.Result.astype(float) # good
errorLog("converting Replicate")
rawdata.Replicate = rawdata.Replicate.astype(int) # good
errorLog("converting VariableResult")
rawdata['VariableResult'] = rawdata['VariableResult'].astype(str) # good
errorLog("converting StationCode")
rawdata['StationCode'] = rawdata['StationCode'].astype(str) # good
errorLog("converting LocationCode")
rawdata['LocationCode'] = rawdata['LocationCode'].astype(str) # good
errorLog("converting AnalyteName")
rawdata['AnalyteName'] = rawdata['AnalyteName'].astype(str) # good
errorLog("converting UnitName")
rawdata['UnitName'] = rawdata['UnitName'].astype(str) # good
errorLog("converting FractionName")
rawdata['FractionName'] = rawdata['FractionName'].astype(str) # good
errorLog("converting ResQualCode")
rawdata['ResQualCode'] = rawdata['ResQualCode'].astype(str) # good
errorLog("converting QACode")
rawdata['QACode'] = rawdata['QACode'].astype(str) # good
errorLog("converting id")
rawdata['id'] = rawdata['id'].astype(str) # good
#rawdata['Result'] = rawdata['Result'].fillna(0)
'''

# pandas2ri.activate() was slowing the code down a lot so we commented it out. Apparently it is not even necessary for what we are doing.
# 22 Feb 2019 uncommented for purpose of experimenting
# 25 Feb 2019 re commented out, also for experimenting to see if IPI will run with it deactivated
#errorLog("activating pandas2ri")
#pandas2ri.activate()
