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

