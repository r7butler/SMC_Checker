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
    # It had to do with the GIS Code and StationCode thing. 
    # Every StationCode in the phabmetric dataframe needs to also be in Stations dataframe and vice versa
    
    # process IPI
    errorLog("Processing IPI")
    statusLog("Processing IPI")
    ipi_output = ipi(r_stations, r_pmetrics)
    errorLog("Done processing IPI")
    statusLog("Done processing IPI")
    
    errorLog("Bringing the IPI output back into python world")
    IPI_output = pandas2ri.ri2py(ipi_output)

    return IPI_output
