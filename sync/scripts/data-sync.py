import os, time,datetime
import arcpy
import pymssql
import pandas
from arcpy import env
from pandas import DataFrame
from sqlalchemy import create_engine
import psycopg2


##### REQUIRED VARIABLES - YOU MUST SET ####
# original table name
table_name = "tblTimeSeriesResults"
# new table name
fc_name = "tmp_timeseriesresults"
