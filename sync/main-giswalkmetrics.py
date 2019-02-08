#!/usr/bin/python
import os, time, datetime
import pymssql
import numpy as np
import pandas as pd
import random
import sys
import smtplib
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from pandas import DataFrame
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate
from os.path import basename

#========================================================================================================================#
#       PURPOSE:       
#
#           To copy new gis crosswalk and metrics data that has been added to existing server (PORTAL) but not to new server (UNIFIEDSMC)                  
#           
#
#
#
#
#       RESOURCES:
#
#           Existing OLDSMC (PORTAL)
#           Server: 192.168.1.8 (portal.sccwrp.org)
#           Database: SMCPHab
#           GIS Tables: luGISStationCodeXwalk and tblGISMetrics
#
#           Future UNIFIEDSMC
#           Database: smc
#           Server: 192.168.1.17 (smcchecker.sccwrp.org)
#           Taxonomic Tables: lu_gisstationcodexwalk and tbl_gismetrics
#
#========================================================================================================================#


# 1ST - INITIALIZE EMAIL MESSAGE AND COLLECT DATA FROM SMC
#
# ACTION: Get new data from OLDSMC destined for UNIFIEDSMC: 
#     1. Purge existing crosswalk records
#     2. Pull down new crosswalk records
#     3. Find lastupdatedate field and compare to todays date
#     4. Get new records
#
# DESCRIPTION:
#     this code looks at the unified taxonomy table in the new SMC database and checks the date of the most recent SMC records. It then compares that date to the date of the most recent
#     records in the old SMCPHab database. If it finds newer records in the SMCPHab database, it will merge those records into the smc database with new appended fields record_origin 
#     and record_publish.



# Initializes Email Message
msgs = ['GIS XWALKMETRICS SYNC SUMMARY:\n']

# connection info
sccwrp_engine = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')

# purge existing backup table
sccwrp_engine.execute("delete from bak_lu_gisstationcodexwalk")
backup_delete_result = sccwrp_engine.execute("select * from bak_lu_gisstationcodexwalk")
backup_result = backup_delete_result.fetchall()
if len(backup_result) == 0:
	print "true"
	# checksum on the production table
	checksum_production_sql = sccwrp_engine.execute("select count(*) from lu_gisstationcodexwalk")
	checksum_production = checksum_production_sql.fetchall()
	# copy production table to backup table
	copy_result = sccwrp_engine.execute("insert into bak_lu_gisstationcodexwalk(select * from lu_gisstationcodexwalk)")
	check_copy = sccwrp_engine.execute("select * from bak_lu_gisstationcodexwalk")
	# checksum on backup of production table
	backup_result = backup_delete_result.fetchall()
	# compare both checksum

	# if equal then clear production
	sccwrp_engine.execute("delete from lu_gisstationcodexwalk")
	production_delete_result = sccwrp_engine.execute("select * from lu_gisstationcodexwalk")
	production_result = production_delete_result.fetchall()
	if len(production_result) == 0:
		print "true"
		# connect to smc luGISStationCodeXwalk using the following query
		smc_engine = create_engine('mssql+pymssql://smc_inquire:$HarP*@192.168.1.8:1433/SMCPHab')
		smc_query = text("SELECT luGISStationCodeXwalk.StationCode, luGISStationCodeXwalk.DatabaseCode, luGISStationCodeXwalk.GISCode FROM luGISStationCodeXwalk ")

		# create a dataframe from all records
		smc_sql = smc_engine.execute(smc_query)
		smc = DataFrame(smc_sql.fetchall())

		smc.columns = smc_sql.keys()
		smc.columns = [x.lower() for x in smc.columns]

		# new field objectid
		last_smc_objid = 0
		smc['objectid'] = smc.index + last_smc_objid + 1

		status = smc.to_sql('lu_gisstationcodexwalk', sccwrp_engine, if_exists='append', index=False)

	else:
		print "false"	
else:
	print "CRITICAL: Something went wrong with deleting records from backup table."


# purge existing backup table
sccwrp_engine.execute("delete from bak_tbl_gismetrics")
backup_delete_result = sccwrp_engine.execute("select * from bak_tbl_gismetrics")
backup_result = backup_delete_result.fetchall()
if len(backup_result) == 0:
	print "true"
	# checksum on the production table
	checksum_production_sql = sccwrp_engine.execute("select count(*) from tbl_gismetrics")
	checksum_production = checksum_production_sql.fetchall()
	# copy production table to backup table
	copy_result = sccwrp_engine.execute("insert into bak_tbl_gismetrics(select * from tbl_gismetrics)")
	check_copy = sccwrp_engine.execute("select * from tbl_gismetrics")
	# checksum on backup of production table
	backup_result = backup_delete_result.fetchall()
	# compare both checksum

	# if equal then clear production
	sccwrp_engine.execute("delete from tbl_gismetrics")
	production_delete_result = sccwrp_engine.execute("select * from tbl_gismetrics")
	production_result = production_delete_result.fetchall()
	if len(production_result) == 0:
		print "true"
		# connect to smc tblGisMetrics using the following query
		smc_engine = create_engine('mssql+pymssql://smc_inquire:$HarP*@192.168.1.8:1433/SMCPHab')
		smc_query = text("SELECT [StationCode],[WgtCode],[Database],[New_Lat],[New_Long],[RegionalBoardNumber],[PSA6c],[PSA9c],[Eco_III_1987],[Eco_III_2010],[Eco_II_1987],[Eco_II_2010],[FlowStatus],[Ag_2000_1K],[Ag_2000_5K],[Ag_2000_WS],[CODE_21_2000_1K],[CODE_21_2000_5K],[CODE_21_2000_WS],[URBAN_2000_1K],[URBAN_2000_5K],[URBAN_2000_WS],[RoadDens_1K],[RoadDens_5K],[RoadDens_WS],[PAVED_INT_1K],[PAVED_INT_5K],[PAVED_INT_WS],[PerManMade_WS],[InvDamDist],[MINES_5K],[GravelMineDensL_R5K],[ELEV_RANGE],[MAX_ELEV],[N_MEAN],[P_MEAN],[PCT_CENOZ],[PCT_NOSED],[PCT_QUART],[PCT_SEDIM],[PCT_VOLCNC],[PPT_00_09],[TEMP_00_09],[NHD_SO],[MAFLOWU],[NHDSLOPE],[FTYPE],[NHDFlow],[Sampled],[BPJ_Nonref],[Active],[AREA_SQKM],[SITE_ELEV],[CaO_Mean],[MgO_Mean],[S_Mean],[UCS_Mean],[LPREM_mean],[AtmCa],[AtmMg],[AtmSO4],[MINP_WS],[MEANP_WS],[SumAve_P],[TMAX_WS],[XWD_WS],[MAXWD_WS],[LST32AVE],[BDH_AVE],[KFCT_AVE],[PRMH_AVE],[CondQR01],[CondQR05],[CondQR25],[CondQR50],[CondQR75],[CondQR95],[CondQR99],[LastUpdateDate],[COMID],[SiteStatus],[Ag_2006_1K],[Ag_2006_5K],[Ag_2006_WS],[CODE_21_2006_1K],[CODE_21_2006_5K],[CODE_21_2006_WS],[URBAN_2006_1K],[URBAN_2006_5K],[URBAN_2006_WS],[Ag_2011_1K],[Ag_2011_5K],[Ag_2011_WS],[CODE_21_2011_1K],[CODE_21_2011_5K],[CODE_21_2011_WS],[URBAN_2011_1K],[URBAN_2011_5K],[URBAN_2011_WS],[PSA10c] FROM [tblGISMetrics]")

		# create a dataframe from all records
		smc_sql = smc_engine.execute(smc_query)
		smc = DataFrame(smc_sql.fetchall())

		smc.columns = smc_sql.keys()
		smc.columns = [x.lower() for x in smc.columns]

		# new field objectid
		last_smc_objid = 0
		smc['objectid'] = smc.index + last_smc_objid + 1

		status = smc.to_sql('tbl_gismetrics', sccwrp_engine, if_exists='append', index=False)

	else:
		print "false"	
else:
	print "CRITICAL: Something went wrong with deleting records from backup table."












