import datetime
import pandas as pd
import pandas_access as mdb
import datetime
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.types import INTEGER, VARCHAR, TIMESTAMP, SMALLINT, NUMERIC

# First
def errorLog(x):
    print(x)

'''
infile = "test.mdb"
project = 'test'
login = 'test login'
agency = 'test agency'
owner = 'test owner'
year = 'test year'
'''
#### ALL OF THE CODE BELOW NEEDS TO GET LOADED INTO A SEPARATE FILE ####
# two new dataframes field and habitat - get loaded into raw unified phab table
def phabconvert(infile, login_info='test-test-test-test-test'):
    
    login_info = login_info.strip().split("-")
    login = str(login_info[0])
    agency = str(login_info[1])
    owner = str(login_info[2])
    year = str(login_info[3])
    project = str(login_info[4])

    errorLog("READ IN TABLES REQUIRED TO RUN QUERY:")
    sample_entry = mdb.read_table(infile, "Sample_Entry", dtype={'s_Generation':str})
    errorLog(sample_entry)
    event_lookup = mdb.read_table(infile, "EventLookUp")
    protocol_lookup = mdb.read_table(infile, "ProtocolLookUp")
    station_lookup = mdb.read_table(infile, "StationLookUp", dtype={'s_Generation':str})
    agency_lookup = mdb.read_table(infile, "AgencyLookUp", dtype={'s_Generation':str})
    project_lookup = mdb.read_table(infile, "ProjectLookUp", dtype={'s_Generation':str})
    qa_lookup = mdb.read_table(infile, "QALookUp", dtype={'s_Generation':str})
    resqual_lookup = mdb.read_table(infile, "ResQualLookUp", dtype={'s_Generation':str})
    stationdetail_lookup = mdb.read_table(infile, "StationDetailLookUp", dtype={'s_Generation':str})
    location_entry = mdb.read_table(infile, "Location_Entry", dtype={'s_Generation':str})
    location_lookup = mdb.read_table(infile, "LocationLookUp", dtype={'s_Generation':str})
    parent_project_lookup = mdb.read_table(infile, "ParentProjectLookUp", dtype={'s_Generation':str})


    collectionmethod_lookup = mdb.read_table(infile, "CollectionMethodLookUp", dtype={'s_Generation':str})
    constituent_lookup = mdb.read_table(infile, "ConstituentLookUp", dtype={'s_Generation':str})
    matrix_lookup = mdb.read_table(infile, "MatrixLookUp", dtype={'s_Generation':str})
    method_lookup = mdb.read_table(infile, "MethodLookUp", dtype={'s_Generation':str})
    analyte_lookup = mdb.read_table(infile, "AnalyteLookUp", dtype={'s_Generation':str})
    unit_lookup = mdb.read_table(infile, "UnitLookUp", dtype={'s_Generation':str})
    fraction_lookup = mdb.read_table(infile, "FractionLookUp", dtype={'s_Generation':str})
    collectiondevice_lookup = mdb.read_table(infile, "CollectionDeviceLookUp", dtype={'s_Generation':str})

    compliance_lookup = mdb.read_table(infile, "ComplianceLookUp", dtype={'s_Generation':str}) 
    batchverification_lookup = mdb.read_table(infile, "BatchVerificationLookUp", dtype={'s_Generation':str})

    ##### field specific tables
    fieldcollection_entry = mdb.read_table(infile, "FieldCollection_Entry", dtype={'s_Generation':str})
    fieldresult_entry = mdb.read_table(infile, "FieldResult_Entry", dtype={'s_Generation':str})

    #### habitat specific tables
    habitatcollection_entry = mdb.read_table(infile, "HabitatCollection_Entry", dtype={'s_Generation':str})
    habitatresult_entry = mdb.read_table(infile, "HabitatResult_Entry", dtype={'s_Generation':str})

    errorLog("STARTING MERGING TOGETHER FIELDS - IE. RUNNING QUERY")
    ### pull together sample and location data - used by both field and habitat queries
    sample = pd.merge(sample_entry[['AgencyCode','EventCode','ProjectCode','ProtocolCode','StationCode','SampleDate','SampleComments','SampleRowID']],agency_lookup[['AgencyCode','AgencyName']], on='AgencyCode', how='left')

    sample = pd.merge(sample, event_lookup[['EventCode','EventName']], on='EventCode', how='left')

    sample = pd.merge(sample, project_lookup[['ParentProjectCode','ProjectCode','ProjectName']], on='ProjectCode', how='left')

    sample = pd.merge(sample, protocol_lookup[['ProtocolCode','ProtocolName']], on='ProtocolCode', how='left')

    station = pd.merge(station_lookup[['StationCode','StationName','EcoregionLevel3Code','HydrologicUnit','County','LocalWatershed','UpstreamArea']],stationdetail_lookup[['StationCode','TargetLatitude','TargetLongitude','Datum']], on='StationCode', how='left')

    sample = pd.merge(sample, station[['StationCode','StationName','EcoregionLevel3Code','HydrologicUnit','County','LocalWatershed','UpstreamArea','TargetLatitude','TargetLongitude','Datum']], on='StationCode', how='left')

    location = pd.merge(location_entry[['SampleRowID','LocationRowID','LocationCode','GeometryShape']],location_lookup[['LocationCode','LocationName']], on='LocationCode', how='left')

    sample = pd.merge(sample, location[['SampleRowID','LocationCode','LocationName','LocationRowID','GeometryShape']], on='SampleRowID', how='left')

    # Robert 20 Feb 2019
    # Here we add the ParentProjectName to the sample dataframe
    sample = pd.merge(sample, parent_project_lookup[['ParentProjectCode', 'ParentProjectName']], on = "ParentProjectCode", how = 'left')


    #### pull together constituent entries - - used by both field and habitat queries
    constituent = pd.merge(constituent_lookup[['ConstituentRowID','AnalyteCode','FractionCode','MatrixCode','MethodCode','UnitCode']],fraction_lookup[['FractionCode','FractionName']], on='FractionCode', how='left')

    constituent = pd.merge(constituent[['ConstituentRowID','AnalyteCode','FractionCode','FractionName','MatrixCode','MethodCode','UnitCode']],analyte_lookup[['AnalyteCode','AnalyteName']], on='AnalyteCode', how='left')

    constituent = pd.merge(constituent[['ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MethodCode','UnitCode']],matrix_lookup[['MatrixCode','MatrixName']], on='MatrixCode', how='left')

    constituent = pd.merge(constituent[['ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MatrixName','MethodCode','UnitCode']],method_lookup[['MethodCode','MethodName']], on='MethodCode', how='left')

    constituent = pd.merge(constituent[['ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MatrixName','MethodCode','MethodName','UnitCode']],unit_lookup[['UnitCode','UnitName']], on='UnitCode', how='left')

    ##### FIELD SPECIFIC CODE
    #### pull together field collection entry
    fieldcollection = pd.merge(fieldcollection_entry[['FieldCollectionRowID','LocationRowID','CollectionMethodCode','CollectionTime','Replicate','CollectionDepth','UnitCollectionDepth','FieldCollectionComments']],collectionmethod_lookup[['CollectionMethodCode','CollectionMethodName']], on='CollectionMethodCode', how='left')

    #### pull together field result entries

    fieldresult = pd.merge(fieldresult_entry[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','CollectionDeviceCode','ComplianceCode','QACode','ResQualCode','ExportData']],batchverification_lookup[['BatchVerificationCode','BatchVerificationDescr']], on='BatchVerificationCode', how='left')

    fieldresult = pd.merge(fieldresult[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','ComplianceCode','QACode','ResQualCode','ExportData']],collectiondevice_lookup[['CollectionDeviceCode','CollectionDeviceDescr']], on='CollectionDeviceCode', how='left')

    fieldresult = pd.merge(fieldresult[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','QACode','ResQualCode','ExportData']],compliance_lookup[['ComplianceCode','ComplianceName']], on='ComplianceCode', how='left')

    fieldresult = pd.merge(fieldresult[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','ResQualCode','ExportData']],qa_lookup[['QACode','QAName','QADescr']], on='QACode', how='left')

    fieldresult = pd.merge(fieldresult[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ExportData']],resqual_lookup[['ResQualCode','ResQualName']], on='ResQualCode', how='left')

    # combine fieldresult and constituent
    fieldresult = pd.merge(fieldresult[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ResQualName', 'ExportData']],constituent[['ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MatrixName','MethodCode','MethodName','UnitCode','UnitName']], on='ConstituentRowID', how='left')

    field = pd.merge(fieldresult[['FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ResQualName','ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MatrixName','MethodCode','MethodName','UnitCode','UnitName','ExportData']],fieldcollection[['FieldCollectionRowID','LocationRowID','CollectionMethodCode','CollectionMethodName','CollectionTime','Replicate','CollectionDepth','UnitCollectionDepth','FieldCollectionComments']], on='FieldCollectionRowID')

    field_query = pd.merge(field[['FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ResQualName','ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MatrixName','MethodCode','MethodName','UnitCode','UnitName','FieldCollectionRowID','LocationRowID','CollectionMethodCode','CollectionMethodName','CollectionTime','Replicate','CollectionDepth','UnitCollectionDepth','FieldCollectionComments']],sample[['LocationRowID','AgencyCode','AgencyName','EventCode','EventName','ProjectCode','ProjectName','ProtocolCode','ProtocolName','StationCode','StationName','EcoregionLevel3Code','HydrologicUnit','County','LocalWatershed','UpstreamArea','TargetLatitude','TargetLongitude','Datum','SampleDate','SampleComments','SampleRowID','LocationCode','LocationName','GeometryShape']], on='LocationRowID')

    # lowercase all fieldname
    field_query.columns = [x.lower() for x in field_query.columns]

    errorLog("created fields -- datetime:")
    ### calculated fields
    ### month, year, ecoregionlayer, ecoregionlevel, ecoregioncode, rwqcb
    # month from sampledate
    # year from sampledate
    # make sampledate type object into datetime
    field_query['sampledate'] = pd.to_datetime(field_query['sampledate'])
    field_query["month"] = field_query.sampledate.dt.month
    field_query["year"] = field_query.sampledate.dt.year
    errorLog("--- end created fields")

    # ecoregionlayer = empty why?
    field_query['ecoregionlayer'] = ''
    # ecoregionlevel = 3 (habitat) or 33 (field)
    field_query['ecoregionlevel'] = 33
    # ecoregioncode = stationlookup.ecoregionlevel3code
    field_query.rename(columns={'ecoregionlevel3code': 'ecoregioncode'}, inplace=True)
    field_query.rename(columns={'agencycode': 'sampleagencycode','agencyname': 'sampleagencyname'}, inplace=True)

    # rwqcb = empty why?
    field_query['rwqcb'] = ''

    ### IMPORTANT THE FIELDS BELOW NEED TO BE FILLED IN ####
    #analytewfraction
    #analytewfractionwunit
    #analytewfractionwmatrixwunit

    #result_textraw
    #resultraw


    # f-h = field or habitat
    field_query['f_h'] = 'f'

    #submittingagency = login_agency
    field_query['submittingagency'] = ''
    #databasefilepath = ''
    field_query['databasefilepath'] = ''
    #dateloaded = submissiondate?
    errorLog("start Timestamp")
    field_query['dateloaded'] = pd.Timestamp(datetime.datetime(2017,1,1))
    errorLog("end Timestamp")
    ##dataloadedby
    field_query['dataloadedby'] = 'checker'
    ##cleaned
    field_query['cleaned'] = 1
    ##qaed
    field_query['qaed'] = 1
    ##metricscalculated
    field_query['metricscalculated'] = 1
    ##deactivate
    field_query['deactivate'] = 1
    ##projectcode
    field_query['projectcode'] = ''
    #loadidnum = submissionid
    field_query['loadidnum'] = -88

    field_query['rownum'] = -88

    field_query['project_code'] = project
    field_query['login_email'] = login
    field_query['login_agency'] = agency
    field_query['login_owner'] = owner
    field_query['login_year'] = year
    field_query['login_project'] = project

    # drop temp columns
    # field_query.drop(['constituentrowid','fieldresultrowid', 'fieldcollectionrowid','fieldresultcomments','qaname','fieldcollectioncomments'],axis=1,inplace=True)

    # Rename the columns to what they will be when it gets loaded into tbl_phab
    field_query.rename(columns={'fieldcollectionrowid': 'collectionrowid', 'fieldresultcomments': 'resultcomments', 'fieldcollectioncomments': 'collectioncomments'},inplace=True)

    ##### END FIELD SPECIFIC CODE

    ##### HABITAT SPECIFIC CODE
    #### pull together field collection entry
    habitatcollection = pd.merge(habitatcollection_entry[['HabitatCollectionRowID','LocationRowID','CollectionMethodCode','CollectionTime','Replicate','HabitatCollectionComments']],collectionmethod_lookup[['CollectionMethodCode','CollectionMethodName']], on='CollectionMethodCode', how='left')

    #### pull together field result entries

    habitatresult = pd.merge(habitatresult_entry[['ConstituentRowID','HabitatResultRowID','HabitatCollectionRowID','VariableResult','Result','HabitatResultComments','CollectionDeviceCode','ComplianceCode','QACode','ResQualCode','ExportData']],collectiondevice_lookup[['CollectionDeviceCode','CollectionDeviceDescr']], on='CollectionDeviceCode', how='left')

    habitatresult = pd.merge(habitatresult[['ConstituentRowID','HabitatResultRowID','HabitatCollectionRowID','VariableResult','Result','HabitatResultComments','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','QACode','ResQualCode','ExportData']],compliance_lookup[['ComplianceCode','ComplianceName']], on='ComplianceCode', how='left')

    habitatresult = pd.merge(habitatresult[['ConstituentRowID','HabitatResultRowID','HabitatCollectionRowID','VariableResult','Result','HabitatResultComments','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','ResQualCode','ExportData']],qa_lookup[['QACode','QAName']], on='QACode', how='left')

    habitatresult = pd.merge(habitatresult[['ConstituentRowID','HabitatResultRowID','HabitatCollectionRowID','VariableResult','Result','HabitatResultComments','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ExportData']],resqual_lookup[['ResQualCode','ResQualName']], on='ResQualCode', how='left')

    # combine fieldresult and constituent
    habitatresult = pd.merge(habitatresult[['ConstituentRowID','HabitatResultRowID','HabitatCollectionRowID','VariableResult','Result','HabitatResultComments','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ResQualName','ExportData']],constituent[['ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MatrixName','MethodCode','MethodName','UnitCode','UnitName']], on='ConstituentRowID', how='left')

    habitat = pd.merge(habitatresult[['HabitatResultRowID','HabitatCollectionRowID','VariableResult','Result','HabitatResultComments','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ResQualName','ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MatrixName','MethodCode','MethodName','UnitCode','UnitName','ExportData']],habitatcollection[['HabitatCollectionRowID','LocationRowID','CollectionMethodCode','CollectionMethodName','CollectionTime','Replicate','HabitatCollectionComments']], on='HabitatCollectionRowID')

    habitat_query = pd.merge(habitat[['HabitatResultRowID','HabitatCollectionRowID','LocationRowID','VariableResult','Result','HabitatResultComments','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ResQualName','ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MatrixName','MethodCode','MethodName','UnitCode','UnitName','CollectionMethodCode','CollectionMethodName','CollectionTime','Replicate','HabitatCollectionComments','ExportData']],sample[['LocationRowID','AgencyCode','AgencyName','EventCode','EventName','ProjectCode','ProjectName','ProtocolCode','ProtocolName','StationCode','StationName','EcoregionLevel3Code','HydrologicUnit','County','LocalWatershed','UpstreamArea','TargetLatitude','TargetLongitude','Datum','SampleDate','SampleComments','SampleRowID','LocationCode','LocationName','GeometryShape']], on='LocationRowID')

    # lowercase all fieldname
    habitat_query.columns = [x.lower() for x in habitat_query.columns]

    ### calculated fields
    ### month, year, ecoregionlayer, ecoregionlevel, ecoregioncode, rwqcb
    # month from sampledate
    # year from sampledate
    # make sampledate type object into datetime
    habitat_query['sampledate'] = pd.to_datetime(habitat_query['sampledate'])
    habitat_query["month"] = habitat_query.sampledate.dt.month
    habitat_query["year"] = habitat_query.sampledate.dt.year

    # ecoregionlayer = empty why?
    habitat_query['ecoregionlayer'] = ''
    # ecoregionlevel = 3 (habitat) or 33 (field)
    habitat_query['ecoregionlevel'] = 3
    # ecoregioncode = stationlookup.ecoregionlevel3code
    habitat_query.rename(columns={'ecoregionlevel3code': 'ecoregioncode'}, inplace=True)
    habitat_query.rename(columns={'agencycode': 'sampleagencycode','agencyname': 'sampleagencyname'}, inplace=True)

    # rwqcb = empty why?
    habitat_query['rwqcb'] = ''


    #analytewfraction
    #analytewfractionwunit
    #analytewfractionwmatrixwunit

    #result_textraw
    #resultraw


    # f-h = field or habitat
    habitat_query['f_h'] = 'h'

    #submittingagency = login_agency
    habitat_query['submittingagency'] = ''
    #databasefilepath = ''
    habitat_query['databasefilepath'] = ''
    #dateloaded = submissiondate?
    habitat_query['dateloaded'] = pd.Timestamp(datetime.datetime(2017,1,1))
    ##dataloadedby
    habitat_query['dataloadedby'] = 'checker'
    ##cleaned
    habitat_query['cleaned'] = 1
    ##qaed
    habitat_query['qaed'] = 1
    ##metricscalculated
    habitat_query['metricscalculated'] = 1
    ##deactivate
    habitat_query['deactivate'] = 1
    ##projectcode
    habitat_query['projectcode'] = ''
    #loadidnum = submissionid
    habitat_query['loadidnum'] = -88

    habitat_query['rownum'] = -88

    habitat_query['project_code'] = project
    habitat_query['login_email'] = login
    habitat_query['login_agency'] = agency
    habitat_query['login_owner'] = owner
    habitat_query['login_year'] = year
    habitat_query['login_project'] = project

    # drop temp columns
    # habitat_query.drop(['constituentrowid','habitatresultrowid', 'habitatcollectionrowid','habitatresultcomments','qaname','habitatcollectioncomments'],axis=1,inplace=True)

    # Rename columns to what they will be when it gets put into tbl_phab
    habitat_query.rename(columns={'habitatcollectionrowid': 'collectionrowid', 'habitatresultcomments': 'resultcomments', 'habitatcollectioncomments': 'collectioncomments'},inplace=True)

    ##### END HABITAT SPECIFIC CODE
    #return field_query, habitat_query


    # For some reason there was a duplicated column in the field query dataframe, and it caused the script to break at the concatenation part.
    # Here is the workaround, to drop duplicated columns
    field_query = field_query.loc[:, ~field_query.columns.duplicated()]

    ## CONCATENATE FIELD AND HABITAT QUERIES
    rawdata = pd.concat([field_query,habitat_query])

    # drop unnecessary columns
    rawdata.drop(['constituentrowid', 'fieldresultrowid', 'habitatresultrowid', 'qaname'], axis = 1, inplace = True)


    ## FILL IN MISSING FIELDS
    # create engine connection
    eng = create_engine('postgresql://sde:dinkum@192.168.1.17:5432/smc')
    # get list of tbl_phab columns
    metadata = MetaData()
    tbl_phab = Table('tbl_phab',metadata,autoload = True, autoload_with = eng)
    tbl_phab_cols = [x.name for x in tbl_phab.columns]
    tbl_phab_cols = [x for x in tbl_phab_cols if x not in ['objectid','globalid','created_user','created_date','last_edited_user','last_edited_date']]
    # get list of missing columns
    missing_cols = [c for c in tbl_phab_cols if c not in rawdata.columns]
    # fill in missing columns with default values. (Needed to load into raw phab data table) 
    for x in missing_cols:
        if isinstance(tbl_phab.columns[x].type,VARCHAR):
            rawdata[x] = ''
        if isinstance(tbl_phab.columns[x].type,INTEGER)|isinstance(tbl_phab.columns[x].type,NUMERIC)|isinstance(tbl_phab.columns[x].type,SMALLINT):
            rawdata[x] = -88
        if isinstance(tbl_phab.columns[x].type,TIMESTAMP):
            rawdata[x] = datetime.date(1950,01,01)

    phabtable = pd.read_sql("SELECT * FROM tbl_phab LIMIT 1", eng)
    phabcolumnsmatched = len(phabtable.columns) - 1

    phabmetricstable = pd.read_sql("SELECT * FROM tmp_phabmetrics LIMIT 1", eng)
    phabmetricscolumnsmatched = len(phabmetricstable.columns) - 1

    return rawdata





################################################
# --               Code Archive             -- #
################################################
'''
errorLog("STARTING MERGING TOGETHER FIELDS - IE. RUNNING QUERY")
### pull together sample and location data - used by both field and habitat queries
sample = pd.merge(sample_entry[['AgencyCode','EventCode','ProjectCode','ProtocolCode','StationCode','SampleDate','SampleComments','SampleRowID']],agency_lookup[['AgencyCode','AgencyName']], on='AgencyCode', how='left')

sample = pd.merge(sample[['AgencyCode','AgencyName','EventCode','ProjectCode','ProtocolCode','StationCode','SampleDate','SampleComments','SampleRowID']],event_lookup[['EventCode','EventName']], on='EventCode', how='left')

sample = pd.merge(sample[['AgencyCode','AgencyName','EventCode','EventName','ProjectCode','ProtocolCode','StationCode','SampleDate','SampleComments','SampleRowID']],project_lookup[['ParentProjectCode','ProjectCode','ProjectName']], on='ProjectCode', how='left')

sample = pd.merge(sample[['AgencyCode','AgencyName','EventCode','EventName','ParentProjectCode','ProjectCode','ProjectName','ProtocolCode','StationCode','SampleDate','SampleComments','SampleRowID']],protocol_lookup[['ProtocolCode','ProtocolName']], on='ProtocolCode', how='left')

station = pd.merge(station_lookup[['StationCode','StationName','EcoregionLevel3Code','HydrologicUnit','County','LocalWatershed','UpstreamArea']],stationdetail_lookup[['StationCode','TargetLatitude','TargetLongitude','Datum']], on='StationCode', how='left')

sample = pd.merge(sample[['AgencyCode','AgencyName','EventCode','EventName','ProjectCode','ProjectName','ProtocolCode','ProtocolName','StationCode','SampleDate','SampleComments','SampleRowID']],station[['StationCode','StationName','EcoregionLevel3Code','HydrologicUnit','County','LocalWatershed','UpstreamArea','TargetLatitude','TargetLongitude','Datum']], on='StationCode', how='left')

location = pd.merge(location_entry[['SampleRowID','LocationRowID','LocationCode','GeometryShape']],location_lookup[['LocationCode','LocationName']], on='LocationCode', how='left')

sample = pd.merge(sample[['AgencyCode','AgencyName','EventCode','EventName','ProjectCode','ProjectName','ProtocolCode','ProtocolName','StationCode','StationName','EcoregionLevel3Code','HydrologicUnit','County','LocalWatershed','UpstreamArea','TargetLatitude','TargetLongitude','Datum','SampleDate','SampleComments','SampleRowID',]],location[['SampleRowID','LocationCode','LocationName','LocationRowID','GeometryShape']], on='SampleRowID', how='left')
'''

'''
#### pull together field result entries

fieldresult = pd.merge(fieldresult_entry[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','CollectionDeviceCode','ComplianceCode','QACode','ResQualCode','ExportData']],batchverification_lookup[['BatchVerificationCode','BatchVerificationDescr']], on='BatchVerificationCode', how='left')

fieldresult = pd.merge(fieldresult[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','ComplianceCode','QACode','ResQualCode','ExportData']],collectiondevice_lookup[['CollectionDeviceCode','CollectionDeviceDescr']], on='CollectionDeviceCode', how='left')

fieldresult = pd.merge(fieldresult[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','QACode','ResQualCode','ExportData']],compliance_lookup[['ComplianceCode','ComplianceName']], on='ComplianceCode', how='left')

fieldresult = pd.merge(fieldresult[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','ResQualCode','ExportData']],qa_lookup[['QACode','QAName','QADescr']], on='QACode', how='left')

fieldresult = pd.merge(fieldresult[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ExportData']],resqual_lookup[['ResQualCode','ResQualName']], on='ResQualCode', how='left')

# combine fieldresult and constituent
fieldresult = pd.merge(fieldresult[['ConstituentRowID','FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ResQualName']],constituent[['ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MatrixName','MethodCode','MethodName','UnitCode','UnitName']], on='ConstituentRowID', how='left')

field = pd.merge(fieldresult[['FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ResQualName','ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MatrixName','MethodCode','MethodName','UnitCode','UnitName','ExportData']],fieldcollection[['FieldCollectionRowID','LocationRowID','CollectionMethodCode','CollectionMethodName','CollectionTime','Replicate','CollectionDepth','UnitCollectionDepth','FieldCollectionComments']], on='FieldCollectionRowID')

field_query = pd.merge(field[['FieldResultRowID','FieldCollectionRowID','CalibrationDate','FieldReplicate','Result','FieldResultComments','BatchVerificationCode','BatchVerificationDescr','CollectionDeviceCode','CollectionDeviceDescr','ComplianceCode','ComplianceName','QACode','QAName','ResQualCode','ResQualName','ConstituentRowID','AnalyteCode','AnalyteName','FractionCode','FractionName','MatrixCode','MatrixName','MethodCode','MethodName','UnitCode','UnitName','FieldCollectionRowID','LocationRowID','CollectionMethodCode','CollectionMethodName','CollectionTime','Replicate','CollectionDepth','UnitCollectionDepth','FieldCollectionComments']],sample[['LocationRowID','AgencyCode','AgencyName','EventCode','EventName','ProjectCode','ProjectName','ProtocolCode','ProtocolName','StationCode','StationName','EcoregionLevel3Code','HydrologicUnit','County','LocalWatershed','UpstreamArea','TargetLatitude','TargetLongitude','Datum','SampleDate','SampleComments','SampleRowID','LocationCode','LocationName','GeometryShape']], on='LocationRowID')
'''
