import os
import time, datetime
import pymssql
from sqlalchemy import create_engine
from pandas import DataFrame
inengine = create_engine('mssql+pymssql://ReadOnlySwamp:ReadOnlySwamp1@165.235.37.226:1433/DW_Full')
results = inengine.execute("SELECT DWE_StationLookUp.StationCode, DWE_Sample.SampleDate, BenthicCollection.Replicate, BenthicCollection.SampleID, BenthicResult.FinalID, BenthicResult.LifeStageCode, BenthicResult.[Distinct], BenthicResult.BAResult, BenthicResult.Result, BenthicResult.UnitName, BenthicResult.ExcludedTaxa, BenthicLabEffort.PersonnelCode AS PersonnelCode_LabEffort, BenthicResult.PersonnelCode AS PersonnelCode_Results, BenthicResult.EnterDate, BenthicResult.LastUpdateDate, BenthicResult.TaxonomicQualifier, BenthicResult.QACode, BenthicResult.ResQualCode, BenthicResult.LabSampleID, BenthicLabEffort.AgencyCode, BenthicResult.BenthicResultComments FROM (((DWE_Location_LocationDetailWQ INNER JOIN (DWE_StationLookUp INNER JOIN DWE_Sample ON DWE_StationLookUp.StationCode = DWE_Sample.StationCode) ON DWE_Location_LocationDetailWQ.SampleRowID = DWE_Sample.SampleRowID) INNER JOIN BenthicCollection ON DWE_Location_LocationDetailWQ.LocationRowID = BenthicCollection.LocationRowID) INNER JOIN BenthicResult ON BenthicCollection.BenthicCollectionRowID = BenthicResult.BenthicCollectionRowID) INNER JOIN BenthicLabEffort ON BenthicCollection.BenthicCollectionRowID = BenthicLabEffort.BenthicCollectionRowID;")
result_dataframe = DataFrame(results.fetchall())
result_dataframe.columns = results.keys()
result_dataframe.to_csv("/home/pauls/taxonomy-results-29jan18.csv", sep=",", encoding="utf-8")
result_dataframe.columns = [x.lower() for x in result_dataframe.columns]
#result_dataframe.rename(columns={"replicate": "fieldreplicate"}, inplace=True)
#result_dataframe.rename(columns={"sampleid": "fieldsampleid"}, inplace=True)
#result_dataframe.rename(columns={"distinct": "distinctcode"}, inplace=True)
#result_dataframe.rename(columns={"resqualcode": "resultqualifiercode"}, inplace=True)
#result_dataframe.rename(columns={"agencycode": "agencycode_labeffort"}, inplace=True)
#result_dataframe.rename(columns={"benthicresultcomments": "benthicresultscomments"}, inplace=True)
result_dataframe.rename(columns={"replicate": "fieldreplicate","sampleid": "fieldsampleid","distinct": "distinctcode","unitname": "unit","resqualcode": "resultqualifiercode","agencycode": "agencycode_labeffort","benthicresultcomments": "benthicresultscomments"}, inplace=True)

gettime = int(time.time())
TIMESTAMP = str(gettime)
def getRandomTimeStamp(row):
	row['objectid'] = int(TIMESTAMP) + int(row.name)
	return row
result_dataframe = result_dataframe.apply(getRandomTimeStamp, axis=1)

# get rid of nan
result_dataframe['baresult'].fillna(-88, inplace=True)

eng = create_engine('postgresql://sde:dinkum@192.168.1.16:5432/smcphab')
status = result_dataframe.to_sql('swamp_taxonomyresults', eng, if_exists='append', index=False)
