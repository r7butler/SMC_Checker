import os
import time, datetime
import pymssql
from sqlalchemy import create_engine
from pandas import DataFrame

inengine = create_engine('mssql+pymssql://ReadOnlySwamp:ReadOnlySwamp1@165.235.37.226:1433/DW_Full')

# first step is to find out if there are any new records
result = inengine.execute("SELECT DWE_StationLookUp.StationCode, DWE_Sample.SampleDate, BenthicCollection.Replicate, BenthicCollection.SampleID, BenthicResult.FinalID, BenthicResult.LifeStageCode, BenthicResult.[Distinct], BenthicResult.BAResult, BenthicResult.Result, BenthicResult.UnitName, BenthicResult.ExcludedTaxa, BenthicLabEffort.PersonnelCode AS PersonnelCode_LabEffort, BenthicResult.PersonnelCode AS PersonnelCode_Results, BenthicResult.EnterDate, BenthicResult.LastUpdateDate, BenthicResult.TaxonomicQualifier, BenthicResult.QACode, BenthicResult.ResQualCode, BenthicResult.LabSampleID, BenthicLabEffort.AgencyCode, BenthicResult.BenthicResultComments FROM (((DWE_Location_LocationDetailWQ INNER JOIN (DWE_StationLookUp INNER JOIN DWE_Sample ON DWE_StationLookUp.StationCode = DWE_Sample.StationCode) ON DWE_Location_LocationDetailWQ.SampleRowID = DWE_Sample.SampleRowID) INNER JOIN BenthicCollection ON DWE_Location_LocationDetailWQ.LocationRowID = BenthicCollection.LocationRowID) INNER JOIN BenthicResult ON BenthicCollection.BenthicCollectionRowID = BenthicResult.BenthicCollectionRowID) INNER JOIN BenthicLabEffort ON BenthicCollection.BenthicCollectionRowID = BenthicLabEffort.BenthicCollectionRowID;")

result_dataframe = DataFrame(result.fetchall())
result_dataframe.columns = result.keys()

# does the record count match between swamp and sccwrp
