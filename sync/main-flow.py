Purpose of this script: To unify existing taxonomic data into a single table "taxonomy" from SMC and CEDEN programs. Also, to process the CSCI index for each sampleid (stationcode, sampledate, collectionmethodcode, replicate).

Action: Get new data from SMC: 
                1. Based on difference (new records) between taxonomy.origin_lastupdatedate and tblToxicityResults.LastUpdateDate.
                2. Modify the record and add new field record_publish set to true if record is in Southern Califoria region and is 2016 or older store in unified taxonomy table.

Action: Get new data from SWAMP:
                **** swamp_taxonomy is a duplicate copy of the original data pulled from SWAMP the only modified field is record_publish ****
                1. Based on difference (new records) between swamp_taxonomy.origin_lastupdatedate and BenthicResult.LastUpdateDate.
                2. Store a copy of the record in swamp_taxonomy and modify record_publish from 1/0 to true/false.
                3. Store a second copy of the record in unified taxonomy table and modify record_publish based on following criteria: 
                        If BenthicResult.LastUpdateDate is set to true and if the record is in Southern Califoria region set to true

Action: Process CSCI scores:
                1. Do we have related gis cross walk data?
                2. Do we have related gis metrics data?
                3. Did the rscript process correctly?

                If we fail any of the above we notify SCCWRP, otherwise the processed records are stored in six csci tables.
