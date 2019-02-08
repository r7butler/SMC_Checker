# get command line arguments
ts <- commandArgs(TRUE)[1] # timestamp
bf <- commandArgs(TRUE)[2] # dynamic bugs filename
sf <- commandArgs(TRUE)[3] # dynamic stations filename
#ts <- "1510768595"

# load library
library(CSCI)
library(pbapply)

#bugs.df<-read.csv(bf)
#stations.df<-read.csv(sf)
bugs<-read.csv(bf)
stations<-read.csv(sf)

#Optional: Clean the bugs data if life stage codes are bad or missing
#bugs.df<-cleanData(bugs.df)
bugs<-cleanData(bugs)

#my.list <- unique(bugs[,c("SampleID","StationCode")])
#
#report <- list()

#pb <- txtProgressBar(min = 0, max = nrow(my.list), style = 3)

#for(i in 1:nrow(my.list)){
#  Sys.sleep(0.1)
#  report.n <- CSCI(bugs = bugs[which(bugs$SampleID == my.list$SampleID[i]),], stations = stations[which(stations$StationCode == my.list$StationCode[i]),], rand = 2)
#  report <- lapply(names(report.n), function(x) rbind(report.n[[x]], report[[x]]))
#  names(report) <- names(report.n)
#  setTxtProgressBar(pb, i)
#}

#fun.csci <- function(x){
#  stn.n <- my.list$StationCode[which(my.list$SampleID == x)]
#  report.n <- CSCI(bugs = bugs[which(bugs$SampleID == x),], stations = stations[which(stations$StationCode == stn.n),], rand =2)
#  rbind(report,report.n)
#}

#Calculate the CSCI
#Optional rand argument makes results repeatable
report<-CSCI(bugs, stations, rand=2)
# kenny code below
#report <- pblapply(my.list$SampleID, fun.csci)
#Export the desired reports
corecsv = paste("/var/www/smc/sync/logs/", ts, ".core.csv", sep = "")
mmi1 = paste("/var/www/smc/sync/logs/", ts, ".Suppl1_mmi.csv", sep = "") 
grps1 = paste("/var/www/smc/sync/logs/", ts, ".Suppl1_grps.csv", sep = "")
oe1 = paste("/var/www/smc/sync/logs/", ts, ".Suppl1_OE.csv", sep = "")
mmi2 = paste("/var/www/smc/sync/logs/", ts, ".Suppl2_mmi.csv", sep = "")
oe2 = paste("/var/www/smc/sync/logs/", ts, ".Suppl2_OE.csv", sep = "")

write.csv(report$core, corecsv)
write.csv(report$Suppl1_mmi, mmi1)
write.csv(report$Suppl1_grps, grps1)
write.csv(report$Suppl1_OE, oe1)
write.csv(report$Suppl2_mmi, mmi2)
write.csv(report$Suppl2_OE, oe2)

# command below returns output to python script
cat(corecsv)
