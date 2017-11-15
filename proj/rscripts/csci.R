# get command line arguments
ts <- commandArgs(TRUE)[1] # timestamp
bf <- commandArgs(TRUE)[2] # dynamic bugs filename
sf <- commandArgs(TRUE)[3] # dynamic stations filename
#ts <- "1510768595"

# load library
library(CSCI)

#bugs.df<-read.csv("/var/www/smc/files/bugs.csv") - working static file
bugs.df<-read.csv(bf)
stations.df<-read.csv(sf)
#bugs.df <- read.csv(paste("/var/www/smc/files/", ts, ".bugs.csv", sep = ""), stringsAsFactors = FALSE)
#stations.df<- read.csv(paste("/var/www/smc/files/", ts, ".stations.csv", sep = ""), stringsAsFactors = FALSE)

#Optional: Clean the bugs data if life stage codes are bad or missing
bugs.df<-cleanData(bugs.df)

#Calculate the CSCI
#Optional rand argument makes results repeatable
report<-CSCI(bugs.df, stations.df, rand=2)
#Export the desired reports
corecsv = paste("/var/www/smc/logs/", ts, ".core.csv", sep = "")
mmi1 = paste("/var/www/smc/logs/", ts, ".Suppl1_mmi.csv", sep = "") 
grps1 = paste("/var/www/smc/logs/", ts, ".Suppl1_grps.csv", sep = "")
oe1 = paste("/var/www/smc/logs/", ts, ".Suppl1_OE.csv", sep = "")
mmi2 = paste("/var/www/smc/logs/", ts, ".Suppl2_mmi.csv", sep = "")
oe2 = paste("/var/www/smc/logs/", ts, ".Suppl2_OE.csv", sep = "")

write.csv(report$core, corecsv)
write.csv(report$Suppl1_mmi, mmi1)
write.csv(report$Suppl1_grps, grps1)
write.csv(report$Suppl1_OE, oe1)
write.csv(report$Suppl2_mmi, mmi2)
write.csv(report$Suppl2_OE, oe2)

#write.csv(report$Suppl1_mmi, "/var/www/smc/files/Suppl1_mmi.csv")#Details about pMMI score
#write.csv(report$Suppl1_grps, "/var/www/smc/files/Suppl1_grps.csv") #Details on ref group membership
#write.csv(report$Suppl1_OE, "/var/www/smc/files/Suppl1_OE.csv") #Details about O/E score
#write.csv(report$Suppl2_mmi, "/var/www/smc/files/Suppl2_mmi.csv") #Iteration-level details on pMMI score
#write.csv(report$Suppl2_OE, "/var/www/smc/files/Suppl2_OE.csv")#Iteration-level details on O/E score
# command below returns output to python script
cat(corecsv)
