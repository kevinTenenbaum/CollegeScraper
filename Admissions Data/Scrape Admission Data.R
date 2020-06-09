library(DBI)
library(RMariaDB)
library(dplyr)


# Source function to connect to DB without exposing credentials and general IPEDS Data Download Functions
source('~/R/Hockey/Scraper/CreateDBConnection.R')
source('~/R/College Data/College IPEDS Data/HelperFunctions.R')



minYear <- 2004
maxYear <- 2018





getAdmissionsData <- function(year){
  varNames <- c('UNITID', 'APPLCN', 'APPLCNM','APPLCNW', "ADMSSN",
                "ADMSSNM",
                "ADMSSNW",
                "ENRLT",
                "ENRLM",
                "ENRLW",
                "ENRLFT",
                "ENRLFTM",
                "ENRLFTW",
                "ENRLPT",
                "ENRLPTM",
                "ENRLPTW",
                "SATVR25",
                "SATVR75",
                "SATMT25",
                "SATMT75",
                "ACTCM25",
                "ACTCM75",
                "ACTEN25",
                "ACTEN75",
                "ACTMT25",
                "ACTMT75",
                "year")
  if(year > 2013){
    url <- paste0("https://nces.ed.gov/ipeds/datacenter/data/ADM", year,".zip")  
  } else {
    url <- paste0("https://nces.ed.gov/ipeds/datacenter/data/IC", year,".zip")
  }
  
  dat <- downloadData(year, url = url, varNames)

  if(year <= 2013){
    dat <- replace(dat, dat == '.', NA)
  } 
  
  con <- dbCon('College')
  dbExecute(con, paste0("delete from FactAdmissions where year = ", year))
  cNames <- dbReadTable(con, "FactAdmissions") %>% colnames()
  RMariaDB::dbWriteTable(con, "FactAdmissions", dat[,cNames], append = FALSE)
  dbDisconnect(con)
  
  Sys.sleep(runif(1, min = 1, max = 4.5))
}





lapply(maxYear:minYear, function(yr){
  getAdmissionsData(year = yr)
})




