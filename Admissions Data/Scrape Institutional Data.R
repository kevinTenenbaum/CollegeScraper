library(DBI)
library(RMariaDB)
library(dplyr)


# Source function to connect to DB without exposing credentials and general IPEDS Data Download Functions
source('~/R/Hockey/Scraper/CreateDBConnection.R')
source('~/R/College Data/College IPEDS Data/HelperFunctions.R')


minYear = 2004
maxYear = 2018

getInstitutionalData <- function(year){
  url <- paste0("https://nces.ed.gov/ipeds/datacenter/data/HD", year, ".zip")  
  
  varNames <- c('UNITID','INSTNM', 'IALIAS', 'CITY','STABBR','ZIP','OBEREG', "NPRICURL","UGOFFER", "INSTCAT",
                "LOCALE", "ACT", "NEWID", "CLOSEDAT", "CYACTIVE","LONGITUD", "LATITUDE", "DFRCGID",'year')
  
  if(year < 2013){
    varNames <- varNames[varNames != "DFRCGID"]
  }
  if(year < 2011){
    varNames <- varNames[varNames != "NPRICURL"]
  }
  if(year == 2009){
    varNames <- tolower(varNames)
  }
  if(year < 2009){
    varNames <- varNames[!(varNames %in% c("LONGITUD", "LATITUDE"))] 
  }
  if(year < 2005){
    varNames <- varNames[!(varNames == 'IALIAS')] 
  }
  dat <- downloadData(year, url, varNames)
  
  if(year < 2010){
    colnames(dat)[colnames(dat) != 'year'] <- toupper(colnames(dat)[colnames(dat) != 'year'])
  }
  if(year < 2013){
    dat[,"DFRCGID"] <- NA
  }
  if(year < 2011){
    dat[,"NPRICURL"] <- NA
  }
  if(year < 2009){
    dat[,"LONGITUD"] <- NA
    dat[,"LATITUDE"] <- NA
  }
  if(year < 2005){
    dat[,'IALIAS'] <- NA
  }
  
  dat[,c("LONGITUD", "LATITUDE")] <- sapply(dat[,c("LONGITUD", "LATITUDE")], as.numeric)
  dat[, c("LONGITUD", "LATITUDE")] <- round(dat[, c("LONGITUD", "LATITUDE")], 5)
  
  con <- dbCon('College')
  # dbExecute(con, "drop table DimCollege")
  dbExecute(con, paste0("delete from DimCollege where year = ", year))
  cNames <- dbReadTable(con, "DimCollege") %>% colnames()
  RMariaDB::dbWriteTable(con, "DimCollege", dat[,cNames], append = TRUE)
  dbDisconnect(con)
  
  Sys.sleep(runif(1, min = 1, max = 4.5))
}

lapply(maxYear:minYear, function(yr){
  getInstitutionalData(year = yr)
})
