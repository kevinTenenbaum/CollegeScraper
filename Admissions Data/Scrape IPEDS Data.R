library(DBI)
library(RMariaDB)
library(dplyr)


# Source function to connect to DB without exposing credentials
source('~/R/Hockey/Scraper/CreateDBConnection.R')

setwd('Admissions Data')

varNames <- c('UNITID', 'APPLCN', 'APPLCNM','APPLCNW',"ENRLT",
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







downloadData <- function(year, varNames){
  cat("Processing ", year, "...\n")
  td <- tempdir()
  if(year > 2013){
    url <- paste0("https://nces.ed.gov/ipeds/datacenter/data/ADM", year,".zip")  
  } else {
    url <- paste0("https://nces.ed.gov/ipeds/datacenter/data/IC", year,".zip")
  }
  
  tf <- tempfile(tmpdir=td, fileext=".zip")
  download.file(url, tf)
  fname <- unzip(tf, list=TRUE)$Name[1]
  unzip(tf, files=fname, exdir=td, overwrite=TRUE)
  fpath = file.path(td, fname)
  
  dat <- read.csv(fpath, header=TRUE, row.names=NULL, 
                  stringsAsFactors=FALSE)

  
  dat$year <- year
  dat <- dat[,varNames]

  
  if(year <= 2013){
    dat <- replace(dat, dat == '.', NA)
  } 
  
  con <- dbCon('College')
  dbExecute(con, paste0("delete from FactAdmissions where year = ", year))
  cNames <- dbReadTable(con, "FactAdmissions") %>% colnames()
  RMariaDB::dbWriteTable(con, "FactAdmissions", dat[,cNames], append = TRUE)
  dbDisconnect(con)
  
  Sys.sleep(runif(1, min = 1, max = 4.5))
}

lapply(2018:2004, downloadData, varNames = varNames)




