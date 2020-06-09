library(DBI)
library(RMariaDB)
library(dplyr)


# Source function to connect to DB without exposing credentials
source('~/R/Hockey/Scraper/CreateDBConnection.R')





downloadData <- function(year, url, varNames = NA){
  cat("Processing ", year, "...\n")
  td <- tempdir()
  
  tf <- tempfile(tmpdir=td, fileext=".zip")
  download.file(url, tf)
  fname <- unzip(tf, list=TRUE)$Name[1]
  unzip(tf, files=fname, exdir=td, overwrite=TRUE)
  fpath = file.path(td, fname)
  
  dat <- read.csv(fpath, header=TRUE, row.names=NULL, 
                  stringsAsFactors=FALSE)
  
  
  dat$year <- year
  
  # varNames[which(!(varNames %in% toupper(colnames(dat))))]
  
  if(!is.na(varNames)){
    dat <- dat[,varNames]
  }
  
  return(dat)
}
  
  