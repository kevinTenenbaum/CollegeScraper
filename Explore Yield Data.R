library(dbplyr)
library(dplyr)
library(ggplot2)

source('~/R/Hockey/Scraper/CreateDBConnection.R')

con <- dbCon('College')
colleges <- tbl(con, "DimCollege") %>% filter(CYACTIVE == 1 & UGOFFER == 1 & INSTCAT == 2)
admissions <- tbl(con, "FactAdmissions")  %>% select(UNITID, year, APPLCN, ADMSSN, ENRLT)

colleges <- collect(colleges)
admissions <- collect(admissions)

dbDisconnect(con)

joined <- colleges %>% inner_join(admissions, 
                                  by = c('UNITID','year')) %>% 
  mutate(AcceptRate = ADMSSN/APPLCN,
         Yield = ENRLT/ADMSSN) %>% select(UNITID, INSTNM, year, APPLCN, AcceptRate, Yield) %>% arrange(year)



ggplot(joined %>% filter(INSTNM == 'Princeton University' ), aes(x = year, y = Yield)) + geom_line()


ggplot(joined %>% filter(AcceptRate <= .4 & APPLCN >= 5000), aes(x = AcceptRate, y = Yield)) + geom_point() + geom_smooth()


joined %>% filter(Yield >= 0.9 & APPLCN >= 1000) %>% filter(year == 2015)
