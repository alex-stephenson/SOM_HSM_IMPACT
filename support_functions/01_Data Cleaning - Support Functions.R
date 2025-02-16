library(tidyverse)
library(lubridate)
library(openxlsx)
library(readxl)
library(rpivotTable)
library(cleaninginspectoR)
library(HighFrequencyChecks)
library(rio)
library(koboquest)
# Log issues function


# Survey time check function
time_check <- function(df, time_min, time_max){
  df <- df %>% mutate(interview_duration = difftime(as.POSIXct(ymd_hms(end)), as.POSIXct(ymd_hms(start)), units = "mins"),
                      CHECK_interview_duration = case_when(
                        interview_duration < time_min ~ "Too short",
                        interview_duration > time_max ~ "Too long",
                        TRUE ~ "Okay"
                      )
  )
  return(df)
}

