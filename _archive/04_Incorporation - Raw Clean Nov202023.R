# clear worspace 
rm(list = ls())

# load  packages
library(cleaningtools)
library(readxl)
library(dplyr)
library(tidyverse)
library(lubridate)
library(openxlsx)
library(readxl)
library(rpivotTable)
library(cleaninginspectoR)
library(HighFrequencyChecks)
library(rio)
library(koboquest)
library(plyr)
library(labelled)


my_raw_dataset <- read_excel("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/Raw_data_hsm_aug24Jul_30_2024_185928.xlsx", sheet = "Raw_data")
tool <- read_excel("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/02_Data Collection & Processing/01_Tool/SOM_REACH_H2R_July_2024_Tool - 090724.xlsx"
)
#Update house numbers

my_raw_dataset <- my_raw_dataset %>% unique()
#############################time checks
## Check 1: survey time taken
mindur <- 20
maxdur<- 80
time_zone <- "Africa/Nairobi"

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


my_raw_dataset <- time_check(my_raw_dataset, time_min = mindur, time_max = maxdur)



clogs <- read.xlsx("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/04_data_cleaning/clog_combined/clog_input_orginal.xlsx")

clogs2 <- clogs %>% filter(change_type=="no_action")
deletion  <- clogs [clogs $change_type ==
                    "remove_survey"
                  , ]

write.xlsx(clogs,"C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/04_data_cleaning/clog_combined/clog_input.xlsx")

# ###############review cleaning log
review_clog <- review_cleaning_log(
  my_raw_dataset,
  raw_data_uuid_column = "_uuid",
  clogs,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column= "question",
  cleaning_log_new_value_column="new_value",
  cleaning_log_change_type_column="change_type",
  change_response_value = "change_response"
)

###############################create clean data from Clogs#####################
clean_data <- create_clean_data(
  my_raw_dataset,
  raw_data_uuid_column = "_uuid",
  clogs,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column="question",
  cleaning_log_new_value_column="new_value",
  cleaning_log_change_type_column="change_type",
  change_response_value = "change_response",
  NA_response_value = "blank_response",
  no_change_value = "no_action",
  remove_survey_value = "remove_survey"
)

write.xlsx(clean_data,"C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/04_data_cleaning/clog_combined/clean_data.xlsx")
 
 
 
 # clean_data <- read_excel("RNA_Data_for_checks.xlsx", sheet = "Clean_Data")

#####################sleeping open space
clean_data <- clean_data %>%  mutate(sleep_open= case_when(
  different_shelter_other=="None" ~ different_shelter,
  TRUE ~ 0,
  
))

clean_data <- clean_data %>%  mutate(
  different_shelter= case_when(different_shelter_other=="None" ~ 0,
                               TRUE ~ different_shelter)
  
)

#######################################################Export Clean dataset#####################

# clean_data <- read.xlsx("D:\\03_RNA\\Raw_cleaned\\Output/Clean_data.xlsx") 


"%!in%" <- Negate("%in%")
clog_input_deletions  <- filter(clogs, uuid %!in% deletion$uuid)

# ############################ First round of REVIEW CLEANING LOG

review_cleaning1 <- review_cleaning(
  my_raw_dataset,
  raw_dataset_uuid_column = "uuid",
  clean_data,
  clean_dataset_uuid_column = "uuid",
  cleaning_log = clog_input_deletions,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_change_type_column = "change_type",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_old_value_column = "old_value",
  cleaning_log_added_survey_value = "added_survey",
  cleaning_log_no_change_value = c("no_action", "no_change"),
  deletion_log = deletion,
  deletion_log_uuid_column = "uuid",
  check_for_deletion_log = T
)


review_cleaning1 <- review_cleaning1 %>% filter(df.question!="interview_duration")
review_cleaning1 <- review_cleaning1 %>% filter(df.question!="audit")
review_cleaning1 <- review_cleaning1 %>% filter(df.question!="deviceid")
review_cleaning1 <- review_cleaning1 %>% filter(df.question!="enum_code")
review_cleaning1 <- review_cleaning1 %>% filter(df.question!="observation_gps")
review_cleaning1 <- review_cleaning1 %>% filter(df.question!="_observation_gps_latitude")
review_cleaning1 <- review_cleaning1 %>% filter(df.question!="_observation_gps_longitude")
review_cleaning1 <- review_cleaning1 %>% filter(df.question!="_observation_gps_altitude")
review_cleaning1 <- review_cleaning1 %>% filter(df.question!="_id")
review_cleaning1 <- review_cleaning1 %>% filter(df.question!="__version__")
review_cleaning1 <- review_cleaning1 %>% filter(df.question!="_observation_gps_precision")
review_cleaning1 <- review_cleaning1 %>% filter(df.question!="CHECK_interview_duration")

# clean_data <- clean_data %>%  mutate(sleep_open= case_when(
#   different_shelter_other=="None" ~ different_shelter,
#   TRUE ~ 0,
#  
# ))
# 
# clean_data <- clean_data %>%  mutate(
#   different_shelter= case_when(different_shelter_other=="None" ~ 0,
#                                 TRUE ~ different_shelter)
#   
# )

#######################################attaching correct settlement names



var_label(clean_data) <- as.list(setNames(as.character(1:ncol(clean_data)),colnames(clean_data)))


######export data as a workbook
RNA_Data <-list("Raw_data"=my_raw_dataset,
                          "Clean_Data"=clean_data,"Cleaning_Log"=clog_input_deletions,"Deletion"=deletion)

write.xlsx(RNA_Data,"RNA_Data_for_checks.xlsx")

write.xlsx(review_cleaning1,"review.xlsx")

