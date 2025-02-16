# REACH Somalia HSM Mar24 - Clean data combination and check Script

###########################################################
########################## Setup ##########################
###########################################################

setwd('C:/Users/reid.jackson/ACTED/IMPACT SOM - 01_REACH/Unit 1 - Intersectoral/SOM1901_HSM/03_Data/2024/01_March Round/')

# clear worspace 
rm(list = ls())

# load  packages
library(cleaningtools)
library(readxl)
library(dplyr)
library(openxlsx)
library(cleaninginspectoR)
library(plyr)
library(purrr)

date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")

#######################################################################################
######################### Read in the raw datasets and clogs ##########################
#######################################################################################

# get all of the processed clogs from all enumerators
clogs_path <- "06_combining_clean_data/fo_level_corrected_clogs_inputs/"
clogs_files_names <- setdiff(list.files(path = clogs_path), list.dirs(path = clogs_path, recursive = FALSE, full.names = FALSE))

# rbind all the datasets
raw_dataset <- clogs_files_names %>%
                  map_dfr(~read_excel(paste0(clogs_path,.x), sheet = "checked_dataset"))

# rbind all the clogs
clogs <- clogs_files_names %>%
                  map_dfr(~read_excel(paste0(clogs_path,.x), sheet = "cleaning_log"))

# rbind all the deletions
deletion_from_clogs <- clogs %>% 
                        filter(change_type == "remove_survey")

######################################################################################
############################## Avoiding False "Errors" ###############################
######################################################################################

# interview durations get rounded, cleaningTools picks up a "you changed a value but didn't indicate it in the cleaning log" when "35.643562436" gets rounded to "35.643"
# leading zeros get removed from phone numbers, triggering a missing cleaning log error. For example 063... gets shorted to 63...

# probably an easier way to do this with map, whatever
raw_dataset <- raw_dataset %>%
                  mutate(interview_duration = round(as.numeric(interview_duration),0),
                         ki_phone_number = as.numeric(ki_phone_number),
                         referral_phone = as.numeric(referral_phone),
                         referral_second = as.numeric(referral_second),
                         enum_code = as.numeric(enum_code),
                         `_gps_altitude` = as.numeric(`_gps_altitude`),
                         `_gps_latitude` = as.numeric(`_gps_latitude`),
                         `_gps_longitude` = as.numeric(`_gps_longitude`),
                         `_gps_precision` = as.numeric(`_gps_precision`)
                         )

#######################################################################################
############################## Review the cleaning logs ###############################
#######################################################################################

review_clog <- review_cleaning_log(raw_dataset,
                                   raw_data_uuid_column = "uuid",
                                   clogs,
                                   cleaning_log_uuid_column = "uuid",
                                   cleaning_log_question_column = "question",
                                   cleaning_log_new_value_column = "new_value",
                                   cleaning_log_change_type_column = "change_type",
                                   change_response_value = "change_response"
                                   )

#######################################################################################
############################ Create clean data with clogs #############################
#######################################################################################

clean_data <- create_clean_data(raw_dataset,
                                raw_data_uuid_column = "uuid",
                                clogs,
                                cleaning_log_uuid_column = "uuid",
                                cleaning_log_question_column = "question",
                                cleaning_log_new_value_column = "new_value",
                                cleaning_log_change_type_column = "change_type",
                                change_response_value = "change_response",
                                NA_response_value = "blank_response",
                                no_change_value = "no_action",
                                remove_survey_value = "remove_survey"
                                ) 

##########################################################################################################################
############################### Removing any clog entries associated with deleted surveys ################################
##########################################################################################################################

"%!in%" <- Negate("%in%")
clog_input_from_surveys_not_removed <- filter(clogs, uuid %!in% deletion_from_clogs$uuid)

#######################################################################################################
######################## Check for discrepancies between clog and clean data ##########################
#######################################################################################################

review_cleaning <- review_cleaning(raw_dataset,
                                   raw_dataset_uuid_column = "uuid",
                                   clean_data,
                                   clean_dataset_uuid_column = "uuid",
                                   cleaning_log = clog_input_from_surveys_not_removed,
                                   cleaning_log_uuid_column = "uuid",
                                   cleaning_log_change_type_column = "change_type",
                                   cleaning_log_question_column = "question",
                                   cleaning_log_new_value_column = "new_value",
                                   cleaning_log_old_value_column = "old_value",
                                   cleaning_log_added_survey_value = "added_survey",
                                   cleaning_log_no_change_value = c("no_action", "no_change"),
                                   deletion_log = deletion_from_clogs,
                                   deletion_log_uuid_column = "uuid",
                                   check_for_deletion_log = T
                                   )

####################################################################################
######################## Combine "other" settlement names ##########################
####################################################################################

# write.xlsx(clean_data, "clean_data_check.xlsx")

# manual entries were stored in settlement_other, so we combine those with the settlement column to create the master list "settlement_combined"
clean_data_settlement_combined <- clean_data %>%
                                        mutate(settlement_combined = ifelse(is.na(settlement_other),
                                                                            settlement, 
                                                                            tolower(gsub(" ", "_", settlement_other)) # put them to lower case and get rid of spaces for consistent formatting
                                                                            )
                                               ) %>%
                                        relocate(settlement_combined, .after = settlement_other) %>%
                                        select(-settlement_other) 

######################################################################################
######################## correct some settlement name typos ##########################
######################################################################################

# import the name corrections
settlement_name_corrections_name <- "_FO_assignments_and_locations/mar24_hsm_settlement_dup_correct_names 031024.xlsx"
correct_settlement_names <- read_excel(settlement_name_corrections_name, sheet = "corrections")

# join to clean_data and apply the name correction, if there is one
clean_data_settlement_spelling_corrected <- clean_data_settlement_combined %>%
                                                    left_join(correct_settlement_names, by = c("settlement_combined" = "incorrect spelling - to be updated")) %>%
                                                    mutate(settlement_combined = ifelse(is.na(`correct spelling`), settlement_combined, `correct spelling`)) %>%
                                                    select(-c("base - variable","region - variable","district - variable","correct spelling"))

####################################################################################
########################### Export the entire workbook #############################
####################################################################################

# combine everything into a nice file
hsm_mar24_data <-list("Raw_data" = raw_dataset,
                      "Clean_Data" = clean_data_settlement_spelling_corrected,
                      "Cleaning_Log" = clog_input_from_surveys_not_removed,
                      "Deletion" = deletion_from_clogs)

# write the data
output_location <- "06_combining_clean_data/combined_clean_data_and_clogs/"
output_file_name <- "clean_data_with_clogs_"

write.xlsx(hsm_mar24_data, paste0(output_location, output_file_name, date_time_now, ".xlsx"))

