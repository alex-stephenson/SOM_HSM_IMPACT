# REACH Somalia HSM Mar24 - Data Cleaning Script

# Week 1
# 3/4: run at 8:55am to produce clogs for 3/3 data collection
# 3/5: run at 9:20am to produce clogs for 3/4 data collection
# 3/6: run at 9:55am to produce clogs for 3/5 data collection
# 3/7: run at 8:45am to produce clogs for 3/6 data collection

# 3/10: run at 8:55am to produce clogs for 3/7, 3/8, 3/9 data collection
# 3/11: run at 9:22am to produce clogs for 3/10 data collection
# 3/12: run at 8:05am to produce clogs for 3/11 data collection
# 3/13: 
# 3/14: 


###########################################################
########################## Setup ##########################
###########################################################

# start with a clean slate
rm(list = ls())

# set the working directory
setwd("C:/Users/Abdirahman IBRAHIM/Documents/R/HSMDEC_2024/CLeaning scripts/_code")


# load additional data cleaning functions
source("C:/Users/Abdirahman IBRAHIM/Documents/R/HSMDEC_2024/CLeaning scripts/_code/support_functions/01_Data Cleaning - Support Functions.R")

# load up our packages
library(cleaningtools)
library(dplyr)
library(readxl)
library(stringr)
library(purrr)
library(naniar)
library(hypegrammaR)
library(httr)
library(KoboconnectR)
library(lubridate)

# get the timestamp to more easily keep track of different versions
date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")

#################################################################################
########################## Download the data from kobo ##########################
#################################################################################
# 
# # update this based on your details / project
 kobo_user_name <- "abdirahmanaia"
 kobo_pw <- "Lewandozki_44"
 kobo_url <- "kobo.impact-initiatives.org"
 #kobo_project_name <- "REACH HSM 2024 December Tool"
 kobo_project_name <- "REACH HSM 2024 August  Tool"

# 
# # get token - make sure your API key is set up in the Security section of Account Settings on Kobo Online
# # get_kobo_token(url = "eu.kobotoolbox.org", uname = "reidjackson", pwd = "uqr9wbr-RVB_arx9gyx")
# library(dplyr)
# 
# # Fetch the Kobo projects and convert to data frame
 kobo_projects <- as.data.frame(kobotools_api(url = kobo_url,
                                              simplified = TRUE,
                                              uname = kobo_user_name,
                                              pwd = kobo_pw))

# # Print the data to check its structure
# print(kobo_projects)
# 
# # Ensure the data frame is not NULL or empty
 if (is.null(kobo_projects) || nrow(kobo_projects) == 0) {
   stop("No projects returned by the API call.")
 }
# 
# # Ensure the data frame contains the 'name' column
 if (!"name" %in% colnames(kobo_projects)) {
   stop("The data frame does not contain a 'name' column.")
 }
# 
# # Filter the data frame to get the asset id of the kobo project for the HSM
 kobo_project_asset_id <- kobo_projects %>%
   filter(name == kobo_project_name) %>%
   pull(asset)
# 
# # Ensure a valid asset id is found
 if (length(kobo_project_asset_id) == 0) {
   stop("No matching project found for the specified name.")
 }
# 
# # Create a new export
 new_export_url <- kobo_export_create(url = kobo_url,
                                      uname = kobo_user_name,
                                      pwd = kobo_pw,
                                      assetid = kobo_project_asset_id,
                                      type = "xls",
                                      all = "false",
                                      lang = "_xml",
                                      hierarchy = "false",
                                     include_grp = "false",
                                      grp_sep = "/",
                                      multi_sel = "both",
                                      media_url = "false",
                                      sleep = 5)
 
# # get the list of all of my kobo projects
# 
# 
# # get the list of existing exports - may need to delete some online if you run out of memory but this hasn't caused me any issues to date
existing_exports <- as.data.frame(kobo_exports(url = kobo_url,
                                   uname = kobo_user_name,
                                   pwd = kobo_pw)$results
                                   )
 
# # filter for the asset id, arrange by the date/time the export was created
 existing_exports_formatted <- existing_exports %>%
                                 select(date_created, last_submission_time, result) %>% 
                                 bind_cols(existing_exports$data) %>%
                                 filter(grepl(kobo_project_asset_id, source)) %>%
                                 arrange(desc(date_created))
 
# # get the most recent export created, assuming that's what you want!!!
project_most_recent_export_url <- existing_exports_formatted[1,]$result
 
 # download the data from the URL
 kobo_project_export <- httr::GET(project_most_recent_export_url, # if you just created a new export url it should show up here after you pull the existing ones, but just make sure!
                                  httr::authenticate(user = kobo_user_name,
                                  password = kobo_pw)
                                 )
 
 kobo_project_content <- httr::content(kobo_project_export,
                                      type = "raw",
                                      encoding = "UTF-8")
 
# # write the raw data and read it back in for cleaning
 kobo_data_export_path <- paste0("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/Raw_data_hsm_aug24", date_time_now, ".xlsx")
 writeBin(kobo_project_content, kobo_data_export_path)
 raw_data <- read_excel(kobo_data_export_path)

# raw_data <- read_excel("C:/Users/reid.jackson/ACTED/IMPACT SOM - 01_REACH/Unit 1 - Intersectoral/SOM1901_HSM/03_Data/2024/01_March Round/_raw_data_download/hsm_mar24_raw_data_Feb_29_2024_113816.xlsx")

##############################################################################
######################### Start Processing the Data ##########################
##############################################################################

#raw_data <- read_excel("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/04_data_cleaning/REACH_HSM_2024_August__Tool_-_all_versions_-_False_-_2024-07-29-07-00-55.xlsx")
# in case we just want to run something again with the raw data without redownloading from kobo
# raw_data <- read_excel("C:/Users/reid.jackson/ACTED/IMPACT SOM - 01_REACH/Unit 1 - Intersectoral/SOM1901_HSM/03_Data/2023/03_December Round/_raw_data_download/hsm_dec23_raw_data_Jan_09_2024_085233.xlsx")
#raw_data <-read_excel("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/04_data_cleaning/REACH_HSM_2024_August__Tool_-_all_versions_-_False_-_2024-07-29-07-00-55.xlsx")


# clean up the raw data before doing further processing
 data_in_processing <- raw_data %>%

  # filter for the days surveys were submitted. This will change each day the clogs are run
  
  # if I ever need to use time and date to filter
  # filter(ymd_hms(`_submission_time`,tz=Sys.timezone()) >= '2023-12-24 00:00:00 EAT' & ymd_hms(`_submission_time`,tz=Sys.timezone()) <= '2023-12-24 23:59:59 EAT') %>%
  
  # multiple days for similarity checks
  # filter(as.Date(`_submission_time`) >= '2023-12-24' & as.Date(`_submission_time`) <= '2023-12-28') %>%
  # filter(as.Date(`_submission_time`) >= '2023-12-28') %>%
  
  # first week of data
   filter(as.Date(`_submission_time`) == '2024-07-30') %>%
  #filter(as.Date(today) == '2024-07-27') %>%
  #ilter(as.Date(today) == '2024-07-28') %>%
  #filter(as.Date(`_submission_time`) == '2024-07-27') %>%
  # filter(as.Date(`_submission_time`) == '2024-03-06') %>%
  # filter(as.Date(`_submission_time`) == '2024-03-07') %>%
  # filter(as.Date(`_submission_time`) >= '2024-03-04' & as.Date(`_submission_time`) <= '2024-03-09') %>%
  
  # second week of data
  # filter(as.Date(`_submission_time`) == '2024-03-10') %>%
  # filter(as.Date(`_submission_time`) == '2024-03-11') %>%
  # filter(as.Date(`_submission_time`) == '2024-03-12') %>%
  # filter(as.Date(`_submission_time`) == '2024-03-13') %>%
  # filter(as.Date(`_submission_time`) == '2024-03-14') %>%

  # third week of data
  #filter(as.Date(`_submission_time`) == '2024-07-21') %>%
  
  # rename the uuid column
  dplyr::rename("uuid" = "_uuid") %>%
  
  # get rid of the unnamed columns at the end due to different versions. They all have "..." in them
  # select(!matches('//.//.//.'))/
  
  # we add a blank column called "comment" to store FO comments when correctly clogs
  mutate(comment = "") %>%
  distinct()

##############################################################################
########################## Kobo Survey and Choices ###########################
##############################################################################

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







kobo_tool_name <- "C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/02_Data Collection & Processing/01_Tool/SOM_REACH_H2R_July_2024_Tool - 090724.xlsx"

# read in the survey questions / choices
kobo_survey <- read_excel(kobo_tool_name, sheet = "survey")
kobo_choice <- read_excel(kobo_tool_name, sheet = "choices")

###############################################################################
############################ FO / District Mapping ############################
###############################################################################

# join the Field Officer / District assignments 

# read in the FO/district mapping
fo_district_mapping <- read_excel("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/02_Data Collection & Processing/_FO_assignments_and_locations/fo_base_assignment_260224.xlsx") %>%
                          select(district_for_code, fo_in_charge_for_code) %>%
                          dplyr::rename("district" = "district_for_code") %>%
                          mutate_all(tolower)

# join the fo to the dataset - this way the code knows which surveys belong to which FOs
data_in_processing <- data_in_processing %>%
                        left_join(fo_district_mapping, by = "district") 

# for testing purposes, assign "dummyFO" to districts without an FO
# data_in_processing <- data_in_processing %>%
#                         mutate_at(vars(fo_in_charge_for_code), ~replace_na(., "dummyFO"))

##############################################################################
############################ Remove "Bad" Surveys ############################
##############################################################################

# Remove as many surveys as we can before creating the cleaning logs. For example, if a survey took 180 minutes, we know its going to be deleted so we shouldn't waste any enumerator time having the correct issues

# set the minimum and maximum "reasonable" survey times. Anything outside of this will be flagged
mindur_delete <- 20
mindur_flag <- 30
maxdur_flag <- 70
maxdur_delete <- 80

# define the time and timezone for surveys that are "too early" or "too late". We only accept surveys started after 6am and before 8pm
time_zone <- "Africa/Nairobi"
earliest_time <- with_tz(as.POSIXct("06:00:00", format = "%H:%M:%S"), tzone = time_zone)
latest_time <- with_tz(as.POSIXct("20:00:00", format = "%H:%M:%S"), tzone = time_zone)


# Continue for all data frames
#combined_df <- dplyr::bind_rows(df1, df2, df3, ...)

# check the data with the mindur / maxdur for deletion
data_in_processing <- time_check(data_in_processing, time_min = mindur_delete, time_max = maxdur_delete)

# filter surveys where KI said no to consent/continue/knowledgeable (cck)
surveys_to_remove_cck <- data_in_processing %>%
                            filter(not(is.na(ki_termination_reason))) %>%
                            mutate(delete_reason = "no KI consent/continue/knowledge")



# remove the cck deletions from any other delete checks. The same survey may also fail other checks in this section of code, but it doesn't matter as long as its deleted
cck_uuids <- surveys_to_remove_cck %>%
                select(uuid) %>%
                pull()


# filter surveys where KI said no community in the specified settlement
surveys_to_remove_no_community <- data_in_processing %>%
                                    filter(!(uuid %in% cck_uuids) & 
                                            (community_in_settlement == "no")) %>%
                                    mutate(delete_reason = "no community in settlement")

# remove the no community deletions from any other delete checks. The same survey may also fail other checks in this section of code, but it doesn't matter as long as its deleted
no_community_uuids <- surveys_to_remove_no_community %>%
                        select(uuid) %>%
                        pull()

# filter surveys where the face-to-face interviewed KI has not been to the settlement in over 30 days
surveys_to_remove_last_visit <- data_in_processing %>%
                                  filter(!(uuid %in% no_community_uuids) &
                                         !(uuid %in% cck_uuids) &
                                          (ki_last_speak_to_residents == "more_than_a_month")) %>%
                                  mutate(delete_reason = "KI last visited >30 days ago")

# remove the no community deletions from any other delete checks.  The same survey may also fail other checks in this section of code, but it doesn't matter as long as its deleted
last_visit_uuids <- surveys_to_remove_last_visit %>%
                          select(uuid) %>%
                          pull()

# filter surveys that are too short or too long
surveys_to_remove_time <- data_in_processing %>%
                            filter(!(uuid %in% last_visit_uuids) &
                                   !(uuid %in% no_community_uuids) &
                                   !(uuid %in% cck_uuids) &
                                    (CHECK_interview_duration == "Too short" | CHECK_interview_duration == "Too long")) %>%
                            mutate(delete_reason = "interview duration")

# remove the too long/too short deletions from any other delete checks.  The same survey may also fail other checks in this section of code, but it doesn't matter as long as its deleted
too_short_too_long_uuids <- surveys_to_remove_time %>%
                                select(uuid) %>%
                                pull()

# too early or too late. deleting surveys that were submitted during odd hours of the day
surveys_to_remove_start_early_late <- data_in_processing %>%
                                          filter(!(uuid %in% last_visit_uuids) &
                                                 !(uuid %in% no_community_uuids) &
                                                 !(uuid %in% cck_uuids) &
                                                 !(uuid %in% too_short_too_long_uuids) & 
                                                  ((hour(ymd_hms(start, tz=Sys.timezone())) <= hour(earliest_time)) | (hour(ymd_hms(start, tz=Sys.timezone())) >= hour(latest_time)))
                                                 ) %>% # we subtract 1 for yesterday as we are processing yesterday's clogs 
                                          mutate(delete_reason = "start too early or too late")


# combine all of the surveys we want to delete before creating clogs for the FOs to review
combined_surveys_to_delete <- surveys_to_remove_cck %>%
                                rbind(surveys_to_remove_no_community, surveys_to_remove_last_visit, surveys_to_remove_time, surveys_to_remove_start_early_late)

# write the deleted surveys for safe keeping/the deletion log
surveys_to_remove_export_path <- paste0("04_data_cleaning/_deleted_prior_to_cleaning/day_by_day/deleted_for_survey_duration_", date_time_now, ".xlsx")
combined_surveys_to_delete %>% write.xlsx(surveys_to_remove_export_path)

# get the list of deleted uuids and filter them out
bad_uuids <- combined_surveys_to_delete %>%
                pull(uuid)

# keep on going with the good surveys
data_in_processing <- data_in_processing %>%
                        filter(!(uuid %in% bad_uuids))

# before we run any clogs checks, we write the data not immediately deleted so that it can be process by the dashboard
# FOs will use the dashboard to check their progress against district/settlement targets. This will inform them where to tell enumerators to seek further interviews
dashboard_data_path <- paste0("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/99_Data_Collection_Dashboard/_data_for_dashboard", date_time_now, ".xlsx")
unchecked_clean_plus_deleted_data_for_dashboard <- data_in_processing %>%
                                                      mutate(delete_reason = "not_deleted") %>%
                                                      rbind(combined_surveys_to_delete)

# write the dashboard data as an excel file
unchecked_clean_plus_deleted_data_for_dashboard %>% write.xlsx(dashboard_data_path)

####################################################################################
########################## Define Checks for Logic Errors ##########################
####################################################################################
# Check if the column exists in the dataset

# These checks will look for unintuitive data or data that is contradictory

# Define the logic checks we will complete
# See the description section below for an explanation
check_list <- data.frame(
                    name = c(
                    # logic checks on reason_moved
                    "moved_drought_check",
                    "moved_flood_check",
                    "moved_pest_invasion_check",
                    "moved_disease_outbreak_check",
                    "moved_general_conflict_check",
                    
                    # logic checks for cannot move / cannot leave
                    "leave_elderly_check",
                    "leave_disability_check",
                    "leave_minority_clan_check",
                    "leave_woman_check",
                
                    # logic checks on crop losses
                    "crop_loss_drought_check",
                    "crop_loss_flooding_check",
                    "crop_loss_pest_invasion_check",
                    "crop_loss_temp_check",
                
                    # logic checks on livestock decrease
                    "livestock_decrease_drought_check",
                    "livestock_decrease_flood_check",
                    "livestock_decrease_disease_check",
                
                    # logic checks for education
                    "school_distance_travel_time_check",
                    "boys_512_school_attendance_check",
                    "girls_512_school_attendance_check",
                    "boys_1317_school_attendance_check",
                    "girls_1317_school_attendance_check",
                    
                    # logic checks for protection
                    "protection_fgm_check"
                
                    ),
                  check = c(
                    # logic checks on reason_moved - could have to manually update binaries for shocks
                    "grepl(/"*drought*/", reason_moved) & !grepl(/"*drought*/", shocks)",
                    "grepl(/"*flood*/", reason_moved) & !grepl(/"*flood*/", shocks)",
                    "grepl(/"*pest*/", reason_moved) & !grepl(/"*pest*/", shocks)",
                    "grepl(/"*human_disease*/", reason_moved) & !grepl(/"*human_disease*/", shocks)",
                    "grepl(/"*insecurity*|*evictions*|*impediments*/", reason_moved) & !grepl(/"*insecurity*/", shocks)",
                    
                    # logic checks for cannot move / cannot leave - could have to manually update binaries
                    # cancel the logic check if entire_household is selected for reasons_cannot_move
                    "grepl(/"*too_elderly*/", groups_cannot_move) & !grepl(/"*elderly*/", reasons_cannot_move) & !grepl(/"*entire_household*/", reasons_cannot_move)",
                    "grepl(/"*physically_disabled*/", groups_cannot_move) & !grepl(/"*pwd*/", reasons_cannot_move) & !grepl(/"*entire_household*/", reasons_cannot_move)",
                    "grepl(/"*clan_discrimination*/", groups_cannot_move) & !grepl(/"*minority_clans*/", reasons_cannot_move) & !grepl(/"*entire_household*/", reasons_cannot_move)",
                    "grepl(/"*male_company*/", groups_cannot_move) & !grepl(/"*girls_under18*|*adult_women_18_59*|*elderly_women_60*/", reasons_cannot_move) & !grepl(/"*entire_household*/", reasons_cannot_move)",
                    
                    # logic checks on crop losses - could have to manually update binaries for shocks
                    "grepl(/"*drought*/", reason_crop_loss) & !grepl(/"*drought*/", shocks)",
                    "grepl(/"*flood*/", reason_crop_loss) & !grepl(/"*flood*/", shocks)",
                    "grepl(/"*locusts*/", reason_crop_loss) & !grepl(/"*pest*/", shocks)",
                    "grepl(/"*temp_too_high*/", reason_crop_loss) & grepl(/"*temp_too_low*/", reason_crop_loss)",
                
                    # logic checks on livestock decrease - could have to manually update binaries for shocks
                    "grepl(/"*drought*/", reason_livestock_decrease) & !grepl(/"*drought*/", shocks)",
                    "grepl(/"*flood*/", reason_livestock_decrease) & !grepl(/"*flood*/", shocks)",
                    "grepl(/"*livestock_disease*/", reason_livestock_decrease) & !grepl(/"*livestock_disease*/", shocks)",
                    
                    # logic checks for education - could have to manually update binaries for barriers
                    "education_facility_available == /"no_learning_facilities_available/" & average_time == /"less_than_15min/"",
                    "grepl(/"*none|a_few*/", proportion_512_attend_school_boys_past_4wks) & grepl(/"*no_barriers*/", barriers_access_education_boys_512)",
                    "grepl(/"*none|a_few*/", proportion_512_attend_school_girls_past_4wks) & grepl(/"*no_barriers*/", barriers_access_education_girls_512)",
                    "grepl(/"*none|a_few*/", proportion_1317_attend_school_boys_past_4wks) & grepl(/"*no_barriers*/", barriers_access_education_boys_1317)",
                    "grepl(/"*none|a_few*/", proportion_1317_attend_school_girls_past_4wks) & grepl(/"*no_barriers*/", barriers_access_education_girls_1317)",
                
                    # logic checks for protection
                    "grepl(/"*fgm*/", safety_concerns) & !grepl(/"*girls*/", groups_concerns)"
                    ),
                  description = c(
                    # logic checks on reason_moved
                    "reason_moved includes drought, but drought was not selected in shocks",
                    "reason_moved includes flooding, but flooding was not selected in shocks",
                    "reason_moved includes pest/locust invasion, but pest/locust invasion was not selected in shocks",
                    "reason_moved includes human disease outbreak, but human disease outbreak was not selected in shocks",
                    "reason_moved includes at least one of conflict_insecurity, evictions, unlawful_impediments but insecurity was not selected in shocks",
                    
                    # logic checks for cannot move / cannot leave
                    "groups_cannot_move includes too_elderly, but elderly men/women were not selected in reasons_cannot_move",
                    "groups_cannot_move includes disabilities, but people with disabilities were not selected in reasons_cannot_move",
                    "groups_cannot_move includes clan discrimination, but minority clans were not selected in reasons_cannot_move",
                    "groups_cannot_move includes unable to travel without male companion, but no groups of women/girls were selected in reasons_cannot_move",
                    
                    # logic checks on crop losses
                    "reason_crop_loss includes drought, but drought was not selected in shocks",
                    "reason_crop_loss includes flooding, but flooding was not selected in shocks",
                    "reason_crop_loss includes pests/locusts, but pest/locust invasion was not selected in shocks",
                    "reason_crop_loss includes both temperature too high and too low",
                    
                    # logic checks on livestock decrease
                    "reason_livestock_decrease includes drought, but drought was not selected in shocks",
                    "reason_livestock_decrease includes flooding, but flooding was not selected in shocks",
                    "reason_livestock_decrease includes livestock disease, but livestock disease was not selected in shocks",
                    
                    # logic checks for education
                    "There is no school within 15 minutes but the average time to reach school is less than 15 minutes",
                    "Boys 5-12 attendance was none or a few, but no barriers to accessing education",
                    "Girls 5-12 attendance was none or a few, but no barriers to accessing education",
                    "Boys 13-17 attendance was none or a few, but no barriers to accessing education",
                    "Girls 13-17 attendance was none or a few, but no barriers to accessing education",
                    
                    # logic checks for protection
                    "safety_concerns is FGM, but mostly_girls is not selected in groups_concerns"
                    ),
                  columns_to_clean = c(
                    
                    # single choice questions don't have child columns, just a parent column with text (i.e. no binaries)
                    
                    # logic checks on reason_moved
                    "reason_moved, reason_moved/drought, shocks, shocks/drought",
                    "reason_moved, reason_moved/flooding, shocks, shocks/flooding",
                    "reason_moved, reason_moved/locusts_pests, shocks, shocks/locusts_pests",
                    "reason_moved, reason_moved/human_disease_outbreak, shocks, shocks/human_disease_outbreak",
                    "reason_moved, reason_moved/conflict_insecurity, reason_moved/evictions, reason_moved/unlawful_impediments, shocks, shocks/insecurity",
                
                    "reasons_cannot_move, reasons_cannot_move/too_elderly, groups_cannot_move, groups_cannot_move/elderly_women_60, groups_cannot_move/elderly_men_60",
                    "reasons_cannot_move, reasons_cannot_move/physically_disabled, groups_cannot_move, groups_cannot_move/pwd",
                    "reasons_cannot_move, reasons_cannot_move/clan_discrimination, groups_cannot_move, groups_cannot_move/minority_clans",
                    "reasons_cannot_move, reasons_cannot_move/no_male_company, groups_cannot_move, groups_cannot_move/girls_under18, groups_cannot_move/adult_women_18_59, groups_cannot_move/elderly_women_60",
                    
                    # logic checks on crop losses
                    "reason_crop_loss, reason_crop_loss/drought, shocks, shocks/drought",
                    "reason_crop_loss, reason_crop_loss/flooding, shocks, shocks/flooding",
                    "reason_crop_loss, reason_crop_loss/locusts_pests, shocks, shocks/locusts_pests",
                    "reason_crop_loss, reason_crop_loss/temp_too_high, reason_crop_loss/temp_too_low",
                    
                    # logic checks on livestock decrease
                    "reason_livestock_decrease, reason_livestock_decrease/drought, shocks, shocks/drought",
                    "reason_livestock_decrease, reason_livestock_decrease/flooding, shocks, shocks/flooding",
                    "reason_livestock_decrease, reason_livestock_decrease/livestock_disease_outbreak, shocks, shocks/livestock_disease_outbreak",
                    
                    # logic checks for education
                    "education_facility_available, average_time",
                    "proportion_512_attend_school_boys_past_4wks, barriers_access_education_boys_512, barriers_access_education_boys_512/no_barriers",
                    "proportion_512_attend_school_girls_past_4wks, barriers_access_education_girls_512, barriers_access_education_girls_512/no_barriers",
                    "proportion_1317_attend_school_boys_past_4wks, barriers_access_education_boys_1317, barriers_access_education_boys_1317/no_barriers",
                    "proportion_1317_attend_school_girls_past_4wks, barriers_access_education_girls_1317, barriers_access_education_girls_1317/no_barriers",
                    
                    # logic checks for protection
                    "safety_concerns, safety_concerns/fgm, groups_concerns, groups_concerns/mostly_girls"
                    )
)

#################################################################################################
########################## excluded_questions Columns For Outlier Check ##########################
##################################################################################################

# below we check numerical data for "outliers" - we exclude any questions with non-numerical answers and put the names of those questions into "excluded_questions"

# we should exclude all questions from outlier checks that aren't integer response types (integer is the only numerical response type)
outlier_excluded_questions <- kobo_survey %>%
                                  filter(type != 'integer') %>%
                                  pull(name) %>%
                                  unique()

# intersect between the dataset and the kobo tool questions to make sure we get a clean list
excluded_questions_in_data <- intersect(colnames(data_in_processing), outlier_excluded_questions)

########################################################################################
########################## Conduct the Checks for Data Issues ##########################
########################################################################################

# group the data by FO
group_by_fo <- data_in_processing %>%
                  dplyr::group_by(fo_in_charge_for_code)



# run all of the logic checks on the data grouped by FO
# we do this as each FO is responsible for specific districts / enumerators
checked_data_by_fo <- group_by_fo %>%
                        dplyr::group_split() %>%
                        purrr::map(~ check_duplicate(dataset = . # run the duplicate unique identifier check
                                                    ) %>%
                                     
                                     # check for outliers
                                     check_outliers(element_name = "checked_dataset",
                                                    kobo_survey = kobo_survey,
                                                    kobo_choices = kobo_choice,
                                                    cols_to_add_cleaning_log = NULL,
                                                    strongness_factor = 3,
                                                    minimum_unique_value_of_variable = NULL,
                                                    remove_choice_multiple = TRUE,
                                                    sm_separator = "/",
                                                    columns_not_to_check = c(excluded_questions_in_data, "interview_duration", "CHECK_interview_duration", "_gps_latitude","_gps_longitude","_gps_altitude","_gps_precision","_id","_index")
                                                    ) %>%
                                     
                                     # this will flag (as opposed to deleting) surveys that were too long or to short
                                     check_duration(column_to_check = "interview_duration",
                                                    uuid_column = "uuid",
                                                    log_name = "duration_log",
                                                    lower_bound = mindur_flag,
                                                    higher_bound = maxdur_flag
                                                    ) %>% 
                                     
                                     # we turn this off when clogs are created
                                     # check for PII to be deleted later
                                     # check_pii(element_name = "checked_dataset",
                                     #           uuid_column = "uuid"
                                     #           ) %>%

                                     # the only "other" option is for settlement
                                     check_others(columns_to_check = c("settlement_other")
                                                 ) %>%

                                     # conduct the logic checks  
                                     check_logical_with_list(list_of_check = check_list,
                                                             check_id_column = "name",
                                                             check_to_perform_column = "check",
                                                             columns_to_clean_column = "columns_to_clean",
                                                             description_column = "description"
                                                             )
                                  )

# Write the cleaning logs for each FO
cleaning_log <- checked_data_by_fo %>%
                  purrr::map(~ .[] %>%
                              create_combined_log() %>% 
                              add_info_to_cleaning_log(
                                dataset = "checked_dataset",
                                cleaning_log = "cleaning_log",
                                information_to_add = c("settlement", "district", "enum_code", "ki_name", "ki_phone_number", "comment")
                              )
                             )


# # the above code errors out if there are no clog entries (empty dataframe). If this is the case, we put in a filler of "ignore" to avoid the error (this is deleted in later R scripts)
# cleaning_log[[3]]$cleaning_log <- cleaning_log[[3]]$cleaning_log %>%
#                                       add_row(uuid = 'ignore', old_value = 'ignore', question = 'ignore', issue = 'ignore', check_id = 'ignore', check_binding = 'ignore', change_type = 'ignore', new_value = 'ignore', settlement = 'ignore', district = 'ignore', enum_code = 'ignore', ki_name = 'ignore', ki_phone_number = 'ignore', comment = 'ignore')


########################################################################################
############################### Write the Cleaning Logs ################################
########################################################################################

# write an excel file summarizing the the data issues found. this will include 3 tabs

# checked_dataset: the original dataset with 5 additional check columns (two for duration, one each for the 3 logic checks above)
# cleaning_log: all of the issues spotted with the original value, question, uuid, and issue. Also some pretty colors
# readme: explanations of different actions we could take to remedy the data issues found
# Define the directory path


# write to each FO's cleaning log folder
cleaning_log %>% purrr::map(~ create_xlsx_cleaning_log(.[], 
                                                       cleaning_log_name = "cleaning_log",
                                                       change_type_col = "change_type",
                                                       column_for_color = "check_binding",
                                                       header_front_size = 10,
                                                       header_front_color = "#FFFFFF",
                                                       header_fill_color = "#ee5859",
                                                       header_front = "Calibri",
                                                       body_front = "Calibri",
                                                       body_front_size = 10,
                                                       use_dropdown = F,
                                                       sm_dropdown_type = "numerical",
                                                       kobo_survey = kobo_survey,
                                                       kobo_choices = kobo_choice,
                                                       output_path = paste0("04_data_cleaning/",
                                                                            unique(.[]$checked_dataset$fo_in_charge_for_code),
                                                                            " - Clogs/",
                                                                            "cleaning_log_",
                                                                            unique(.[]$checked_dataset$fo_in_charge_for_code),
                                                                            "_",
                                                                            date_time_now,
                                                                            ".xlsx")
                                                       )
                            )

# make copies of the clogs for archive purposes
cleaning_log %>% purrr::map(~ create_xlsx_cleaning_log(.[], 
                                                       cleaning_log_name = "cleaning_log",
                                                       change_type_col = "change_type",
                                                       column_for_color = "check_binding",
                                                       header_front_size = 10,
                                                       header_front_color = "#FFFFFF",
                                                       header_fill_color = "#ee5859",
                                                       header_front = "Calibri",
                                                       body_front = "Calibri",
                                                       body_front_size = 10,
                                                       use_dropdown = F,
                                                       sm_dropdown_type = "numerical",
                                                       kobo_survey = kobo_survey,
                                                       kobo_choices = kobo_choice,
                                                       output_path = paste0("04_data_cleaning/_clean_files_for_archive/cleaning_log_",
                                                                            unique(.[]$checked_dataset$fo_in_charge_for_code),
                                                                            "_",
                                                                            date_time_now,
                                                                            ".xlsx")
                                                       )
                            )


###################################################################################################
############################### Write the new contact information #################################
###################################################################################################

# each KI will be asked if they have other potential contacts for the HSM, those are grouped by FO similar to the clogs
contact_data_by_fo <- group_by_fo %>%
                        dplyr::group_split() %>%
                        purrr::map(~ filter(., referral_yn == "yes") %>%
                                     select(fo_in_charge_for_code, enum_base, district, settlement, ki_name, ki_phone_number, referral_name, referral_phone, referral_second) %>%
                                     dplyr::rename(base = enum_base,
                                                   referrer_name = ki_name,
                                                   referrer_number = ki_phone_number,
                                                   contact_name = referral_name,
                                                   contact_number_1 = referral_phone,
                                                   contact_number_2 = referral_second
                                     )
                        )


# same filler as above for clogs
# contact_data_by_fo[[3]] <- contact_data_by_fo[[3]] %>%
#                               add_row(fo_in_charge_for_code = 'muna', base = 'ignore', district = 'ignore', settlement = 'ignore', referrer_name = 'ignore', referrer_number = 'ignore', contact_name = 'ignore', contact_number_1 = 'ignore', contact_number_2 = 'ignore')
# Define the first directory and file path
dir_path1 <- "05_new_contact_information/ahad - new contacts"
file_path1 <- file.path(dir_path1, "contact_info_ahad_Jul_22_2024_080730.xlsx")

# Define the second directory and file path
dir_path2 <- "05_new_contact_information/ - new contacts"
file_path2 <- file.path(dir_path2, "contact_info__Jul_22_2024_080730.xlsx")

# Create the directories if they don't exist
if (!dir.exists(dir_path1)) {
  dir.create(dir_path1, recursive = TRUE)
}
if (!dir.exists(dir_path2)) {
  dir.create(dir_path2, recursive = TRUE)
}

# Now you can create the files
file.create(file_path1)
file.create(file_path2)


# need to make this go to the correct folders
contact_data_by_fo %>% purrr::map( ~write.xlsx(.,
                                               paste0("05_new_contact_information/",
                                                      unique(.[]$fo_in_charge_for_code),
                                                      " - new contacts/contact_info_",
                                                      unique(.[]$fo_in_charge_for_code),
                                                      "_",
                                                      date_time_now,
                                                      ".xlsx")
                                              )
                                  )

# write them all to a separate location to be consolidated later
contact_data_by_fo %>% purrr::map( ~write.xlsx(.,
                                               paste0("05_new_contact_information/_all_contacts_for_next_cycle/day_by_day/contact_info_",
                                                      unique(.[]$fo_in_charge_for_code),
                                                      "_",
                                                      date_time_now,
                                                      ".xlsx")
                                               )
                                   )

########################################################################################
############################## Check For Soft Duplicates ###############################
########################################################################################

# this is to check for surveys that are too similar to one another. These surveys will be flagged to FOs for enumerator follow up

# group the surveys by enumerator
group_by_enum <- data_in_processing %>%
                    filter(!(enum_code %in% c("45861"))) %>% # if an enumerator only has 1 survey (likely due to a typo), the code will error out so remove certain enum_codes as necessary
                    dplyr::group_by(enum_code)

# check number or surveys per enumerators - check_soft_duplicates errors out if an enumerator has just 1 survey 
surveys_per_enum <- group_by_enum %>%
                      summarise(num_surveys = n()) %>%
                      arrange(num_surveys)

# check the surveys for similarities
soft_per_enum <- group_by_enum %>%
                    dplyr::group_split() %>%
                    purrr::map(~ check_soft_duplicates(dataset = .,
                                                       kobo_survey = kobo_survey,
                                                       uuid_column = "uuid",
                                                       idnk_value = "dnk",
                                                       sm_separator = "/",
                                                       log_name = "soft_duplicate_log",
                                                       threshold = 10
                                                       )
                               )

# recombine the similar survey data
similar_surveys <- soft_per_enum %>%
                      purrr::map(~ .[["soft_duplicate_log"]]) %>%
                      purrr::map2(
                        .y = dplyr::group_keys(group_by_enum) %>% unlist(),
                        ~ dplyr::mutate(.x, enum = .y)
                      ) %>%
                      do.call(dplyr::bind_rows, .)

similar_surveys_with_info <- similar_surveys %>%
                                left_join(data_in_processing, by = "uuid") %>%
                                select(fo_in_charge_for_code, district, settlement, ki_interview, start, end, uuid, issue, enum, num_cols_not_NA, total_columns_compared, num_cols_dnk, id_most_similar_survey, number_different_columns)

similar_survey_raw_data <- data_in_processing %>%
                              filter(uuid %in% (similar_surveys_with_info$uuid))


similar_survey_export_path <- paste0("04_data_cleaning/_similar_survey_checks/similar_surveys_", date_time_now, ".xlsx")

# create a workbook with our data
wb <- createWorkbook()
addWorksheet(wb, "similar_surveys")
addWorksheet(wb, "similar_survey_raw_data")

writeData(wb, 1, similar_surveys_with_info)
writeData(wb, 2, similar_survey_raw_data)

saveWorkbook(wb, similar_survey_export_path, overwrite = TRUE)


