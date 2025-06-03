# ──────────────────────────────────────────────────────────────────────────────
########################## Setup ##########################
# ──────────────────────────────────────────────────────────────────────────────

# start with a clean slate
rm(list = ls())

date_to_filter <- "2025-06-03"

# load up our packages
library(cleaningtools)
library(tidyverse)
library(readxl)
library(ImpactFunctions)
library(openxlsx)
#devtools::install_github("alex-stephenson/ImpactFunctions")
# get the timestamp to more easily keep track of different versions
date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")
# install.packages("ImpactFunctions")
#  install.packages("devtools")

# ──────────────────────────────────────────────────────────────────────────────
# 1. Download the data from kobo 
# ──────────────────────────────────────────────────────────────────────────────

raw_kobo_data <- ImpactFunctions::get_kobo_data(asset_id = "amqYCpvrmCdZaUGmWwrMMv", un = "abdirahmanaia") %>% 
  select(-uuid) %>% 
  rename(uuid = `_uuid`)

version_count <- n_distinct(raw_kobo_data$`__version__`)
if (version_count > 1) {
  print(" !!!!!!!!!! /n There are multiple versions of the tool in use /n !!!!!!!!!!!!!!!!!")
}

kobo_data_export_path <- paste0("03_output/01_raw_data/raw_kobo_output.xlsx")
writexl::write_xlsx(raw_kobo_data, kobo_data_export_path)


########## read in the tool ###########

kobo_tool_name <- "04_tool/REACH_SOM_HSM_MAY_25_TOOL.xlsx"
kobo_survey <- read_excel(kobo_tool_name, sheet = "survey")
kobo_choice <- read_excel(kobo_tool_name, sheet = "choices")

######### read in some additional geo data

geo_data <- readxl::read_excel("02_input/District_Region_Ref_Data.xlsx") %>% 
  mutate(Pcode = str_replace_all(Pcode, " ", "_")) %>% 
  select(settlement_name = name, settlement = Pcode)

# ──────────────────────────────────────────────────────────────────────────────
# 2. Start Processing the Data
# ──────────────────────────────────────────────────────────────────────────────

# clean up the raw data before doing further processing
data_in_processing <- raw_kobo_data %>%
  mutate(comment = "",
         district = tolower(district)) %>%
  left_join(geo_data) %>% 
  relocate(settlement_name, .after = settlement)
  


# ──────────────────────────────────────────────────────────────────────────────#
# 3. FO / District Mapping
# ──────────────────────────────────────────────────────────────────────────────#

# join the Field Officer / District assignments 

# read in the FO/district mapping
fo_district_mapping <- read_excel("02_input/fo_base_assignment_1224.xlsx") %>%
                          select(district_for_code, fo_in_charge_for_code) %>%
                          dplyr::rename("district" = "district_for_code") %>%
                          mutate_all(tolower)

# join the fo to the dataset - this way the code knows which surveys belong to which FOs
data_in_processing <- data_in_processing %>%
                        left_join(fo_district_mapping, by = "district") 

# ──────────────────────────────────────────────────────────────────────────────
# 4. Remove "Bad" Surveys
# ──────────────────────────────────────────────────────────────────────────────

 # set the minimum and maximum "reasonable" survey times. Anything outside of this will be flagged
 mindur_delete <- 20
 mindur_flag <- 30
 maxdur_flag <- 70
 maxdur_delete <- 80
 
# Remove as many surveys as we can before creating the cleaning logs. For example, if a survey took 180 minutes, we know its going to be deleted so we shouldn't waste any enumerator time having the correct issues
 kobo_data_metadata <- get_kobo_metadata(dataset = data_in_processing, un = "abdirahmanaia", asset_id = "amqYCpvrmCdZaUGmWwrMMv")
 data_in_processing <- kobo_data_metadata$df_and_duration

 raw_metadata_length <- kobo_data_metadata$audit_files_length
 
 write_rds(raw_metadata_length, "03_output/01_raw_data/raw_metadata.rds")

 data_in_processing <- data_in_processing %>%
   mutate(length_valid = case_when(
     interview_duration < mindur_delete ~ "Too short",
     interview_duration > maxdur_delete ~ "Too long",
     TRUE ~ "Okay"
   ))
 

## produce an output for tracking how many surveys are being deleted
 data_in_processing %>%
   count(fo_in_charge_for_code, length_valid) %>%
   pivot_wider(names_from = length_valid, values_from = n) %>%
   mutate(Okay = replace_na(Okay, 0),
          `Too long` = replace_na(`Too long`, 0),
          `Too short` = replace_na(`Too short`, 0),
          total = Okay + `Too long` + `Too short`) %>%
   writexl::write_xlsx(., paste0('03_output/02_time_checks/time_check.xlsx')) 

 
 time_issue <- data_in_processing %>%
   filter(length_valid != "Okay") %>% 
   select(uuid, settlement_name, settlement, enum_code, interview_duration) %>%
   mutate(comment = 'Interview length too short or too long')
 
 surveys_to_remove_cck <- data_in_processing %>%
   filter(!is.na(ki_termination_reason)) %>%
   select(uuid, settlement_name, settlement, enum_code, interview_duration) %>%
   mutate(comment = "no KI consent/continue/knowledge")

 no_community <- data_in_processing %>%
   filter(community_in_settlement == "no") %>%
   select(uuid, settlement_name, settlement, enum_code, interview_duration) %>%
   mutate(comment = "no community in settlement")
 
 last_visit <- data_in_processing %>% 
   filter(ki_last_speak_to_residents == "more_than_a_month") %>% 
   select(uuid, settlement_name, enum_code, interview_duration) %>%
   mutate(comment = "Ki last visit > month")
 
 all_deletions <- time_issue %>% 
   bind_rows(surveys_to_remove_cck) %>% 
   bind_rows(no_community) %>% 
   bind_rows(last_visit) %>% 
   distinct(.keep_all = T)
 
 all_deletions %>% 
   writexl::write_xlsx(., paste0("03_output/03_deletion_log/deletion_log.xlsx"))
 
 ## filter only valid surveys and for the specific date
 data_in_processing <- data_in_processing %>%
   filter(! uuid %in% all_deletions$uuid) %>%
   filter(today == date_to_filter)
 

 

# ──────────────────────────────────────────────────────────────────────────────
# 5. Define Checks for Logic Errors 
# ──────────────────────────────────────────────────────────────────────────────
 

 check_list <- data.frame(
                    name = c(
                    # logic checks on reason_moved
                    "moved_lack_of_rain_check_code",
                    "moved_flood_check_code",
                    "moved_pest_invasion_check_code",
                    "moved_disease_outbreak_check_code",
                    "moved_general_conflict_check_code",
                    
                    # logic checks for cannot move / cannot leave
                    "leave_elderly_check",
                    "leave_disability_check",
                    "leave_minority_clan_check",
                    "leave_woman_check",
                
                    # logic checks on crop losses
                    "crop_loss_lack_of_rain_check_code",
                    #"crop_loss_flooding_check",
                    "crop_loss_pest_invasion_check_code",
                    "crop_loss_temp_check",
                
                    # logic checks on livestock decrease
                    "livestock_decrease_lack_of_rain_check_code",
                    "livestock_decrease_flood_check_code_code",
                    "livestock_decrease_disease_check_code",
                
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
                    "grepl(\"*lack_of_rain*\", reason_moved) & !grepl(\"*lack_of_rain*\", shocks)",
                    "grepl(\"*flood*\", reason_moved) & !grepl(\"*flood*\", shocks)",
                    "grepl(\"*pest*\", reason_moved) & !grepl(\"*pest*\", shocks)",
                    "grepl(\"*human_disease*\", reason_moved) & !grepl(\"*human_disease*\", shocks)",
                    "grepl(\"*insecurity*|*evictions*|*impediments*\", reason_moved) & !grepl(\"*insecurity*\", shocks)",
                    
                    # logic checks for cannot move / cannot leave - could have to manually update binaries
                    # cancel the logic check if entire_household is selected for reasons_cannot_move
                    "grepl(\"*too_elderly*\", groups_cannot_move) & !grepl(\"*elderly*\", reasons_cannot_move) & !grepl(\"*entire_household*\", reasons_cannot_move)",
                    "grepl(\"*physically_disabled*\", groups_cannot_move) & !grepl(\"*pwd*\", reasons_cannot_move) & !grepl(\"*entire_household*\", reasons_cannot_move)",
                    "grepl(\"*clan_discrimination*\", groups_cannot_move) & !grepl(\"*minority_clans*\", reasons_cannot_move) & !grepl(\"*entire_household*\", reasons_cannot_move)",
                    "grepl(\"*male_company*\", groups_cannot_move) & !grepl(\"*girls_under18*|*adult_women_18_59*|*elderly_women_60*\", reasons_cannot_move) & !grepl(\"*entire_household*\", reasons_cannot_move)",
                    
                    # logic checks on crop losses - could have to manually update binaries for shocks
                    "grepl(\"*lack_of_rain*\", reason_crop_loss) & !grepl(\"*lack_of_rain*\", shocks)",
                    #"grepl(\"*flood*\", reason_crop_loss) & !grepl(\"*flood*\", shocks)",
                    "grepl(\"*locusts*\", reason_crop_loss) & !grepl(\"*pest*\", shocks)",
                    "grepl(\"*temp_too_high*\", reason_crop_loss) & grepl(\"*temp_too_low*\", reason_crop_loss)",
                
                    # logic checks on livestock decrease - could have to manually update binaries for shocks
                    "grepl(\"*lack_of_rain*\", reason_livestock_decrease) & !grepl(\"*lack_of_rain*\", shocks)",
                    "grepl(\"*flood*\", reason_livestock_decrease) & !grepl(\"*flood*\", shocks)",
                    "grepl(\"*livestock_disease*\", reason_livestock_decrease) & !grepl(\"*livestock_disease*\", shocks)",
                    
                    # logic checks for education - could have to manually update binaries for barriers
                    "education_facility_available == \"no_learning_facilities_available\" & average_time == \"less_than_15min\"",
                    "grepl(\"*none|a_few*\", proportion_512_attend_school_boys_past_4wks) & grepl(\"*no_barriers*\", barriers_access_education_boys_512)",
                    "grepl(\"*none|a_few*\", proportion_512_attend_school_girls_past_4wks) & grepl(\"*no_barriers*\", barriers_access_education_girls_512)",
                    "grepl(\"*none|a_few*\", proportion_1317_attend_school_boys_past_4wks) & grepl(\"*no_barriers*\", barriers_access_education_boys_1317)",
                    "grepl(\"*none|a_few*\", proportion_1317_attend_school_girls_past_4wks) & grepl(\"*no_barriers*\", barriers_access_education_girls_1317)",
                
                    # logic checks for protection
                    "grepl(\"*fgm*\", safety_concerns) & !grepl(\"*girls*\", groups_concerns)"
                    ),
                  description = c(
                    # logic checks on reason_moved
                    "reason_moved includes lack_of_rain, but lack_of_rain was not selected in shocks",
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
                    "reason_crop_loss includes lack_of_rain, but lack_of_rain was not selected in shocks",
                    #"reason_crop_loss includes flooding, but flooding was not selected in shocks",
                    "reason_crop_loss includes pests/locusts, but pest/locust invasion was not selected in shocks",
                    "reason_crop_loss includes both temperature too high and too low",
                    
                    # logic checks on livestock decrease
                    "reason_livestock_decrease includes lack_of_rain, but lack_of_rain was not selected in shocks",
                    "reason_livestock_decrease includes flooding, but flooding was not selected in shocks",
                    "reason_livestock_decrease includes livestock disease, but livestock disease was not selected in shocks",
                    
                    # logic checks for education
                    "There is no school within 15 minutes but the average time to reach school is less than 15 minutes",
                    "Boys 5-12 attendance was none or a few, but no barriers to accessing education",
                    "Girls 5-12 attendance was none or a few, but no barriers to accessing education",
                    "Boys 13-17 attendance was none or a few, but no barriers to accessing education",
                    "Girls 13-17 attendance was none or a few, but no barriers to accessing education",
                    
                    # logic checks for protection
                    "safety_concerns is FGM, but girls/women is not selected in groups_concerns"
                    ),
                  columns_to_clean = c(
                    
                    # single choice questions don't have child columns, just a parent column with text (i.e. no binaries)
                    
                    # logic checks on reason_moved
                    "reason_moved/lack_of_rain, shocks/lack_of_rain_dryseason, shocks/lack_of_rain_rainseason",
                    "reason_moved/flooding, shocks/flooding",
                    "reason_moved/locusts_pests, shocks/locusts_pests",
                    "reason_moved/human_disease_outbreak, shocks/human_disease_outbreak",
                    "reason_moved/conflict_insecurity, reason_moved/evictions, reason_moved/unlawful_impediments, shocks/insecurity",
                
                    "reasons_cannot_move/too_elderly, groups_cannot_move/elderly_women_60, groups_cannot_move/elderly_men_60",
                    "reasons_cannot_move/physically_disabled, groups_cannot_move/pwd",
                    "reasons_cannot_move/clan_discrimination, groups_cannot_move/minority_clans",
                    "reasons_cannot_move/no_male_company, groups_cannot_move/girls_under18, groups_cannot_move/adult_women_18_59, groups_cannot_move/elderly_women_60",
                    
                    # logic checks on crop losses
                    "reason_crop_loss/lack_of_rain, shocks/lack_of_rain_dryseason, shocks/lack_of_rain_rainseason",
                    #"reason_crop_loss/flooding, shocks/flooding",
                    "reason_crop_loss/locusts_pests, shocks/locusts_pests",
                    "reason_crop_loss/temp_too_high, reason_crop_loss/temp_too_low",
                    
                    # logic checks on livestock decrease
                    "reason_livestock_decrease/lack_of_rain, shocks/lack_of_rain_dryseason, shocks/lack_of_rain_rainseason",
                    "reason_livestock_decrease/flooding, shocks/flooding",
                    "reason_livestock_decrease/livestock_disease_outbreak, shocks/livestock_disease_outbreak",
                    
                    # logic checks for education
                    "education_facility_available, average_time",
                    "proportion_512_attend_school_boys_past_4wks, barriers_access_education_boys_512, barriers_access_education_boys_512/no_barriers",
                    "proportion_512_attend_school_girls_past_4wks, barriers_access_education_girls_512, barriers_access_education_girls_512/no_barriers",
                    "proportion_1317_attend_school_boys_past_4wks, barriers_access_education_boys_1317, barriers_access_education_boys_1317/no_barriers",
                    "proportion_1317_attend_school_girls_past_4wks, barriers_access_education_girls_1317, barriers_access_education_girls_1317/no_barriers",
                    
                    # logic checks for protection
                    "safety_concerns/fgm, groups_concerns/mostly_girls, groups_concerns/mostly_adult_women, groups_concerns/mostly_elderly_women"
                    )
)

# ──────────────────────────────────────────────────────────────────────────────
# 6. excluded_questions Columns For Outlier Check 
# ──────────────────────────────────────────────────────────────────────────────

# we should exclude all questions from outlier checks that aren't integer response types (integer is the only numerical response type)
outlier_excluded_questions <- kobo_survey %>%
                                  filter(type != 'integer') %>%
                                  pull(name) %>%
                                  unique()

# intersect between the dataset and the kobo tool questions to make sure we get a clean list
excluded_questions_in_data <- intersect(colnames(data_in_processing), outlier_excluded_questions)

# ──────────────────────────────────────────────────────────────────────────────
# 7. Conduct the Checks for Data Issues
# ──────────────────────────────────────────────────────────────────────────────



# group the data by FO
group_by_fo <- data_in_processing %>%
                  dplyr::group_by(fo_in_charge_for_code)

# run all of the logic checks on the data grouped by FO
# group the data by FO
group_by_fo <- data_in_processing %>%
  dplyr::group_by(fo_in_charge_for_code)

# run all of the logic checks on the data grouped by FO
checked_data_by_fo <- group_by_fo %>%
  dplyr::group_split() %>%
  purrr::map(~ check_duplicate(dataset = . # run the duplicate unique identifier check
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
    check_others(columns_to_check = c("settlement_other")) %>%
    
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
                 information_to_add = c("settlement", "settlement_name", "district", "enum_code", "ki_name", "ki_phone_number", "comment")
               )
  )

# # the above code errors out if there are no clog entries (empty dataframe). If this is the case, we put in a filler of "ignore" to avoid the error (this is deleted in later R scripts)
# cleaning_log[[3]]$cleaning_log <- cleaning_log[[3]]$cleaning_log %>%
#                                       add_row(uuid = 'ignore', old_value = 'ignore', question = 'ignore', issue = 'ignore', check_id = 'ignore', check_binding = 'ignore', change_type = 'ignore', new_value = 'ignore', settlement = 'ignore', district = 'ignore', enum_code = 'ignore', ki_name = 'ignore', ki_phone_number = 'ignore', comment = 'ignore')


# ──────────────────────────────────────────────────────────────────────────────
# 8. Write the Cleaning Logs
# ──────────────────────────────────────────────────────────────────────────────

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
                                                       use_dropdown = T,
                                                       sm_dropdown_type = "numerical",
                                                       kobo_survey = kobo_survey,
                                                       kobo_choices = kobo_choice,
                                                       output_path = paste0("01_cleaning_logs/",
                                                                            unique(.[]$checked_dataset$fo_in_charge_for_code),
                                                                            "/cleaning_log_",
                                                                            unique(.[]$checked_dataset$fo_in_charge_for_code),
                                                                            "_",
                                                                            date_time_now,
                                                                            ".xlsx")
                                                       )
                            )



# ────────────────────────────────────────────────────────────────────────────── 
# 9. Write the new contact information
# ────────────────────────────────────────────────────────────────────────────── 

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

contact_data_by_fo <- Filter(function(x) nrow(x) > 0, contact_data_by_fo)

contact_data_by_fo %>% purrr::map( ~write.xlsx(.,
                                               paste0("03_output/11_referral_data/",
                                                      unique(.[]$fo_in_charge_for_code),
                                                      "/contact_info_",
                                                      unique(.[]$fo_in_charge_for_code),
                                                      "_",
                                                      date_time_now,
                                                      ".xlsx")
                                              )
                                  )



group_by_fo %>% 
  ungroup() %>% 
  filter(referral_yn == "yes") %>%
  select(fo_in_charge_for_code, enum_base, district, settlement, ki_name, ki_phone_number, referral_name, referral_phone, referral_second) %>%
  dplyr::rename(base = enum_base,
                referrer_name = ki_name,
                referrer_number = ki_phone_number,
                contact_name = referral_name,
                contact_number_1 = referral_phone,
                contact_number_2 = referral_second
  ) %>% 
  writexl::write_xlsx(paste0("03_output/11_referral_data/all/all_contact_data_", date_time_now, ".xlsx"))







