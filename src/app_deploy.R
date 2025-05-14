rm(list = ls())

library(dplyr)
library(ggplot2)
library(readxl)
library(readr)
library(purrr)
library(tidyr)
library(stringr)
library(lubridate)
library(shiny)
library(shinydashboard)
library(reactable)

print("----------PACAKAGES SUCCESSFULLY LOADED-----------------")

### read in relevant data
message("Loading data...")
site_data <- readxl::read_excel("02_input/site_opz_district_data.xlsx") %>% 
  rename(operational_zone = OPZ) %>% 
  mutate(district = stringr::str_to_lower(district)) %>% 
  filter(! str_detect(name, "other"))


### clean data

message("Loading cleaned data...")

tryCatch({
  clean_data <- readxl::read_excel("03_output/06_final_cleaned_data/SOM_HSM_Output.xlsx", sheet =  "cleaned_data")
}, error = function(e) {
  message("❌ Failed to load clean Kobo data: ", e$message)
})

message("successfully loaded clean data")

### deletion log

all_dlogs <- readxl::read_excel("03_output/07_combined_cleaning_log/combined_deletion_log.xlsx")

print("----------DATA SUCCESSFULLY LOADED-----------------")


### FO Data
fo_district_mapping <- read_excel("inputs/fo_base_assignment_1224.xlsx") %>%
  select(district_name = district, "district" = district_for_code, "fo" = fo_in_charge_for_code) %>% 
  mutate(district = stringr::str_to_lower(district))

clean_data <- clean_data %>% 
  mutate(district = tolower(district)) %>%
  left_join(fo_district_mapping)
  

#--------------------------------------------------------
# Site level Completion
#--------------------------------------------------------


sampling_df <- site_data %>% count(district, operational_zone, name = "Total_Surveys") %>% 
  left_join(fo_district_mapping)



idp_count <- clean_data %>%
  mutate(district = tolower(district)) %>% 
  count(district, operational_zone, name = "Surveys_Done") 


KIIs_Done <- sampling_df %>%
  left_join(idp_count, by = c("district", "operational_zone")) %>%
  select(district, operational_zone, fo, Surveys_Done, Total_Surveys) %>% 
  mutate(Complete = ifelse(Surveys_Done >= Total_Surveys, "Yes", "No"))


KIIs_Done %>%
  writexl::write_xlsx(., "03_output/10_dashboard_output/completion_report.xlsx")

## Completion by FO

completion_by_FO <- KIIs_Done %>%
  group_by(fo) %>%
  summarise(total_surveys = sum(Total_Surveys, na.rm = T),
            total_done = sum(Surveys_Done, na.rm = T)) %>%
  mutate(Completion_Percent = round((total_done / total_surveys) * 100, 1)) %>%
  mutate(Completion_Percent = ifelse(Completion_Percent > 100, 100, Completion_Percent))

completion_by_FO %>% 
  writexl::write_xlsx("03_output/10_dashboard_output/completion_by_FO.xlsx")

### OPZ burndown

total_tasks <- sum(KIIs_Done$Total_Surveys)

actual_burndown <- clean_data %>%
  mutate(today = as.Date(today),
         Day = as.integer(today - min(today)) + 1) %>%  # Calculate day number s
  group_by(Day) %>%  # Group by FO, Region, and District
  summarise(
    Tasks_Completed = n(),  # Count tasks completed on each day
    .groups = "drop"
  ) %>%
  mutate(
    Remaining_Tasks = total_tasks - cumsum(Tasks_Completed)  # Calculate running total
  )

actual_burndown %>% 
  write_csv(., "03_output/10_dashboard_output/actual_burndown.csv")


### enumerator performance

deleted_data <- all_dlogs %>%
  count(enum_code, name = "deleted") %>%
  mutate(enum_code = as.character(enum_code))

valid_data <- clean_data %>%
  count(enum_code, name = "valid") %>%
  mutate(enum_code = as.character(enum_code))

enum_performance <- deleted_data %>%
  full_join(valid_data) %>%
  mutate(valid = replace_na(valid, 0),
         deleted = replace_na(deleted, 0),
         total = deleted + valid, 
         pct_valid = round((valid / (deleted + valid)) * 100)) %>%
  filter(total > 5)


mean_per_day <- clean_data %>%
  group_by(fo, enum_code, today, district) %>%
  summarise(n = n()) %>%
  ungroup() %>%
  group_by(fo, enum_code, district) %>%
  summarise("Average per day" = round(mean(n))) %>%
  mutate(enum_code = as.character(enum_code))

enum_performance <- enum_performance %>%
  left_join(mean_per_day) %>%
  select(fo, enum_code, district, valid, deleted, total, pct_valid, `Average per day`)

enum_performance %>% 
  write_csv(., "03_output/10_dashboard_output/enum_performance.csv")




###########################

deploy_app_input <- list.files(full.names = T, recursive = T) %>%
  keep(~ str_detect(.x, "03_output/10_dashboard_output/enum_performance.csv") 
       | str_detect(.x, "03_output/10_dashboard_output/actual_burndown.csv")
       | str_detect(.x, "03_output/10_dashboard_output/completion_by_FO.csv")
       | str_detect(.x, "03_output/09_completion_report/completion_report.xlsx")
       | str_detect(.x, "app.R")
       
  )

rsconnect::deployApp(appFiles =deploy_app_input, 
                     appDir = ".",
                     appPrimaryDoc = "./src/app.R",
                     appName = "REACH_SOM_HSM_Field_Dashboard", 
                     account = "impact-initiatives")
