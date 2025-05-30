rm(list=ls())
library(readxl)
library(tidyverse)
library(openxlsx)
library(cleaningtools)


raw_kobo_data <- read_excel("03_output/06_final_cleaned_data/SOM_HSM_Output.xlsx", sheet = "raw_data")
clean_kobo_data <- read_excel("03_output/06_final_cleaned_data/SOM_HSM_Output.xlsx", sheet = "cleaned_data")
cleaning_log_final <- read_excel("03_output/07_combined_cleaning_log/combined_cleaning_log.xlsx")
deletion_log_final <- read_excel("03_output/07_combined_cleaning_log/combined_deletion_log.xlsx")

# ──────────────────────────────────────────────────────────────────────────────
# 1. Review cleaning
# ──────────────────────────────────────────────────────────────────────────────

review_clog <- cleaningtools::review_cleaning_log(raw_kobo_data,
                                                  raw_data_uuid_column = "uuid",
                                                  cleaning_log = cleaning_log_final,
                                                  cleaning_log_uuid_column = "uuid",
                                                  cleaning_log_question_column = "question",
                                                  cleaning_log_new_value_column = "new_value",
                                                  cleaning_log_change_type_column = "change_type",
                                                  change_response_value = "change_response"
)


# ──────────────────────────────────────────────────────────────────────────────
# 2.Check for clog and data discrepancy
# ──────────────────────────────────────────────────────────────────────────────

review_cleaning <- review_cleaning(raw_kobo_data,
                                   raw_dataset_uuid_column = "uuid",
                                   clean_kobo_data,
                                   clean_dataset_uuid_column = "uuid",
                                   cleaning_log = cleaning_log_final,
                                   cleaning_log_uuid_column = "uuid",
                                   cleaning_log_change_type_column = "change_type",
                                   cleaning_log_question_column = "question",
                                   cleaning_log_new_value_column = "new_value",
                                   cleaning_log_old_value_column = "old_value",
                                   cleaning_log_added_survey_value = "added_survey",
                                   cleaning_log_no_change_value = c("no_action", "no_change"),
                                   deletion_log = deletion_log_final,
                                   deletion_log_uuid_column = "uuid",
                                   check_for_deletion_log = T
)



# ──────────────────────────────────────────────────────────────────────────────
# 3. Produce an output with all the reviews
# ──────────────────────────────────────────────────────────────────────────────

# Function to read and convert all columns to character and adds a file path
read_and_clean_uuid <- function(file, sheet) {
  read_excel(file, sheet = sheet) %>%
    mutate(across(everything(), as.character)) %>%  # Convert all columns to character
    mutate(file_path = file) %>% 
    select(uuid, file_path)
}

# Read and combine all files into a single dataframe
cleaning_logs_uuid <- map_dfr(file_list, sheet = 'cleaning_log', read_and_clean_uuid)

first_review <- review_cleaning %>% 
  left_join(cleaning_logs_uuid %>% distinct())

write.xlsx(first_review,"03_output/11_clog_review/clog_review.xlsx")
