rm(list = ls())
### post collection checks

library(tidyverse)
library(cleaningtools)
library(readxl)

date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")

# ──────────────────────────────────────────────────────────────────────────────
# 1. Read in all the data
# ──────────────────────────────────────────────────────────────────────────────

## FO data
fo_district_mapping <- read_excel("inputs/fo_base_assignment_1224.xlsx") %>%
  select(district_for_code, fo_in_charge_for_code) %>%
  dplyr::rename("district" = "district_for_code") %>%
  mutate_all(tolower)

## raw data
raw_kobo_data <- read_xlsx("03_output/01_raw_data/raw_kobo_output.xlsx")

## tool
kobo_tool_name <- "04_tool/REACH_SOM_HSM_MAY_25_TOOL.xlsx"
kobo_survey <- read_excel(kobo_tool_name, sheet = "survey")
kobo_choice <- read_excel(kobo_tool_name, sheet = "choices")

##geo data
geo_data <- readxl::read_excel("02_input/District_Region_Ref_Data.xlsx") %>% 
  mutate(Pcode = str_replace_all(Pcode, " ", "_")) %>% 
  select(settlement_name = name, settlement = Pcode)


# read in the survey questions / choices
kobo_survey <- read_excel(kobo_tool_name, sheet = "survey") %>% 
  mutate(type = stringr::str_squish(type))

## Read in clogs
all_files <- list.files(path = "01_cleaning_logs", recursive = TRUE, full.names = TRUE)

file_list <- all_files %>%
  keep(~ str_detect(.x, "/completed_validated/") & str_detect(.x, "cleaning_log.*\\.xlsx$"))

# Function to read and convert all columns to character
read_and_clean <- function(file, sheet) {
  read_excel(file, sheet = sheet) %>%
    mutate(across(everything(), as.character))  # Convert all columns to character
}

# Read and combine all files into a single dataframe
cleaning_logs <- map_dfr(file_list, sheet = 'cleaning_log', read_and_clean)

cleaning_logs <- cleaning_logs %>%
  filter(!is.na(change_type)) ### this needs taking out at the end

## also check all dlogs

deletion_exclusions <- read_xlsx("03_output/08_similar_survey_check/exclusions.xlsx")

all_dlogs <- readxl::read_excel(r"(03_output/03_deletion_log/deletion_log.xlsx)", col_types = "text")
manual_dlog <- readxl::read_excel("03_output/04_deletion_log_manual/HSM_Manual_Deletion_Log.xlsx", col_types = "text")
cleaning_log_deletions <- cleaning_logs %>% 
  filter(change_type == "remove_survey") %>% 
  mutate(interview_duration = "") %>% 
  select(uuid, settlement, enum_code,interview_duration, comment = issue)

all_deletions <- bind_rows(all_dlogs, manual_dlog) %>% 
  bind_rows(cleaning_log_deletions) %>% 
  filter(! uuid %in% deletion_exclusions$uuid)


raw_kobo_data_nas <- raw_kobo_data %>%
  mutate(interview_duration = NA)

# ──────────────────────────────────────────────────────────────────────────────
# 2. Create the clean data
# ──────────────────────────────────────────────────────────────────────────────

my_clean_data <- create_clean_data(raw_dataset = raw_kobo_data_nas,
                                   raw_data_uuid_column = "uuid",
                                   cleaning_log = cleaning_logs, 
                                   cleaning_log_uuid_column = "uuid",
                                   cleaning_log_question_column = "question",
                                   cleaning_log_new_value_column = "new_value",
                                   cleaning_log_change_type_column = "change_type")

my_clean_data_parentcol <- recreate_parent_column(dataset = my_clean_data,
                                                  uuid_column = "uuid",
                                                  kobo_survey = kobo_survey,
                                                  kobo_choices = kobo_choice,
                                                  sm_separator = "/", 
                                                  cleaning_log_to_append = cleaning_logs)



cleaning_log <- my_clean_data_parentcol$cleaning_log

my_clean_data <- my_clean_data_parentcol$data_with_fix_concat %>%
  filter(! uuid %in% all_deletions$uuid) %>% 
  left_join(geo_data) %>% 
  relocate(settlement_name, .after = settlement)

cleaning_log_final <- cleaning_log %>% 
  filter(! uuid %in% all_deletions$uuid)

# ──────────────────────────────────────────────────────────────────────────────
# 4. Produce all the outputs
# ──────────────────────────────────────────────────────────────────────────────

all_clean_data <- list("cleaned_data" = my_clean_data, "raw_data" = raw_kobo_data)

my_clean_data %>%
  writexl::write_xlsx(., paste0('03_output/05_daily_cleaned_data/all_clean_data_', today(), '.xlsx'))

#my_clean_data %>%
#  select(!contains('/')) %>%
#  writexl::write_xlsx(., paste0('03_output/05_daily_cleaned_data/all_clean_data_dashboard.xlsx'))

all_clean_data %>% 
  writexl::write_xlsx(., "03_output/06_final_cleaned_data/SOM_HSM_Output.xlsx")

cleaning_log_final %>% 
  writexl::write_xlsx(., "03_output/07_combined_cleaning_log/combined_cleaning_log.xlsx")

all_deletions %>% 
  writexl::write_xlsx(., "03_output/07_combined_cleaning_log/combined_deletion_log.xlsx")





# ──────────────────────────────────────────────────────────────────────────────
# 5. Review soft duplicates
# ──────────────────────────────────────────────────────────────────────────────

## read in already approved ones
exclusions <- read_excel("03_output/08_similar_survey_check/exclusions.xlsx")

my_clean_data_added <- my_clean_data %>%
  left_join(fo_district_mapping)

enum_typos <- my_clean_data_added %>%
  dplyr::count(enum_code) %>%
  filter(n >= 5) %>%
  pull(enum_code)

group_by_enum <- my_clean_data_added %>%
  filter(enum_code %in% enum_typos) %>%
  group_by(enum_code)

soft_per_enum <- group_by_enum %>%
  dplyr::group_split() %>%
  purrr::map(~ check_soft_duplicates(dataset = .,
                                     kobo_survey = kobo_survey,
                                     uuid_column = "uuid",
                                     idnk_value = "dnk",
                                     sm_separator = "/",
                                     log_name = "soft_duplicate_log",
                                     threshold = 5
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
  left_join(my_clean_data_added, by = "uuid") %>%
  left_join(my_clean_data_added %>% select(uuid, similiar_survey_date = today), by = join_by("id_most_similar_survey" == "uuid")) %>%
  select(district, settlement, fo_in_charge_for_code, today, start, end, uuid, issue, enum_code, num_cols_not_NA, total_columns_compared, num_cols_dnk, similiar_survey_date, id_most_similar_survey, number_different_columns) %>% 
  filter(! uuid %in% exclusions$uuid & ! id_most_similar_survey %in% exclusions$id_most_similar_survey,
         today == similiar_survey_date)


similar_survey_raw_data <- my_clean_data %>%
  filter(uuid %in% (similar_surveys_with_info$uuid))

similar_survey_export_path <- paste0("03_output/similar_survey_checks/similar_surveys_", today(), ".xlsx")

# create a workbook with our data
wb <- createWorkbook()
addWorksheet(wb, "similar_surveys")
addWorksheet(wb, "similar_survey_raw_data")

writeData(wb, 1, similar_surveys_with_info)
writeData(wb, 2, similar_survey_raw_data)

saveWorkbook(wb, similar_survey_export_path, overwrite = TRUE)





