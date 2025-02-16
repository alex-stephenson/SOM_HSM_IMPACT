## This code is used to run up to three checks on the clogs - 
## 1. Is no action or change response logically consistent with new value and old_value
## 2. are the child actions selected valid based on the question and the survey
## 3. Have the binaries been updated based on the child column
## For the next HSM I would advise using the standard cleaningtools approach, which is to only have the child binaries and then to make the parent column after

# Load required libraries
library(dplyr)
library(tidyr)
library(readr)
library(openxlsx)
rm(list = ls())


tool_options <- function(tool_path, survey_sheet = "survey", choice_sheet = "choices") {
  
  
  kobo_tool_name <- tool_path
  
  kobo_survey <- read_excel(kobo_tool_name, sheet = survey_sheet)
  kobo_choice <- read_excel(kobo_tool_name, sheet = choice_sheet)
  
  kobo_survey_sm <- kobo_survey %>%
    filter(str_detect(type, "multiple")) %>%
    pull(name)
  
  kobo_survey_one <- kobo_survey %>%
    filter(str_detect(type, "select_one")) %>%
    select(type, name) %>%
    mutate(list_name = str_remove_all(type, "select_one "))
  
  kobo_survey_one_list <- kobo_survey_one %>%
    dplyr::rename(survey_name = name) %>%
    left_join(kobo_choice, by = join_by("list_name" == "list_name")) %>%
    select(survey_question = survey_name, survey_answer = name) %>%
    mutate(survey_options = paste0(survey_question, "/", survey_answer))
  
  q_list <- kobo_survey_one_list %>%
    select(survey_question) %>%
    distinct() %>%
    pull(survey_question)
  
  
  q_list_answers <- kobo_survey_one_list %>%
    select(survey_options) %>%
    distinct() %>%
    pull(survey_options)
}

tool_options(tool_path = kobo_tool_name)


kobo_tool_name <- "../../00_tool/SOM_REACH_HSM_December_2024_Tool_Pilot_ver_AS_EDITS_v2.xlsx"

kobo_survey <- read_excel(kobo_tool_name, sheet = "survey")
kobo_choice <- read_excel(kobo_tool_name, sheet = "choices")

kobo_survey_sm <- kobo_survey %>%
  filter(str_detect(type, "multiple")) %>%
  pull(name)

kobo_survey_one <- kobo_survey %>%
  filter(str_detect(type, "select_one")) %>%
  select(type, name) %>%
  mutate(list_name = str_remove_all(type, "select_one "))

kobo_survey_one_list <- kobo_survey_one %>%
  dplyr::rename(survey_name = name) %>%
  left_join(kobo_choice, by = join_by("list_name" == "list_name")) %>%
  select(survey_question = survey_name, survey_answer = name) %>%
  mutate(survey_options = paste0(survey_question, "/", survey_answer))

q_list <- kobo_survey_one_list %>%
  select(survey_question) %>%
  distinct() %>%
  pull(survey_question)


q_list_answers <- kobo_survey_one_list %>%
  select(survey_options) %>%
  distinct() %>%
  pull(survey_options)


## wrap it in a function to do 

process_cleaning_log <- function(tool_path, survey_sheet = "survey", choice_sheet = "choices", file_path, output_dir) {

  "%!in%" <- Negate("%in%")
  
  ## start by preparing the options available in the tool
  
  kobo_tool_name <- tool_path
  
  kobo_survey <- read_excel(kobo_tool_name, sheet = survey_sheet)
  kobo_choice <- read_excel(kobo_tool_name, sheet = choice_sheet)
  
  kobo_survey_sm <- kobo_survey %>%
    filter(str_detect(type, "multiple")) %>%
    pull(name)
  
  kobo_survey_one <- kobo_survey %>%
    filter(str_detect(type, "select_one")) %>%
    select(type, name) %>%
    mutate(list_name = str_remove_all(type, "select_one "))
  
  kobo_survey_one_list <- kobo_survey_one %>%
    dplyr::rename(survey_name = name) %>%
    left_join(kobo_choice, by = join_by("list_name" == "list_name")) %>%
    select(survey_question = survey_name, survey_answer = name) %>%
    mutate(survey_options = paste0(survey_question, "/", survey_answer))
  
  q_list <- kobo_survey_one_list %>%
    select(survey_question) %>%
    distinct() %>%
    pull(survey_question)
  
  
  q_list_answers <- kobo_survey_one_list %>%
    select(survey_options) %>%
    distinct() %>%
    pull(survey_options)
  

  
  # Extract the file name for tagging output
  clog_name <- basename(file_path)
  
  # Read the cleaning log
  cleaning_log <- read_excel(file_path, sheet = "cleaning_log") %>%
    mutate(new_value = str_replace_all(new_value, "  ", " "),
           old_value = str_replace_all(old_value, "  ", " "))
  
  # Action check
  action_check <- cleaning_log %>%
    mutate(
      clog = clog_name,
      clog_issue = case_when(
        new_value == old_value & change_type == "no_action" ~ "Correct",
        new_value != old_value & change_type == "change_response" ~ "Correct",
        new_value == old_value & change_type == "change_response" ~ "The new value and the old value are the same but the action type is change_response",
        new_value != old_value & change_type == "no_action" ~ "The new value and the old value are different but the action is no_action",
        TRUE ~ NA
      )
    ) %>%
    filter(clog_issue != "Correct")
  
  # Parent question validation
  parent_questions <- cleaning_log %>%
    filter(!str_detect(question, "/")) %>%
    mutate(
      added_options = map2_chr(
        str_split(new_value, "\\s+"),
        str_split(old_value, "\\s+"),
        ~ paste(setdiff(.x, .y), collapse = " ")
      )
    ) %>%
    filter(added_options != "")
  
  validation_results <- parent_questions %>%
    rowwise() %>%
    mutate(
      valid_children = ifelse( ## column we will use to see if a child is valid
        added_options == "", ## if no added options then we skip
        TRUE,
        all(
          str_split(added_options, "\\s+")[[1]] %>% ## split each added option into one
            map_lgl(~ { # use map_lgl to check each option 
              child_question <- paste0(question, "/", .x) # create a child_question value that will create the child questions we would expect to see, separated by /
              child_exists <- cleaning_log %>% ## now filter the cleaning log dataset to see where the the binding id and newly constructed child options are the same
                filter(
                  question == child_question,
                  check_binding == check_binding
                ) %>%
                nrow() > 0 ## if there is a match then child exists, otherwise it is FALSE (child does not exist in child rows)
              child_exists
            })
        )
      )
    ) %>%
    ungroup()
  
  summary_report <- validation_results %>%
    filter(valid_children == FALSE) %>%
    mutate(clog_issue = paste0(added_options, " is added in the clog but the corresponding score is not changed from 0 to 1."))
  
  
  # Check whether the questions are valid -- select multiple
  
  colnames = colnames(read_excel(r"(outputs\dashboard/unchecked_data_for_dashboard_Dec_19_2024_082211.xlsx)"))
  
  child_q_check <- cleaning_log %>%
    filter(question %in% kobo_survey_sm)
  
  individual_rows <- child_q_check %>%
    filter(!str_detect(question, "/"),
           change_type == "change_response") %>%
    separate_rows(new_value, sep = "\\s+") %>%
    filter(new_value != "") %>%
    mutate(question_answer = paste0(question, "/", new_value)) %>%
    filter(question_answer %!in% colnames) %>%
    mutate(clog_issue = "This option is not in the tool [select multiple]")
  
  
  # Check whether the questions are valid -- select one

  select_single_qs <- cleaning_log %>% 
    filter(question %in% q_list,
           change_type == "change_response") %>%
    mutate(question_answer = paste0(question, "/", new_value)) %>%
    filter(question_answer %!in% q_list_answers) %>%
    mutate(clog_issue = "This option is not in the tool [select one]")
  
  tool_answer_check <- rbind(individual_rows, select_single_qs)
  
  
  # Write to Excel
  output_path <- file.path(output_dir, paste0("cleaning_log_report_", clog_name))
  wb <- createWorkbook()
  addWorksheet(wb, "Action Check")
  writeData(wb, "Action Check", action_check)
  addWorksheet(wb, "Tool answer check")
  writeData(wb, "Tool answer check", tool_answer_check)
  addWorksheet(wb, "Child answer check")
  writeData(wb, "Child answer check", summary_report)
  saveWorkbook(wb, output_path, overwrite = TRUE)
  
  cat("Processed:", clog_name, "-> Output:", output_path, "\n")
  
}


hassan_finished <- r"(C:\Users\alex.stephenson\ACTED\IMPACT SOM - 01_REACH\2024_25\01_ISU\SOM1901_HSM\03_Data & Data Analysis\DEC_24\01CLeaning scripts\04_data_cleaning\ahad - Clogs\Finished_Clogs/cleaning_log_ahad_Dec_19_2024_082211.xlsx)"
hassan_review <- r"(C:\Users\alex.stephenson\ACTED\IMPACT SOM - 01_REACH\2024_25\01_ISU\SOM1901_HSM\03_Data & Data Analysis\DEC_24\01CLeaning scripts\04_data_cleaning\ahad - Clogs\Finished_Clogs/clog_review)"

process_cleaning_logs(file_path = hassan_finished, output_dir = hassan_review)

process_all_logs <- function(input_dir, output_dir) {
  files <- list.files(input_dir, full.names = TRUE, pattern = "\\.xlsx$")
  for (file_path in files) {
    process_cleaning_log(file_path, output_dir)
  }
}

process_all_logs(
  input_dir = r"(C:\Users\alex.stephenson\ACTED\IMPACT SOM - 01_REACH\2024_25\01_ISU\SOM1901_HSM\03_Data & Data Analysis\DEC_24\01CLeaning scripts\04_data_cleaning\kala - Clogs\Finished_Clogs)",
  output_dir = r"(C:\Users\alex.stephenson\ACTED\IMPACT SOM - 01_REACH\2024_25\01_ISU\SOM1901_HSM\03_Data & Data Analysis\DEC_24\01CLeaning scripts\04_data_cleaning\kala - Clogs\Finished_Clogs/clog_review)"
)

file_path <- r"(C:\Users\alex.stephenson\ACTED\IMPACT SOM - 01_REACH\2024_25\01_ISU\SOM1901_HSM\03_Data & Data Analysis\DEC_24\01CLeaning scripts\04_data_cleaning\hassan - Clogs\Finished_Clogs/cleaning_log_hassan_Dec_22_2024_084354.xlsx)"










         
