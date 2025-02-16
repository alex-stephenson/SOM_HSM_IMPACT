## This function creates the parent column based on the child binaries - 
## but it should be deprecated as there is a cleaningtools function that will do this.

rm(list = ls())


#combined_clogs <- readxl::read_excel(r"(outputs/combined_clean_data/hassan_combined_corrected_clogs_Jan_05_2025_101333.xlsx)", sheet = "cleaning_log")


combined_clogs <- readxl::read_excel(r"(C:\Users\alex.stephenson\Downloads/combined_corrected_clogs_2025-01-06.xlsx)", sheet = "cleaning_log")
combined_clogs <- combined_clogs %>%
  distinct(uuid,old_value, question,change_type,
     new_value, enum_code, .keep_all = T)

change_response <- combined_clogs %>%
  filter(change_type == "change_response")

change_response <- change_response %>%
  mutate(question_stem = str_extract(question, "^[^/]+"))

data <- change_response

#change_response %>%
#  filter(uuid == "02f09260-237c-45d0-a191-2dc784092f93") %>% View()
## remove check binding fom here and see if it works

#example_issue <- combined_clogs %>%
  filter(uuid == "ec98f27d-2adc-4413-93a6-8638bdbebf45")

#data <-change_response

  update_parent_values <- function(data) {
    # Identify the parent rows (those without '/') and child rows (those with '/')
    data <- data %>%
      mutate(
        is_parent = !str_detect(question, "/"),
        child_option = ifelse(!is_parent, str_remove(question, ".*/"), NA)
      ) 
    
    # Get the parent rows that have missing child options
    missing_children <- data %>%
      filter(!is_parent, new_value == "1") %>%
      select(uuid, question_stem, child_option) %>%
      distinct()
    
    # Ensure the 'child_option' is retained after the join
    data_updated <- data %>%
      filter(is_parent) %>%
      left_join(missing_children, by = c("uuid", "question_stem")) %>%
      group_by(uuid, question_stem) %>%
      mutate(
        # Combine the parent value and missing child options
        updated_new_value = paste(
          unique(c(str_split(new_value, "\\s+")[[1]], child_option.x, child_option.y)),
          collapse = " "
        )
      ) %>%
      ungroup()
    
    # Check if the 'updated_new_value' exists and fix the issue by using proper column names
    if("updated_new_value" %in% colnames(data_updated)) {
      data_final <- data_updated %>%
        bind_rows(data %>% filter(!is_parent)) %>%
        distinct() %>%
        filter(!is.na(uuid) & !is.na(question)) %>%
        arrange(uuid, question)
    } else {
      stop("Error: 'updated_new_value' column is not present in the data.")
    }
    
    return(data_final)
  }
  

  resolved_data <- update_parent_values(change_response) %>%
    select(question, check_binding, select_multiple_value = updated_new_value) %>%
    filter(!str_detect(question, "/"))
  
  parent_q_correct <- change_response %>%
    left_join(resolved_data, by = join_by(question == question, check_binding == check_binding)) 
  
  
  combined_new_answers <- parent_q_correct %>%
    mutate(new_value = ifelse(is.na(select_multiple_value), new_value, select_multiple_value),
           new_value = str_replace_all(new_value, "NA", ""),
           new_value = str_squish(new_value)) %>%
    distinct()
  
combined_new_answers %>% writexl::write_xlsx(r"(C:\Users\alex.stephenson\Downloads/combined_corrected_clogs.xlsx)")
  
  

  combined_new_answers %>%
    filter(uuid == "02f09260-237c-45d0-a191-2dc784092f93") %>%
    pull(new_value)
  
  data_updated %>%
    filter(uuid == "02f09260-237c-45d0-a191-2dc784092f93") %>% View()
    pull(new_value)
  