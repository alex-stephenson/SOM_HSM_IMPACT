# REACH Somalia HSM Dec23 - Results Table Formatting Script

###################################################################################
###################################### Setup ######################################
###################################################################################

rm(list = ls())

setwd('C:/Users/reid.jackson/ACTED/IMPACT SOM - 01_REACH/Unit 1 - Intersectoral/SOM1901_HSM/03_Data/2024/01_March Round')
library(readxl)
library(openxlsx)
library(dplyr)
library(tidyr)

date_time_now <- format(Sys.time(), "%a_%b_%d_%Y_%H%M%S")

######################################################################################
########################## Read in KI, HH Level & Kobo Tool ##########################
######################################################################################

ki_level_results <- read_excel("08_results_table/ki_level/results_table_Thu_Mar_14_2024_132947.xlsx")
hh_level_results <- read_excel("08_results_table/hh_level/results_table_Thu_Mar_14_2024_133443.xlsx")

kobo_tool_name <- "03_kobo_tool/SOM_REACH_H2R_Mar_2024_Tool - 290224.xlsx"
survey <- read_excel(kobo_tool_name, "survey")
choices <- read_excel(kobo_tool_name, "choices")

######################################################################################
################################ Relabel and Transpose ###############################
######################################################################################

# hh will always have the same or a lesser number of columns (districts)
cols_in_ki_not_in_hh <- setdiff(colnames(ki_level_results), colnames(hh_level_results))

# if hh has less, add them so we can rbind to do the reformatting all at once (i.e. rbind)
if(length(cols_in_ki_not_in_hh) > 0) {
    for(col in cols_in_ki_not_in_hh) {
        hh_level_results[[col]] <- NA
    }
}

# get out list of choices
choice_list <- survey %>%
                    select(name, choice_list, question_type, order) %>%
                    na.omit()

# rbind, sub "." for "/"
# all drag down values to replace NAs
combined_results <- rbind(ki_level_results, hh_level_results) %>%
                        dplyr::rename(question_choice_lookup = `Question.xml`) %>%
                        
                        # split the question_choice_lookup into question name and choice (only if its a multiple choice question)
                        mutate(survey_question_name_for_lookup = sub("\\..*", "", question_choice_lookup)) %>%
                        
                        # join to get the choice_list name, question_type, and question order        
                        left_join(choice_list, by = c("survey_question_name_for_lookup" = "name")) %>%
                        
                        # choice: if the question is multiple choice, we split off the choice after the period
                        # choice_list_choice_lookup: fill in the items without choice_lists with the question_choice_lookup
                        mutate(survey_question_name = ifelse(question_type == "select_multiple",
                                                             sub("\\..*", "", question_choice_lookup),
                                                             question_choice_lookup),
                               choice = ifelse(question_type == "select_multiple",
                                               sub(".*\\.", "", question_choice_lookup),
                                               NA),
                               choice_list_choice_lookup = ifelse((is.na(choice_list) | is.na(choice)),
                                                                  question_choice_lookup,
                                                                  paste0(choice_list,".", choice)),
                               row_num = row_number()) #%>%



combined_results$question_label <- survey$`label::English`[match(combined_results$survey_question_name_for_lookup, survey$name)]

# update the clean label - if there is a match with a name in the choices list, update to the "label::English" that would appear in the kobo tool, if not just keep the label from the original output
combined_results$choice_label <- ifelse(!match(combined_results$choice_list_choice_lookup, paste0(choices$list_name, ".", choices$name)) | is.na(match(combined_results$choice_list_choice_lookup, paste0(choices$list_name, ".", choices$name))),
                                        combined_results$Question.label,
                                        choices$`label::English`[match(combined_results$choice_list_choice_lookup, paste0(choices$list_name, ".", choices$name))])

# make it neater
combined_results <- combined_results %>%
                        relocate(row_num, survey_question_name, question_type, choice_list, choice, choice_list_choice_lookup, question_choice_lookup, question_label, choice_label, .before = `Question.label`)


# now we want to split the data into select_one, select_multiple, and calcuated HH variables

# this is the cutoff for calculated fields in the HH level script
fcs_score_row <- match("fcs_score",combined_results$choice_list_choice_lookup)

select_one_and_multiple_fields <- combined_results %>%
                                        filter(row_num < fcs_score_row) %>%
                                        fill(choice_list, question_type, survey_question_name)

combined_results_select_multiple <- select_one_and_multiple_fields %>%
                                    filter(question_type == "select_multiple") %>%
                                    fill(question_label, choice) %>%
                                    group_by(question_label, choice) %>%
                                    arrange(match(choice_label, c("NC", "0", "Average", "1")), .by_group = TRUE) %>%
                                    filter(!(choice_label %in% c("0", "NC"))) %>%
                                    # there's probably a cleaner way to do this than just entering all the districts, but for now it works
                                    # fill("All","xudur","tiyeglow","buloburto","dhuusomareb","afmadow","jalalaqsi","qandala","ceelbuur","kurtunwaarey","rab_dhuure","xarardhere","buur_hakaba","balcad","jamaame","buaale","saakow","jilib","ceeldheer","qansax_dheere","adanyabaal","ceelwaaq","diinsoor","waajid","sablaale","laasqoray") %>%
                                    fill("All","qandala","laasqoray") %>%
                                    filter(!(choice_label %in% c("1", "Average")))

combined_results_select_one <- select_one_and_multiple_fields %>%
                                    filter(question_type != "select_multiple" | is.na(question_type)) %>% # include the num surveys by including NA
                                    fill(question_label) %>% 
                                    # be careful with this below statement, I'm just using it to fill in NAs for survey count but make sure its not overwriting anything on accident!
                                    mutate(question_label = replace_na(question_label, "Number of Surveys")) %>%
                                    # fill("All","xudur","tiyeglow","buloburto","dhuusomareb","afmadow","jalalaqsi","qandala","ceelbuur","kurtunwaarey","rab_dhuure","xarardhere","buur_hakaba","balcad","jamaame","buaale","saakow","jilib","ceeldheer","qansax_dheere","adanyabaal","ceelwaaq","diinsoor","waajid","sablaale","laasqoray",
                                        # .direction = "up") %>%
                                    fill("All","qandala","laasqoray",
                                        .direction = "up") %>%
                                    filter(choice_label != "Average") %>%
                                    # not sure why its not working doing both of these in one filter, but oh well
                                    filter(question_label != choice_label)                                   

combined_results_calculated_fields <- combined_results %>%
                                            filter(row_num >= fcs_score_row) %>%
                                            mutate(question_label = ifelse(is.na(All),
                                                                           Question.label,
                                                                           NA)) %>%
                                            fill(question_label) %>%
                                            filter(!is.na(All))


combined_results_to_output <- combined_results_select_multiple %>%
                                    rbind(combined_results_select_one, combined_results_calculated_fields) %>%
                                    arrange(row_num) %>%
                                    select(-one_of(c("Question.label", "row_num","order","survey_question_name_for_lookup")))

# can import question and answer orders to clean up file

# transpose to match previous formats
transposed_combined_results <- as.data.frame(t(combined_results_to_output))

# add more_than_1_settlement if needed
combined_results_file_output_path <- paste0("08_results_table/transposed_hsm_mar_24_results_table_", date_time_now, ".xlsx")
transposed_combined_results %>% write.xlsx(combined_results_file_output_path, 
                                           sheetName = "HSM Mar 24 Results Table",
                                           rowNames = TRUE,
                                           colNames = FALSE)
