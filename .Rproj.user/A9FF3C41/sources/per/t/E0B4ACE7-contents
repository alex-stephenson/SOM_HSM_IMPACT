# Goal: Handle "duplicate correct parent columns"
# Each parent column for a given UUID, e.g. "reason_crop_loss", should have the same "new_value" after FOs review the clogs
# However, this may not happen if a parent column shows up in multiple checks that cause different updates
# This code harmonizes all checks to create one uniform "new_value" per parent column

# Update 1/8/24: I need to modify this script to handle all binaries, not just those that are flagged as a part of the "duplicate correct parent columns" - THIS IS DONE!
# This will ensure that our data is completely clean, and any changes made to the binaries will then have a corresponding entry added in the cleaning log (created automatically by cleaningTools)

# start with a clean slate
rm(list = ls())


library(dplyr)
library(readxl)
library(stringr)
library(purrr)
library(openxlsx)

date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")

###########################################################################
######################### Get the Kobo Tool Info ##########################
###########################################################################

# import the kobo tool
kobo_tool_name <- "../../00_tool/SOM_REACH_HSM_December_2024_Tool_Pilot_ver_AS_EDITS_v2.xlsx"

# importing survey as we only need to run these checks for select_multiple questions
kobo_survey <- read_excel(kobo_tool_name, sheet = "survey")
kobo_choice <- read_excel(kobo_tool_name, sheet = "choices")

# we create a list of all choices to check if enumerators entered something incorrectly that isn't a possible choice
all_choices <- kobo_choice %>%
                    select(name) %>%
                    distinct() %>%
                    pull()

# choices to identify choices that can only be selected by themselves
only_selectable_alone <- kobo_choice %>%
                            filter(only_selectable_by_itself == TRUE) %>%
                             select(name) %>%
                             distinct() %>%
                            pull()

####################################################################################
######################### Get the reviewed clogs for 1 FO ##########################
####################################################################################

fo_in_charge <- "kala"
#fo_finished_clogs_path <- paste0("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/04_data_cleaning", fo_in_charge," - Clogs/finished_clogs/")
fo_finished_clogs_path  <- paste0("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2023_24/01_ISU/SOM1901_HSM/03_Data & Data Analysis/01_March Round/04_data_cleaning/",fo_in_charge," -  Clogs/Finished_Clogs/")
# get the files only (exclude any folders)
fo_finished_clogs_file_names <- setdiff(list.files(path = fo_finished_clogs_path), list.dirs(path = fo_finished_clogs_path, recursive = FALSE, full.names = FALSE))

# rbind all the clogs into a big dataframe
finished_clogs <- fo_finished_clogs_file_names %>%
  map_dfr(~read_excel(paste0(fo_finished_clogs_path, .x), sheet = 'cleaning_log'))

# rbind all the raw dta into a big dataframe
datasets <- fo_finished_clogs_file_names %>%
  map_dfr(~read_excel(paste0(fo_finished_clogs_path, .x), sheet = 'checked_dataset'))
datasets <- read_excel(path = "C:/Users/Abdirahman IBRAHIM/Documents/R/HSMDEC_2024/04_data_cleaning/kala _ Clogs/Finished_Clogs/cleaning_log_kala_Dec_19_2024_082211_DT-FIN.xlsx", sheet = "checked_dataset")

###############################################################################
############################# Identify issues to fix ###############################
####################################################################################

# we rename the data enumerators entered into the clogs as "orig_new_value", reserving "new_value" for the final output after all the checks are performed
# we rename the data enumerators entered into the clogs as "orig_new_value", reserving "new_value" for the final output after all the checks are performed
finished_clogs<- read_excel("C:/Users/Abdirahman IBRAHIM/Documents/R/HSMDEC_2024/04_data_cleaning/kala _ Clogs/Finished_Clogs/cleaning_log_kala_Dec_19_2024_082211_DT-FIN.xlsx")
 finished_clogs <- finished_clogs %>%
  dplyr::rename(orig_new_value = 'new_value') %>%
   mutate('new_value' = "")
 
 
# identify uuid/question combos with multiple instances. These are the items we need to look at most closely and update if different changes were made for different logic checks. These should only be parent columns 
uuid_question_count <- finished_clogs %>%
                            mutate(uuid_question_id = paste0(uuid, question)) %>%
                            group_by(uuid_question_id) %>%
                            dplyr::summarise(instances = n())

# filter for the parent columns of the items we need to look at
# filter for only select multiple columns. These checks aren't relevant for select_one, text, or integer
clogs_in_processing <- finished_clogs %>%
                            mutate(uuid_question_id = paste0(uuid, question)) %>%
                            left_join(uuid_question_count, by = "uuid_question_id") %>%
                            left_join(kobo_survey, by = c("question" = "name")) %>%
                            filter(str_detect(question,"/") == FALSE, question_type == "select_multiple") %>%
                            # filter(instances >= 2, str_detect(question,"/") == FALSE)
                            select(uuid, question, question_type, old_value, change_type, orig_new_value, uuid_question_id, instances)

####################################################################################
################################### Fix Issues #####################################
####################################################################################

# get our list of uuid/question to go through
uuid_question_id_list <- clogs_in_processing %>%
                            pull(uuid_question_id) %>%
                            unique()

# we write all of the text output to a file for later review
#sink(paste0("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/06_cobining_clean_data/", fo_in_charge, "_step_2_output.txt"))
sink(paste0("C:/Users/Abdirahman IBRAHIM/Documents/R/HSMDEC_2024/06_Cmbining cleaned_data/", fo_in_charge, "_step_2_output.txt"))


if (as.numeric(updated_choice_binary) == 0) {
  # Code for when updated_choice_binary is 0
}

for (id in uuid_question_id_list) {

    # testing
    # id <- uuid_question_id_list[1]
    # id <- "17437ed9-ae38-4907-8f5d-830812ffb24cbarriers_access_education_boys" # updating binaries that don't appear in clogs
    # id <- "123911be-03c7-4ac1-b6d7-82a77db66130shocks" # how to handle binaries that show up in "old_value" and should already be 1
    # id <- "0cb3ecb0-7912-4db3-80e8-03833c7c71a5shocks" # dnk issue with parent / chils columns
    # id <- "75793b40-b929-4ee9-bff4-7d6dac435cc0reason_livestock_decrease"
    # id <- "6c865ffa-44ef-4bd9-9dbb-78991a73c865barriers_access_education_boys"
    # id <- "5fc9b1e0-e321-429c-b414-f33e8490cd3breason_crop_loss"
    
    # identify the question
    current_question <- clogs_in_processing %>%
        filter(uuid_question_id == id) %>%
        select(question) %>%            
        distinct() %>% 
        pull(question)

    # identify the uuid
    current_uuid <- clogs_in_processing %>%
        filter(uuid_question_id == id) %>%
        select(uuid) %>%            
        distinct() %>% 
        pull(uuid)
    
    print(paste0("Looking at uuid, question combo: ", current_uuid, ", ", current_question,", which has ", unique(clogs_in_processing[clogs_in_processing$uuid == current_uuid & clogs_in_processing$question == current_question,]$instances), " instance(s)"))
    
    # get the selections in the parent column from the FO review, will be a list where the length is the number of instances of the parent column in the clog
    parent_selections <- clogs_in_processing %>%
                            filter(uuid_question_id == id) %>%
                            pull(orig_new_value)
    
    # collapse all of the choices in all of the parent columns into a single string
    parent_selections_combined <- paste(parent_selections, collapse = " ")
    
    # get a unique list of the choices
    parent_selections_unique <- unique(str_split(parent_selections_combined, " ")[[1]])
    
    print(paste0("The parent column selections were: ", paste(parent_selections_unique, collapse = ", ")))
    
    # initialize a blank string for the correct parent column entries
    parent_selections_updated <- ""

    old_values <- clogs_in_processing %>%
                    filter(uuid_question_id == id) %>%
                    pull(old_value)
    
    # record the old_value, i.e. what was originally submitted by the enumerator
    old_values_combined <- paste(old_values, collapse = " ")
    
    # get a unique list of the old choices
    old_values_unique <- unique(str_split(old_values_combined, " ")[[1]])
    
    print(paste0("The old value selections were: ", paste(old_values_unique, collapse = ", ")))

    # we cycle through each choice in the orig_new_value to see if there are any inconsistencies, or binaries that need updating
    for (choice in parent_selections_unique) {

        #testing
        # choice <- parent_selections_unique[1]
        
        # this will stop the code if something was entered that is not a valid choice (comparing against the kobo tool)
        # if the message looks like the variable choice is blank - ensure that there aren't any double spaces in the clogs. This will confuse the code - there should only be one space between choices
        if (!(choice %in% all_choices)) {
            stop(paste0("the choice: ", choice, " is not valid"))
        }

        # start cycling through the different choices
        print(paste0("Looking at choice: ", choice))
        
        # if the binary value for the choice has been updated in the cleaning logs, we get that value and treat it as correct
        # if the binary hasn't been updated, this will return character(0), and therefore we assume that it is correct because it wasn't changed in the clog
        updated_choice_binary <- finished_clogs  %>%
                                    filter(uuid == current_uuid,
                                           question == paste0(current_question, "/", choice)
                                          ) %>%
                                    pull(orig_new_value) %>%
                                    unique()
        
        # if there are multiple entries for the same uuid/question combo (i.e. 0s and 1s), we take the "inclusive approach" and set all entries to 1
        if(length(updated_choice_binary) > 1) {
            print(paste0("There are both 0s and 1s for: ", choice, " - to be inclusive we'll add it"))
            parent_selections_updated <- paste0(parent_selections_updated, " ", choice)
            
            print(paste0("There are both 0s and 1s for: ", choice, " - we are updating the binaries to all be 1 for the inclusive approach"))
            current_question_binary <- paste0(current_question, "/", choice)
            finished_clogs[finished_clogs$uuid == current_uuid & finished_clogs$question == current_question_binary,]$new_value <- "1"
        
        # we have to deal with "special choices", i.e. those that can only be selected on their own. It we come across a choice like this, we just add it to the updated values right away
        } else if (any(grepl(choice, only_selectable_alone))) {
            print(paste0(choice," is only selectable by itself. Adding for now to check later"))
            parent_selections_updated <- paste0(parent_selections_updated, " ", choice)
        
        } else {
            # check if the choice isn't in the clogs
            if (length(updated_choice_binary) == 0L) {
                print(paste0("Keeping ", choice, " in the updated parent list because it was either 1) included in 'old_value' or 2) a new write in to 'new_value' without a child column in the clog for now"))
                parent_selections_updated <- paste0(parent_selections_updated, " ", choice)
                
                # print(paste0("Updating the binary for ", choice, " to 1 (either this value is already 1 if it was in 'old_value' or we are overwriting to 1 based on presence in the 'new_value' column)"))
                current_question_binary <- paste0(current_question, "/", choice)

                # make sure there's not an issue with character 1 vs number 1 - probably rerun everything to be safe
                if (as.numeric(datasets[datasets$uuid == current_uuid, current_question_binary]) == 1) {
                    print(paste0("The child field ", current_question_binary, " is already set to 1 in the raw data, so we don't need to add a clog entry updating this"))
                } else if (as.numeric(datasets[datasets$uuid == current_uuid, current_question_binary]) == 0) {
                    print(paste0("The child field ", current_question_binary, " is 0 in the raw data, so we will add an entry in the clog setting it to 1"))
                    
                    # add a new row in the clog setting the binary to 1    
                    finished_clogs <- finished_clogs %>% 
                                        add_row(uuid = current_uuid,
                                                old_value = "0",
                                                question = current_question_binary,
                                                issue = "generated from reid's clog review code",
                                                change_type = "change_response",
                                                new_value = "1"
                                                )
                }

            # check if the choice was updated to "0"
            } else if (as.numeric(updated_choice_binary) == 0) {
                print(paste0("Not adding ", choice, " to the updated parent list because 0 was in the child binary"))
            
            # check if the choice was updated to "1"    
            } else if (as.numeric(updated_choice_binary) == 1) {
                parent_selections_updated <- paste0(parent_selections_updated, " ", choice)
                print(paste0("Adding ", choice, " to the updated parent list because 1 was in the child binary"))
                
            # ideally this should never happen because everything will be either 0, 1, or character()
            } else {
                stop("Something funky is happening with choice binaries")
            }
        }
    }
    
    # now we have our fully updated list of choices. We trim to remove any extra whitespace
    parent_selections_updated <- str_trim(parent_selections_updated)
    
    print(paste0("The proposed final list is: ", parent_selections_updated))
    
    # This is where we do the check to see if any of our choices fall under the "only selectable on their own" options
    # We check 1) are any of the options in "only_selectable_alone" and 2) are there more than 1 choices in parent_selections_updated
    # If both of these are true, we remove the only_selectable_alone option
    if (any(sapply(paste0("//b", only_selectable_alone, "//b"), grepl, parent_selections_updated)) & length(str_split(parent_selections_updated, " ")[[1]]) >= 2) {
    
        # find which only_selectable_alone is in parent_selections_updated
        osa_check <- sapply(paste0("//b",only_selectable_alone, "//b"), grepl, parent_selections_updated)
        osa_value <- osa_check[osa_check == TRUE]
        osa_to_remove <- gsub("////b","",names(osa_value))
        
        # remove the choice
        parent_selections_updated <- str_trim(gsub(osa_to_remove, "", parent_selections_updated))
    
        # the the binary to 0    
        finished_clogs[finished_clogs$uuid == current_uuid & finished_clogs$question == paste0(current_question, "/", osa_to_remove),]$new_value <- "0"
        
        print(paste0("We found an only_selectable_alone choice and removed: ", osa_to_remove, ". The final list is actually: ", paste(parent_selections_updated, collapse = ", ")))
        print(paste0("We also set the binary for ", osa_to_remove, " to 0"))
    }

    # update the "new_value" column with the final parent column selections
    finished_clogs[finished_clogs$uuid == current_uuid & finished_clogs$question == current_question,]$new_value <- parent_selections_updated
    
    # make sure old values are set to 0 if they aren't in parent_selections_updated
    for (old_choice in old_values_unique) {
        
        # old_choice <- old_values_unique[1]
        
        if (!grepl(old_choice, parent_selections_updated) & nrow(finished_clogs[finished_clogs$uuid == current_uuid & finished_clogs$question == paste0(current_question, "/", old_choice),]) == 0) {
            
            finished_clogs <- finished_clogs %>% 
                                add_row(uuid = current_uuid,
                                        old_value = "1",
                                        question = paste0(current_question, "/", old_choice),
                                        issue = "generated from reid's clog review code",
                                        change_type = "change_response",
                                        new_value = "0"
                                        )
            print(paste0("We found ", old_choice, " in 'old_value' and it isn't in 'new_value' / doesn't have a binary update in the cleaning log to set it to 0 yet, so we added one"))
            
        }
    }
    
    cat("/n")
    
}

sink()
# perhaps need to have one final check to take the "inclusive approach"
print(updated_choice_binary)
print(paste0("Debugging: updated_choice_binary is ", updated_choice_binary))
# Identify problematic rows in finished_clogs where 'orig_new_value' might be NA or blank
problematic_rows <- finished_clogs[is.na(finished_clogs$orig_new_value) | finished_clogs$orig_new_value == "", ]
print(problematic_rows)
View(problematic_rows)
View(updated_choice_binary)####################################################################################
############### Merge the "good" and "corrected parts of the clogs #################
####################################################################################

finished_clogs <- finished_clogs %>%
                            filter(uuid != "ignore") %>% # you can't write a clog with zero rows using cleaning tools, so I used "ignore" placeholders to get them to write. removing them here
                            mutate(new_value = ifelse(new_value == "", orig_new_value, new_value)) %>% # if we made a correction, it'll already be in new_value, if not then copy the orig_new_value to new_value
                            mutate(change_type = ifelse(new_value == old_value & change_type != "remove_survey", "no_action", "change_response")) %>%
                            # getting rid of any helper columns we used to review the clogs
                            select(-one_of(c("unique_id", "dup_count", "mismatch lookup", "mismatch?", "manually_entered_correct_answer", "comment","Comments", "Old/New value", "no_action", "change_response", "problem?", "Old/new", "No_action", "problem", "comments","old/new the same?","no action?","change_response?","Old/New","old/New", "old/new"))) %>%
                            relocate(new_value, .before = orig_new_value)

####################################################################################
############ Write the data and corrected clogs combined for all days ##############
####################################################################################

write_path <- paste0("06_combining_clean_data/fo_level_corrected_clogs_inputs/", fo_in_charge, "_combined_corrected_clogs_", date_time_now, ".xlsx")

# create a workbook with our data

wb <- createWorkbook()
addWorksheet(wb, "checked_dataset")
addWorksheet(wb, "cleaning_log")

writeData(wb, 1, datasets)
writeData(wb, 2, finished_clogs)

saveWorkbook(wb, write_path , overwrite = TRUE)

