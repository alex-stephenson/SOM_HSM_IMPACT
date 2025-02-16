# compiling all of the surveys deleted before clogs were generated

library(dplyr)
library(purrr)
library(readxl)
library(writexl)

deleted_surveys_location <- "C:/Users/reid.jackson/ACTED/IMPACT SOM - 01_REACH/Unit 1 - Intersectoral/SOM1901_HSM/03_Data/2024/01_March Round/04_data_cleaning/_deleted_prior_to_cleaning/day_by_day/"

deleted_surveys_files <- setdiff(list.files(path = deleted_surveys_location), list.dirs(path = deleted_surveys_location, recursive = FALSE, full.names = FALSE))

deleted_surveys <- deleted_surveys_files %>%
                    map_dfr(~read_excel(paste0(deleted_surveys_location,.x))) %>%
                    distinct() # just a few days multiple times testing things so there may be dupes. take distinct to be safe

uuid_dup_check <- deleted_surveys %>%
                        group_by(uuid) %>%
                        dplyr::summarise(count = n())

# note: for now you need to manually update any uuids with count >= 2!!
deleted_surveys %>% write_xlsx("C:/Users/reid.jackson/ACTED/IMPACT SOM - 01_REACH/Unit 1 - Intersectoral/SOM1901_HSM/03_Data/2024/01_March Round/04_data_cleaning/_deleted_prior_to_cleaning/consolidated/deleted_surveys_hsm_mar24_consolidated.xlsx")
