# compiling all of the hsm referrals

library(dplyr)
library(purrr)
library(readxl)
library(writexl)

contacts_location <- 'C:/Users/reid.jackson/ACTED/IMPACT SOM - 01_REACH/Unit 1 - Intersectoral/SOM1901_HSM/03_Data/2023/03_December Round/05_contact_information_dec_cycle/_all_contacts_for_next_cycle/day_by_day/'

contact_file_names <- list.files(path = contacts_location)

contacts <- contact_file_names %>%
                    map_dfr(~read_excel(paste0(contacts_location,.x))) %>%
                    filter(district != "ignore") # needed to use some placeholders when creating clogs, so we delete them

write_xlsx(contacts,"dec23_hsm_referrals.xlsx")
