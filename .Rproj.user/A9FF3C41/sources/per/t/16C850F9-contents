# REACH Somalia HSM Dec 2024

rm(list = ls())
options(scipen = 999)

if (!require("pacman")) install.packages("pacman")

pacman::p_load(tidyverse,
               hypegrammaR,
               rio,
               readxl,
               openxlsx)

source("support_functions/05_Results Table - Support Functions.R")
#########################################################################
########################## read in needed data ##########################
#########################################################################

data <- read.csv("01_input/01_clean_data/SOM1901_HSM_Dec_2024_Clean_Data.csv", stringsAsFactors = F, na.strings = c("n/a","#N/A","NA",""))
data  <- to_factor(data)

# Exclude districts with lower sample size than 25 interviews
small_smple_dist = as.data.frame(table(data$district)) |> filter(Freq <= 15) |> rename(distric = Var1)

to_exclude <- c("deviceid", "enum_base", "ki_consent", "ki_continue", "ki_knowledgeable","settlement")

data <- data |> select(-all_of(to_exclude)) |> 
  filter(!district %in% small_smple_dist$distric )

koboToolPath <- "01_input/02_tool/REACH_SOM_HSM_Server_Tool.xlsx"
questions <- read_xlsx(koboToolPath,
                       guess_max = 50000,
                       na = c("NA","#N/A",""," "),
                       sheet = 1) %>% filter(!is.na(name)) %>% 
  mutate(q.type=as.character(lapply(type, function(x) str_split(x, " ")[[1]][1])),
         list_name=as.character(lapply(type, function(x) str_split(x, " ")[[1]][2])),
         list_name=ifelse(str_starts(type, "select_"), list_name, NA))

choices <- read_xlsx(koboToolPath,
                     guess_max = 50000,
                     na = c("NA","#N/A",""," "),
                     sheet = 2)

names(data) <- gsub("/",".", names(data))
############################################################################
########################## make the results table ##########################
############################################################################

res <- generate_results_table(data = data,
                              questions = questions,
                              choices = choices,
                              weights.column = NULL,
                              use_labels = T,
                              labels_column = "label::English",
                              strata = "district"
)

export_table(res, "02_output/Analysis_Result_Table/resutl_table")

write.csv(res, file = "02_output/Analysis_Result_Table/SOM1901_HSM_Dec_2024_Analysis_resutl_table.csv", row.names = FALSE)

saveRDS(res, "02_output/Analysis_Result_Table/SOM1901_HSM_Dec_2024_Analysis_resutl_table.RDS")
