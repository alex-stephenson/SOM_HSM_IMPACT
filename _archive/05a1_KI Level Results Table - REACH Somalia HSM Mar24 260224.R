# REACH Somalia RNA may24 - ki_level aggregation script

###########################################################
########################## Setup ##########################
###########################################################

rm(list = ls())

options(scipen = 999)

if (!require("pacman")) install.packages("pacman")

pacman::p_load(tidyverse,
               hypegrammaR,
               rio,
               readxl,
               openxlsx)

setwd('C:\\Users\\Abdirahman IBRAHIM\\ACTED\\IMPACT SOM - General\\02_Research\\01_REACH\\Unit 1 - Intersectoral\\SOM1901_HSM\\03_Data\\2024\\01_March Round')
source("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/02_Data Collection & Processing/_code/support_functions/05_Results Table - Support Functions.R")
#########################################################################
########################## read in needed data ##########################
#########################################################################

#data <- read.csv("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM2403_Gu Flooding RNA/03_Data & analysis/01_Data Cleaning/01_RNA/inputs/GU_test_Dataset.csv", stringsAsFactors = F, na.strings = c("n/a","#N/A","NA",""))
#the aggrreated data will be loaded here 
data <- read_csv("C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/aggregation/Thu_Aug_01_2024_141847.csv")
koboToolPath <- "C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/02_Data Collection & Processing/01_Tool/SOM_REACH_H2R_July_2024_Tool - 090724.xlsx"
#stringsAsFactors = F, na.strings = c("n/a","#N/A","NA",""))
# not sure why this has to be factor, but not messing with it
data  <- to_factor(data)

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
names(data) <- gsub("/"
             , "."
             , names
             (data))
############################################################################
########################## make the results table ##########################
############################################################################

res <- generate_results_table(data = data,
                              questions = questions,
                              choices = choices,
                              weights.column = NULL,
                              use_labels = T,
                              labels_column = "label::English",
                              "district" 
                              )

export_table(res,"C:/Users/Abdirahman IBRAHIM/ACTED/IMPACT SOM - General/02_Research/01_REACH/2024_25/01_ISU/SOM1901_HSM/aggregation/result_table")

write.csv(res, file = "output.csv", row.names = FALSE)
write.csv(res,output)
