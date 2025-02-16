# download audit files using the audit_URL
download_audit_files <- function(df, uuid_column = "_uuid", audit_dir, usr, pass){
  if (is.na(audit_dir) || audit_dir == "") 
    stop("The path for storing audit files can't be empty!")
  
  if (is.na(usr) || usr == "") 
    stop("Username can't be empty!")
  
  if (is.na(pass) || pass == "") 
    stop("Password can't be empty!")
  
  require(httr)
  
  # checking if the output directory is already available
  if (!dir.exists(audit_dir)) {
    dir.create(audit_dir)
    if (dir.exists(audit_dir)) {
      cat("Attention: The audit file directory was created in", audit_dir,"\n")
    }
  }
  
  # checking if creating output directory was successful
  if (!dir.exists(audit_dir))
    stop("download_audit_fils was not able to create the output directory!")
  # checking if uuid column exists in data set
  if (!uuid_column %in% names(df))
    stop("The column ", uuid_column, " is not available in data set.")
  # checking if column audit_URL exists in data set
  if (!uuid_column %in% names(df))
    stop("The column ", uuid_column, " is not available in data set.")
  if (!"audit_URL" %in% names(df))
    stop("Error: the column audit_URL is not available in data set.")
  
  # getting the list of uuids that are already downloaded
  available_audits <- dir(audit_dir)
  
  # excluding uuids that their audit files are already downloaded
  df <- df[!df[[uuid_column]] %in% available_audits,]
  
  audits_endpoint_link <- df[["audit_URL"]]
  names(audits_endpoint_link) <- df[[uuid_column]]
  audits_endpoint_link <- na.omit(audits_endpoint_link)
  
  if (length(audits_endpoint_link) > 0) {
    # iterating over each audit endpoint from data
    for (i in 1:length(audits_endpoint_link)) {
      uuid = names(audits_endpoint_link[i])
      endpoint_link_i <- audits_endpoint_link[i]
      cat("Downloading audit file for", uuid, "\n")
      
      # requesting data
      audit_file <- content(GET(endpoint_link_i,
                                authenticate(usr, pass),
                                timeout(1000),
                                progress()), "text", encoding = "UTF-8")
      
      if (!is.na(audit_file)) {
        if (length(audit_file) > 2) {
          dir.create(paste0(audit_dir, "/", uuid), showWarnings = F)
          write.csv(audit_file, paste0(audit_dir, "/", uuid, "/audit.csv"), row.names = F)
        }else if(!audit_file == "Attachment not found"){
          if (grepl("[eventnodestartend]", audit_file)) {
            dir.create(paste0(audit_dir, "/", uuid), showWarnings = F)
            write.table(audit_file, paste0(audit_dir, "/", uuid, "/audit.csv"), row.names = F, col.names = FALSE, quote = F)
          } else{
            cat("Error: Downloading audit was unsucessful!\n")
          }
        }
      } else{
        cat("Error: Downloading audit was unsucessful!\n")
      }
    }
  } else{
    cat("Attention: All audit files for given data set is downloaded!")
  }
}



# download the raw data
{rm(list = ls())
  cat("\014")
  library(httr)
  library(stringr)
  library(readxl)
  library(openxlsx)
  library(KoboconnectR)
  library(lubridate)
  library(robotoolbox)
  library(tidyverse)
  
  source("R/data_cleaning_module.R")
  source("R/download_kobo_data.R")
  
  uname="********"
  pwd="*********"
  
  url = "kobo.impact-initiatives.org"
  token = "**********************"
  
  assetid = "asSj6Aq8kDg5FULShCKbjN"
}
######################################### download data from kobo server ##############################################
# get asseIndometh
# kobotools_api(
#   url = url,
#   simplified = TRUE,
#   uname = uname,
#   pwd = pwd,
#   encoding = "UTF-8"
# )

# download the audit file
audit_output_dir <- "C:/Users/sulai/ACTED/IMPACT SOM - KenSom GIS and DATA team - 01_DSA/REACH_SOM_DSA_VIII/audit_files/"

sensitive_cols <- c(
  "audit",
  "audit_URL"
  # "ki_contact",
  # "ki_name"
)

data_file_names <- list.files("./raw_data/", pattern = "*.xlsx", full.names = T)

#getting the recent file name
names(data_file_names) <- str_extract(data_file_names, "[0-9]{4}(-[0-9]{2}){2}")
data_file_names <- data_file_names[sort(names(data_file_names))]
recent_data_file_names <- data_file_names[length(data_file_names)]

recent_data_main <- read_excel(recent_data_file_names, guess_max = 15000)

recent_ster_main <- recent_data_main[,!colnames(recent_data_main) %in% sensitive_cols]

df <- str_extract(recent_data_file_names,"DSA.*")
write.xlsx(recent_ster_main,paste0("C:/Users/sulai/ACTED/IMPACT SOM - KenSom GIS and DATA team - 01_DSA/REACH_SOM_DSA_VIII/raw_data/",df))

download_audit_files(recent_data_main, audit_dir = audit_output_dir, usr = uname, pass = pwd, uuid_column = "_uuid")
