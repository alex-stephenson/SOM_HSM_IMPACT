
rm(list = ls())
date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")
RC = "HSM_DEC_24"

unchecked_clean_plus_deleted_data_for_dashboard <- read_xlsx("outputs/dashboard/unchecked_data_for_dashboard_Dec_19_2024_082211.xlsx")


dashboard_data_path = r"(outputs/dashboard/)"

dashboard_files <- list.files(path = dashboard_data_path,
                                         pattern = "*unchecked_data_for_dashboard_*", 
                                         recursive = FALSE)

all_data <- dashboard_files %>%
  map_dfr(~read_excel(paste0(dashboard_data_path,.x)))



districts <- read_xlsx(r"(inputs/District_Region_Ref_Data.xlsx)", sheet = "Districts") %>%
  select(District, D_code) %>%
  distinct() %>%
  mutate(
    District = tolower(str_replace_all(District, " ", "_")))
  

Regions <- read_xlsx(r"(inputs/District_Region_Ref_Data.xlsx)", sheet = "Region") %>%
  select(Region, R_code) %>%
  distinct() %>%
  mutate(
    Region = tolower(str_replace_all(Region, " ", "_")))
# 
# site_reference_data <- read_xlsx(r"(inputs/Very Heavy Restriction Settlements 6_REF.xlsx)", sheet = "Correct") %>%
#   select(District, D_code) %>%
#   distinct() %>%
#   mutate(
#     District = tolower(str_replace_all(District, " ", "_")),
#     admin1Name = tolower(str_replace_all(admin1Name, " ", "_"))) %>%
#   filter(admin1Name != "<null>")

dashboard_and_codes <- all_data %>% 
  mutate(region = tolower(region),
         region = case_when(
           region == "middle_juba_kismayo" ~ "middle_juba",
           region == "middle_juba_baidoa" ~ "middle_juba",
           TRUE ~ region)) %>%
  left_join(districts, by = join_by("district" == "District")) %>%
  left_join(Regions, by = join_by("region" == "Region"))
  

enum_data_raw <- dashboard_and_codes %>%
  select(enum_code, today, district, D_code, region, R_code, length_valid, field_officer = fo_in_charge_for_code) %>%
  mutate(RC = "HSM Dec 24") %>%
  mutate(length_valid = 
           case_when(
             length_valid == 'Too short' ~ FALSE,
             length_valid == 'Too long' ~ FALSE,
             TRUE ~ TRUE
           ))

enum_performance <- enum_data_raw %>%
  count(field_officer, enum_code, length_valid) %>%
  group_by(field_officer, enum_code, length_valid) %>%
  summarise(n = sum(n)) %>%
  ungroup() %>%
  tidyr::pivot_wider(names_from = length_valid, names_prefix = "Interview_Valid: ", values_from = n, values_fill = 0) %>%
  mutate(
    "Total Done" = (`Interview_Valid: FALSE` + `Interview_Valid: TRUE`), 
    "Percent Accepted" = (`Interview_Valid: TRUE` / (`Interview_Valid: FALSE` + `Interview_Valid: TRUE`)) * 100
  )


mean_per_day <- enum_data_raw %>%
  group_by(field_officer, enum_code, today) %>%
  summarise(n = n()) %>%
  ungroup() %>%
  group_by(field_officer, enum_code) %>%
  summarise("Average per day" = mean(n))

enum_performance <- enum_performance %>%
  left_join(mean_per_day) %>%
  mutate(RC = "HSM Dec 24")



grouped_enum_performance <- enum_data_raw %>%
  group_by(RC, R_code, field_officer, enum_code, today, D_code) %>%
  summarise(percent_valid = mean(length_valid))

  
  
write.csv(grouped_enum_performance, paste0("outputs/enum_performance/grouped_enum_performance", date_time_now, ".csv"))

input_path_performance <- r"(C:\Users\alex.stephenson\ACTED\IMPACT SOM - 02_Research\01_REACH\Data Team\05_Enumerator_Performance\01_input_files/performance_data/enum_performance_)"
write.csv(enum_performance, paste0(input_path_performance, RC, date_time_now, ".csv"))

  

