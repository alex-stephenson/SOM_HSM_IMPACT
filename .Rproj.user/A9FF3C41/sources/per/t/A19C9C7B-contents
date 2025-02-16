## I dont think this script is likely to be used again. I made it for Hanna to assist with idntifying which sites are oversampled
## and then to support in identifying which sites should be removed based on a set of provided criteria. 


oversampled_uuids <- dashboard_data %>%
  filter(str_detect(Settlement, "so")) %>%
  dplyr::count(Settlement) %>%
  filter(n > 1) %>%
  arrange(desc(n)) %>%
  pull(Settlement)

oversampled_sites <- dashboard_data %>%
  filter(Settlement %in% oversampled_uuids) %>% 
  distinct(Settlement)

oversampled_data <- dashboard_data %>%
  filter(Settlement %in% oversampled_uuids)


old_oversampled <- readxl::read_excel(r"(C:\Users\alex.stephenson\ACTED\IMPACT SOM - 01_REACH\2024_25\01_ISU\SOM1901_HSM\03_Data & Data Analysis\DEC_24\01CLeaning scripts\_code/oversampled_sites.xlsx)")
oo_uuids <- old_oversampled %>%
  pull(uuid)



new_oversampled <- oversampled_data %>%
  filter(uuid %!in% oo_uuids)
new_oversampled %>%
  writexl::write_xlsx(., "outputs/new_oversampled.xlsx")

  
  

oversampling_set_two <- readxl::read_excel(r"(C:\Users\alex.stephenson\Downloads/second set_to delete_oversampled sites_Dec 24_v1.xlsx)") %>%
  mutate(file = "second set_to delete_oversampled sites_Dec 24_v1")
oversampling_set_one <- readxl::read_excel(r"(C:\Users\alex.stephenson\Downloads/to delete_oversampled_sites_HSM_v2.xlsx)") %>%
  mutate(file = "to delete_oversampled_sites_HSM_v2")

  
joined <- rbind(oversampling_set_two, oversampling_set_one)
    
## validation
same_actions <- joined %>%
  group_by(uuid) %>%
  summarise(n = n(),
            action = paste0(Action, collapse = ", ")) %>%
  filter(n > 1) %>%
  ungroup()

in_new <- joined %>%
  filter(Action == "Keep") %>%
  group_by(uuid, Settlement) %>%
  slice_head(n = 1) %>%
  select(uuid) %>%
  mutate("is_new" = TRUE) 

two_settlements <- joined %>% 
  filter(Action == "Keep") %>%
  dplyr::count(Settlement) %>%
  filter(n>1) %>%
  pull(Settlement)



oversampled <- joined %>%
  filter(Action != "Keep") %>%
  select(`Reason for Action`, uuid)
  
oversampled %>%
  writexl::write_xlsx(., "outputs/oversampled_to_delete.xlsx")

  
  
  

oversampled_data %>%
  group_by(Settlement) %>%
  summarise(iv_method = paste0(`Interview Type`, collapse = "/")) %>%
  group_by(iv_method) %>%
  summarise(n = n())


remove_not_phone <- oversampled_data %>%
  group_by(Settlement) %>%
  mutate(iv_method = paste0(`Interview Type`, collapse = "/")) %>% 
  filter(str_detect(iv_method, "phone_interview") & str_detect(iv_method, "face_to_face")) %>%
  filter(`Interview Type` != "phone_interview") %>%
  pull(uuid)

oversampled_minus_in_type <- oversampled_data %>%
  filter(uuid %!in% remove_not_phone)
  

  # Define the preferred profiles
preferred_profiles <- c("religious_leader", "comm_leader", "healthcare_professional", "educator")

# Group by 'Settlement' and filter the data
oversampled_minus_roles <- oversampled_minus_in_type %>%
  group_by(Settlement) %>%
  filter(
    if (any(ki_type %in% preferred_profiles)) {
      ki_type %in% preferred_profiles
    } else {
      TRUE
    }
  ) %>%
  ungroup()


oversampled_minus_roles %>% group_by(Settlement) %>% summarise(n = n()) %>%
  filter(n > 1)


# Define the months in order for proper comparison
month_order <- c("january", "february", "march", "april", "may", "june", 
                 "july", "august", "september", "october", "november", "december")

# Convert `ki_last_in_site` to an ordered factor for chronological comparison
oversampled_minus_roles <- oversampled_minus_roles %>%
  mutate(ki_last_in_site = factor(tolower(ki_last_in_site), levels = month_order, ordered = TRUE))

# Further filter the data
oversampled_last_visit <- oversampled_minus_roles %>%
  group_by(Settlement) %>%
  filter(
    if (all(`Interview Type` == "face_to_face")) {
      ki_last_in_site == max(ki_last_in_site, na.rm = TRUE)
    } else {
      TRUE
    }
  ) %>%
  ungroup()

oversampled_minus_roles %>%
  group_by(Settlement) %>%
  summarise(`Interview Type` = paste0(`Interview Type`, collapse = "/")) %>%
  group_by(`Interview Type`) %>%
  summarise(n = n())

oversampled_last_visit %>%
  group_by(ki_last_speak_to_residents) %>%
  summarise(n =n())



  
oversampled_last_visit %>%
  group_by(Settlement) %>%
  summarise(n = n()) %>%
  filter(n > 1)
  






