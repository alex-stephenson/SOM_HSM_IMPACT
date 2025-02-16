audit_files <- robotoolbox::kobo_audit("asSj6Aq8kDg5FULShCKbjN", progress = T)

audit_files_length <- audit_files %>%
  mutate(metadata_duration = (end_int - start_int ) / 60000)

audit_files_length_gps <- audit_files_length %>%
  filter(!str_detect(node, "geo"))

iv_lengths <- audit_files_length_gps %>% 
  group_by(`_id`) %>% 
  summarise(metadata_iv_len = sum(metadata_duration, na.rm = T))

time_delta <- df %>% ## this DF is taken from the 01_Data_Cleaning_DSA8 script.
  filter(district_name == "Jalalaqsi",
         str_detect(end, "2024-12-05")) %>%
  select(enum_name, `_id`, district_name) %>%
  left_join(iv_lengths)





metadata_duration_exclusion <- time_delta %>%
  filter(metadata_iv_len < 25 |metadata_iv_len > 120)

"%!in%" <- Negate("%in%")

too_short_invs <- df %>%
  left_join(metadata_duration_exclusion, "_id") %>%
  filter(`_id` %in% metadata_duration_exclusion$`_id`)



valid_ivs <- time_delta %>%
  mutate(
    valid_length = case_when(
      metadata_iv_len < 25 ~ 0,
      metadata_iv_len > 120 ~ 0,
      TRUE ~ 1
    )
  )

valid_ivs_for_enum <- valid_ivs %>%
  group_by(district_name, enum_name) %>%
  summarise(valid_pct = mean(valid_length))


write_csv(valid_ivs_for_enum, "valid_interviews.csv")



valid_ivs_for_enum %>%
  ggplot(aes(valid_pct)) +
  geom_histogram()





# iv_lengths %>%
#   filter(metadata_iv_len < 120) %>%
#   ggplot(aes(metadata_iv_len)) +
#   geom_histogram(binwidth = 2) +
#   geom_vline(xintercept = 45) +
#   scale_x_continuous(breaks = seq(0, 120, 10))






