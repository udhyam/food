# Setup Work Environment
# -----------------------------------------------------------------------------

# Clean the workspace
rm(list = ls())

# Loading Packages
library(KoboconnectR)
library(tidyverse)
library(janitor)
library(lubridate)
library(googlesheets4)

# Load Credentials
# -----------------------------------------------------------------------------

# Load the KoboToolbox credentials
USER_ID <- Sys.getenv(c("USER_ID"))
PASSWORD <- Sys.getenv(c("PASSWORD"))

# Login into Kobo
kobo_token <- get_kobo_token(url = "kf.kobotoolbox.org", uname = USER_ID, pwd = PASSWORD)

# Login into Google
gs4_auth(cache = gargle::gargle_oauth_cache())

# Authorise Google Account
2

# List of Kobo forms data to download
# kobo_forms_list <- kobotools_api(url = "kf.kobotoolbox.org", simplified = F, uname = USER_ID, pwd = PASSWORD)

# Download Data
# -----------------------------------------------------------------------------

# Select forms to download
id_pitching_vyapaaris <- Sys.getenv(c("ID_PITCH_FORM"))
id_onboarding_vyapaaris <- Sys.getenv(c("ID_ONBOARD_FORM"))
id_tracker_vyapaaris <- Sys.getenv(c("ID_TRACK_FORM"))
id_case_vyapaaris <- Sys.getenv(c("ID_CASE_FORM"))

# Pitching - Vyapaaris
data_pitching_vyapaaris_raw <- 
  kobo_df_download(
    url = "kf.kobotoolbox.org",
    uname = USER_ID,
    pwd = PASSWORD,
    assetid = id_pitching_vyapaaris,
    lang = "_xml",
    sleep = 5
  )

# Onboarding - Vyapaaris
data_onboarding_vyapaaris_raw <- 
  kobo_df_download(
    url = "kf.kobotoolbox.org",
    uname = USER_ID,
    pwd = PASSWORD,
    assetid = id_onboarding_vyapaaris,
    lang = "_xml",
    sleep = 5
  )

# Tracker - Vyapaaris
data_tracker_vyapaaris_raw <- 
  kobo_df_download(
    url = "kf.kobotoolbox.org",
    uname = USER_ID,
    pwd = PASSWORD,
    assetid = id_tracker_vyapaaris,
    lang = "_xml",
    sleep = 5
  )

# Case Management - Vyapaaris
data_case_vyapaaris_raw <- 
  kobo_df_download(
    url = "kf.kobotoolbox.org",
    uname = USER_ID,
    pwd = PASSWORD,
    assetid = id_case_vyapaaris,
    lang = "_xml",
    sleep = 5
  )

# Load the pilot data
data_pilot_master <- read_sheet("https://docs.google.com/spreadsheets/d/1I7s-m2E6xXxP0A-c95lOJ3ebVqO_HoYXHNXIsXLXsXs/", 
                                sheet = "pilot_final")

# Clean the workspace
rm(kobo_token)

# Create Master Dataset
# -----------------------------------------------------------------------------

# Normalize field names
data_pitching_vyapaaris <- clean_names(data_pitching_vyapaaris_raw)
data_onboarding_vyapaaris <- clean_names(data_onboarding_vyapaaris_raw)
data_tracker_vyapaaris <- clean_names(data_tracker_vyapaaris_raw)
data_case_vyapaaris <- clean_names(data_case_vyapaaris_raw)

# Add suffixes to field names
data_pitching_vyapaaris <- data_pitching_vyapaaris |>
  rename_with( .fn = function(.x){paste0(.x, "_pitch")})
data_onboarding_vyapaaris <- data_onboarding_vyapaaris |>
  rename_with( .fn = function(.x){paste0(.x, "_onboard")})
data_tracker_vyapaaris <- data_tracker_vyapaaris |>
  rename_with( .fn = function(.x){paste0(.x, "_track")})

# Joining all the data
vyapaari_master <- data_pitching_vyapaaris |> 
  full_join(data_onboarding_vyapaaris, 
            by = join_by(vyapaari_phone_number_pitch == vyapaari_phone_number_onboard)) |>
  full_join(data_tracker_vyapaaris, 
            by = join_by(vyapaari_phone_number_pitch == vyapaari_phone_number_track)) |>
  mutate(phase = "cohort 1")

# Append the pilot data
vyapaari_master_all <- bind_rows(vyapaari_master, data_pilot_master)

# Clean the workspace
rm(data_pitching_vyapaaris, data_pitching_vyapaaris_raw,
   data_onboarding_vyapaaris, data_onboarding_vyapaaris_raw,
   data_tracker_vyapaaris, data_tracker_vyapaaris_raw,
   vyapaari_master, data_pilot_master)

# Transform the data
# -----------------------------------------------------------------------------

# Clean Dates
vyapaari_master_all <- vyapaari_master_all |>
  mutate(start_pitch = as.Date(start_pitch, "%Y-%m-%d"),
         start_onboard = as.Date(start_onboard, "%Y-%m-%d"),
         start_track = as.Date(start_track, "%Y-%m-%d"),
         fssai_issue_date_track = as.Date(fssai_issue_date_track, "%Y-%m-%d"),
         online_onboarding_date_track = as.Date(online_onboarding_date_track, "%Y-%m-%d"))

# Create Master Saathi Details
vyapaari_master_all <- vyapaari_master_all |>
    mutate(enumerator_location_final = 
           if_else(!(is.na(enumerator_location_pitch) | enumerator_location_pitch == ""), enumerator_location_pitch,
                   if_else(!(is.na(enumerator_location_onboard) | enumerator_location_onboard == ""), enumerator_location_onboard,
                           if_else(!(is.na(enumerator_location_track) | enumerator_location_track == ""), enumerator_location_track,
                                   "bangalore"))),
         enumerator_name_final = 
           if_else(!(is.na(enumerator_name_pitch) | enumerator_name_pitch == ""), enumerator_name_pitch,
                   if_else(!(is.na(enumerator_name_onboard) | enumerator_name_onboard == ""), enumerator_name_onboard,
                           if_else(!(is.na(enumerator_name_track) | enumerator_name_track == ""), enumerator_name_track,
                                   "harshith"))))

# Clean Vyapaari Details
vyapaari_master_all <- vyapaari_master_all |>
  mutate(vyapaari_name_final = 
           if_else(!(is.na(vyapaari_name_pitch) | vyapaari_name_pitch == ""), vyapaari_name_pitch,
                   if_else(!(is.na(vyapaari_name_onboard) | vyapaari_name_onboard == ""), vyapaari_name_onboard,
                           vyapaari_name_track)),
         vyapaari_phone_number_final = vyapaari_phone_number_pitch,
         fssai_flag = if_else(fssai_issued_track == "OK", "Issued",
                              if_else(fssai_applied_track == "OK", "Applied", "")),
         business_location_final = paste0(x_business_location_latitude_pitch, ", ", x_business_location_longitude_pitch),
         business_food_type_pitch = str_replace(str_trim(str_to_upper(business_food_type_pitch)), "AND", "&"))

# Vyapaari Stage Tracker
vyapaari_master_all <- vyapaari_master_all |>
  mutate(pitched_flag = if_else(!is.na(x_index_pitch), "YES", "NO"),
         onboarded_flag = if_else(!is.na(x_index_onboard), "YES", "NO"),
         tracked_flag = if_else(!is.na(x_index_track), "YES", "NO")) |>
  mutate(final_stage_flag = if_else(tracked_flag == "YES", "Platform Onboarded",
                                    if_else(onboarded_flag == "YES", "Udhyam Onboarded",
                                            "Pitched")))

# Vyapaari Stage Detailed Tracker
vyapaari_master_all <- vyapaari_master_all |>
  mutate(pitched_flag_detailed = if_else((onboarded_flag == "YES" | tracked_flag == "YES"), "Progressed",
                                         if_else((vyapaari_interest_pitch == "yes" & (today() - start_pitch) <= 14), "Progressed", "Dropped")),
         onboarded_flag_detailed = if_else(tracked_flag == "YES", "Progressed",
                                           if_else((today() - start_onboard) <= 14, "Progressed", "Dropped")),
         tracked_flag_detailed = if_else((is.na(online_business_shutdown_track) | online_business_shutdown_track == ""), "Active",
                                         if_else(online_business_shutdown_track == "yes", "Inactive",
                                                 if_else((today() - start_onboard) > 14 & fssai_issued_track != "OK", "Dropped",
                                                         if_else((today() - fssai_issue_date_track) > 14 & platforms_applied_zomato_track == 1, "Dropped",
                                                                 "Active"))))) |>
  mutate(onboarded_flag_detailed = if_else((pitched_flag_detailed == "Progressed" & is.na(onboarded_flag_detailed)),
                                           "Pipeline", onboarded_flag_detailed),
         tracked_flag_detailed = if_else((tracked_flag == "YES" & platforms_live_zomato_track == 1), tracked_flag_detailed, ""))

# Add Funder Flags
vyapaari_master_all <- vyapaari_master_all |>
  mutate(funder_flag = if_else(enumerator_location_final == "lucknow", "Bikhchandani", 
                               if_else(enumerator_location_final == "bangalore" & phase !=  "pilot" , "HSBC", "Not funded")),
         phase = if_else((!is.na(start_pitch) & start_pitch >= "2024-10-01") | 
                         (!is.na(start_onboard) & start_onboard >= "2024-10-01") | 
                         (!is.na(online_onboarding_date_track) & online_onboarding_date_track >= "2024-10-01"),
                         "cohort 2", phase))

# Case Master Data
# -----------------------------------------------------------------------------
vyapaari_master_case <- data_case_vyapaaris |>
  mutate(visit_date = as.Date(visit_date, "%Y-%m-%d")) |>
  filter(start != 1)

# Clean the workspace
rm(data_case_vyapaaris, data_case_vyapaaris_raw)

# Impact tracker data
# -----------------------------------------------------------------------------

# Calculate Online Revenues and Orders
vyapaari_master_tracked <- vyapaari_master_all |>
  rowwise() |>
  mutate(total_offline_revenue = financial_avg_revenue_daily_onboard*business_monthly_working_days_onboard,
         total_offline_expenses = financial_avg_expenses_daily_onboard*business_monthly_working_days_onboard,
         total_offline_profit = financial_profit_revenue_daily_onboard*business_monthly_working_days_onboard,
         total_online_revenue = sum(
           group_online_tracker_month_one_1_online_revenue_track, group_online_tracker_month_two_1_online_revenue_track,
           group_online_tracker_month_three_1_online_revenue_track, group_online_tracker_month_four_1_online_revenue_track,
           group_online_tracker_month_five_1_online_revenue_track, group_online_tracker_month_six_1_online_revenue_track,
           group_online_tracker_month_seven_1_online_revenue_track, group_online_tracker_month_eight_1_online_revenue_track,
           group_online_tracker_month_nine_1_online_revenue_track, group_online_tracker_month_ten_1_online_revenue_track,
           group_online_tracker_month_eleven_1_online_revenue_track, group_online_tracker_month_twelve_1_online_revenue_track, na.rm = TRUE),
         total_online_orders = sum(
           group_online_tracker_month_one_1_online_orders_track, group_online_tracker_month_two_1_online_orders_track,
           group_online_tracker_month_three_1_online_orders_track, group_online_tracker_month_four_1_online_orders_track,
           group_online_tracker_month_five_1_online_orders_track, group_online_tracker_month_six_1_online_orders_track,
           group_online_tracker_month_seven_1_online_orders_track, group_online_tracker_month_eight_1_online_orders_track,
           group_online_tracker_month_nine_1_online_orders_track, group_online_tracker_month_ten_1_online_orders_track,
           group_online_tracker_month_eleven_1_online_orders_track, group_online_tracker_month_twelve_1_online_orders_track, na.rm = TRUE),
         avg_online_revenue = mean(c_across(c(
           group_online_tracker_month_one_1_online_revenue_track, group_online_tracker_month_two_1_online_revenue_track,
           group_online_tracker_month_three_1_online_revenue_track, group_online_tracker_month_four_1_online_revenue_track,
           group_online_tracker_month_five_1_online_revenue_track, group_online_tracker_month_six_1_online_revenue_track,
           group_online_tracker_month_seven_1_online_revenue_track, group_online_tracker_month_eight_1_online_revenue_track,
           group_online_tracker_month_nine_1_online_revenue_track, group_online_tracker_month_ten_1_online_revenue_track,
           group_online_tracker_month_eleven_1_online_revenue_track, group_online_tracker_month_twelve_1_online_revenue_track)), na.rm = TRUE),
         avg_online_orders = mean(c_across(c(
           group_online_tracker_month_one_1_online_orders_track, group_online_tracker_month_two_1_online_orders_track,
           group_online_tracker_month_three_1_online_orders_track, group_online_tracker_month_four_1_online_orders_track,
           group_online_tracker_month_five_1_online_orders_track, group_online_tracker_month_six_1_online_orders_track,
           group_online_tracker_month_seven_1_online_orders_track, group_online_tracker_month_eight_1_online_orders_track,
           group_online_tracker_month_nine_1_online_orders_track, group_online_tracker_month_ten_1_online_orders_track,
           group_online_tracker_month_eleven_1_online_orders_track, group_online_tracker_month_twelve_1_online_orders_track)), na.rm = TRUE),
         avg_online_revenue_q1 = mean(c_across(c(
           group_online_tracker_month_one_1_online_revenue_track,
           group_online_tracker_month_two_1_online_revenue_track,
           group_online_tracker_month_three_1_online_revenue_track)), na.rm = TRUE),
         avg_online_orders_q1 = mean(c_across(c(
           group_online_tracker_month_one_1_online_orders_track,
           group_online_tracker_month_two_1_online_orders_track,
           group_online_tracker_month_three_1_online_orders_track)), na.rm = TRUE),
         avg_online_revenue_q2 = mean(c_across(c(
           group_online_tracker_month_four_1_online_revenue_track,
           group_online_tracker_month_five_1_online_revenue_track,
           group_online_tracker_month_six_1_online_revenue_track)), na.rm = TRUE),
         avg_online_orders_q2 = mean(c_across(c(
           group_online_tracker_month_four_1_online_orders_track,
           group_online_tracker_month_five_1_online_orders_track,
           group_online_tracker_month_six_1_online_orders_track)), na.rm = TRUE),
         avg_online_revenue_q3 = mean(c_across(c(
           group_online_tracker_month_seven_1_online_revenue_track,
           group_online_tracker_month_eight_1_online_revenue_track,
           group_online_tracker_month_nine_1_online_revenue_track)), na.rm = TRUE),
         avg_online_orders_q3 = mean(c_across(c(
           group_online_tracker_month_seven_1_online_orders_track,
           group_online_tracker_month_eight_1_online_orders_track,
           group_online_tracker_month_nine_1_online_orders_track)), na.rm = TRUE)) |>
  mutate(revenue_share = (avg_online_revenue / total_offline_revenue))

# Subset for Export
# -----------------------------------------------------------------------------

# Overall Vyapaari Subset
vyapaari_master_all <- vyapaari_master_all |>
  select(enumerator_location_final, enumerator_name_final, vyapaari_name_final, vyapaari_phone_number_final,
         start_pitch, end_pitch, vyapaari_gender_pitch, vyapaari_age_pitch,
         vyapaari_photograph_pitch, vyapaari_photograph_url_pitch, business_name_pitch,
         vyapaari_smartphone_access_pitch, vyapaari_smartphone_usability_pitch,
         business_location_final, business_locality_pitch, business_photograph_pitch, business_photograph_url_pitch,
         business_facility_type_pitch, business_food_type_pitch, vyapaari_online_awareness_pitch, vyapaari_online_listing_pitch,
         vyapaari_licence_awareness_pitch, vyapaari_licence_issued_pitch, vyapaari_interest_pitch, vyapaari_other_details_pitch,
         x_id_pitch, x_index_pitch,
         start_onboard, end_onboard, business_hygiene_onboard, business_monthly_working_days_onboard,
         business_start_time_onboard, business_end_time_onboard, 
         business_start_year_current_onboard, business_start_year_food_onboard,
         business_total_staff_onboard, business_family_staff_onboard,
         business_location_challenges_onboard, financial_bank_account_onboard, financial_digital_payments_onboard,
         financial_avg_revenue_daily_onboard, financial_avg_expenses_daily_onboard, financial_profit_revenue_daily_onboard,
         financial_other_income_onboard, vyapaari_family_size_onboard, vyapaari_sole_earner_onboard, vyapaari_earning_family_onboard,
         vyapaari_family_residence_onboard, vyapaari_family_origin_onboard,
         vyapaari_intent_onboard, vyapaari_onboarding_check_onboard,
         vyapaari_onboarding_check_activation_onboard, vyapaari_onboarding_check_maintenance_onboard, vyapaari_other_details_onboard,
         x_id_onboard, x_index_onboard,
         start_track, end_track, fssai_applied_track, fssai_issued_track, fssai_issue_date_track,
         fssai_registration_number_track, fssai_flag, platforms_applied_track, platforms_live_track, 
         platforms_applied_zomato_track, platforms_live_zomato_track, zomato_id_number_track, zomato_link_track,
         platforms_applied_swiggy_track, platforms_live_swiggy_track, swiggy_id_number_track, swiggy_link_track,
         platforms_applied_ondc_track, platforms_live_ondc_track, ondc_id_number_track, ondc_link_track,
         online_onboarding_date_track, online_setup_steps_track,
         online_setup_steps_images_track, online_setup_steps_description_track, online_setup_steps_recco_track,
         online_setup_steps_distance_track, online_setup_steps_discounts_track, online_setup_steps_combos_track,
         online_setup_steps_logo_track, online_business_shutdown_track, online_business_shutdown_month_track,
         vyapaari_other_details_track, x_id_track, x_index_track,
         pitched_flag, onboarded_flag, tracked_flag, final_stage_flag,
         pitched_flag_detailed, onboarded_flag_detailed, tracked_flag_detailed, phase, funder_flag)

# Ops Master Subset
vyapaari_master_ops <- vyapaari_master_all |>
  select(location_name = enumerator_location_final, saathi_name = enumerator_name_final,
         vyapaari_name = vyapaari_name_final, vyapaari_phone_number = vyapaari_phone_number_final,
         business_name = business_name_pitch, business_food_type = business_food_type_pitch,
         zomato_id = zomato_link_track,
         daily_offline_revenue = financial_avg_revenue_daily_onboard, 
         daily_offline_expense = financial_avg_expenses_daily_onboard,
         daily_offline_profit = financial_profit_revenue_daily_onboard,
         pitch_status = pitched_flag_detailed, udhyam_onboard_status = onboarded_flag_detailed, 
         fassai_issued_status = fssai_issued_track, platform_onboard_status = tracked_flag_detailed,
         online_business_shutdown = online_business_shutdown_track, funder = funder_flag, phase)

# Case Master Subset
vyapaari_master_case <- vyapaari_master_case |>
  select(enumerator_name, vyapaari_name, vyapaari_phone_number, shop_name, shop_closed,
         visit_type, visit_date, visit_start_time, visit_end_time,
         problem_discussed, problem_discussed_other, solution_proposed, action_items_proposed, 
         vyapaari_agreement, vyapaari_disagreement, next_visit_date, rating, vyapaari_continued, 
         follow_up_conversation, vyapaari_action, vyapaari_action_impact, vyapaari_inaction_reason, 
         follow_up_solution, reponse_reason)

# Impact Tracker Subset
vyapaari_master_tracked <- vyapaari_master_tracked |>
  filter(platforms_live_zomato_track == 1 & phase == "cohort 1") |>
  filter(!is.na(start_track)) %>%
  select(vyapaari_name_pitch, vyapaari_phone_number_pitch, 
         business_name_pitch, business_food_type_pitch, business_facility_type_pitch,
         financial_avg_revenue_daily_onboard, financial_avg_expenses_daily_onboard, financial_profit_revenue_daily_onboard,
         total_offline_revenue, total_offline_expenses, total_offline_expenses, total_offline_profit,
         total_online_revenue, total_online_orders, avg_online_revenue, avg_online_orders,
         avg_online_revenue_q1, avg_online_orders_q1, avg_online_revenue_q2, avg_online_orders_q2,
         avg_online_revenue_q3, avg_online_orders_q3,
         group_online_tracker_month_one_1_online_revenue_track, group_online_tracker_month_two_1_online_revenue_track,
         group_online_tracker_month_three_1_online_revenue_track, group_online_tracker_month_four_1_online_revenue_track,
         group_online_tracker_month_five_1_online_revenue_track, group_online_tracker_month_six_1_online_revenue_track,
         group_online_tracker_month_seven_1_online_revenue_track, group_online_tracker_month_eight_1_online_revenue_track,
         group_online_tracker_month_nine_1_online_revenue_track, group_online_tracker_month_ten_1_online_revenue_track,
         group_online_tracker_month_eleven_1_online_revenue_track, group_online_tracker_month_twelve_1_online_revenue_track,
         group_online_tracker_month_one_1_online_orders_track, group_online_tracker_month_two_1_online_orders_track,
         group_online_tracker_month_three_1_online_orders_track, group_online_tracker_month_four_1_online_orders_track,
         group_online_tracker_month_five_1_online_orders_track, group_online_tracker_month_six_1_online_orders_track, 
         group_online_tracker_month_seven_1_online_orders_track, group_online_tracker_month_eight_1_online_orders_track,
         group_online_tracker_month_nine_1_online_orders_track, group_online_tracker_month_ten_1_online_orders_track,
         group_online_tracker_month_eleven_1_online_orders_track, group_online_tracker_month_twelve_1_online_orders_track,
         revenue_share, final_stage_flag, funder_flag)

# Export the data
# -----------------------------------------------------------------------------

# Overall Master
write_sheet(vyapaari_master_all, 
            ss = "https://docs.google.com/spreadsheets/d/1I7s-m2E6xXxP0A-c95lOJ3ebVqO_HoYXHNXIsXLXsXs/",
            sheet = "vyapaari_master_final")

# Ops Tracker Master
write_sheet(vyapaari_master_ops, 
            ss = "https://docs.google.com/spreadsheets/d/1UXs61qRt0sVRm0o2kfOC9PgdJ1jrpwsXuEFOeiGEduA/",
            sheet = "vyapaari_master_ops")

# Case Tracker Master
write_sheet(vyapaari_master_case, 
            ss = "https://docs.google.com/spreadsheets/d/1I7s-m2E6xXxP0A-c95lOJ3ebVqO_HoYXHNXIsXLXsXs/",
            sheet = "vyapaari_master_case")

# Impact Tracker Master
write_sheet(vyapaari_master_tracked, 
            ss = "https://docs.google.com/spreadsheets/d/1I7s-m2E6xXxP0A-c95lOJ3ebVqO_HoYXHNXIsXLXsXs/",
            sheet = "vyapaari_master_tracked")

# -----------------------------------------------------------------------------
# Known issues to be fixed
# - 10 digit phone numbers
# - Data gaps across
