  # Setup Work Environment
  # -----------------------------------------------------------------------------
  
  # Clean the worskspace
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
  data_pitching_vyapaaris <- 
    kobo_df_download(
      url = "kf.kobotoolbox.org",
      uname = USER_ID,
      pwd = PASSWORD,
      assetid = id_pitching_vyapaaris,
      lang = "_xml",
      sleep = 5
    )
  
  # Onboarding - Vyapaaris
  data_onboarding_vyapaaris <- 
    kobo_df_download(
      url = "kf.kobotoolbox.org",
      uname = USER_ID,
      pwd = PASSWORD,
      assetid = id_onboarding_vyapaaris,
      lang = "_xml",
      sleep = 5
    )
  
  # Tracker - Vyapaaris
  data_tracker_vyapaaris <- 
    kobo_df_download(
      url = "kf.kobotoolbox.org",
      uname = USER_ID,
      pwd = PASSWORD,
      assetid = id_tracker_vyapaaris,
      lang = "_xml",
      sleep = 5
    )
  
  # Case Management - Vyapaaris
  data_case_vyapaaris <- 
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
  
  # Clean the data
  # -----------------------------------------------------------------------------
  
  # Normalize field names
  data_pitching_vyapaaris <- clean_names(data_pitching_vyapaaris)
  data_onboarding_vyapaaris <- clean_names(data_onboarding_vyapaaris)
  data_tracker_vyapaaris <- clean_names(data_tracker_vyapaaris)
  
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
  
  # Transform the data
  # -----------------------------------------------------------------------------
  
  # Vyapaari Stage Tracker
  vyapaari_master_all <- vyapaari_master_all |>
    mutate(pitched_flag = if_else(!is.na(x_index_pitch), "YES", "NO"),
           onboarded_flag = if_else(!is.na(x_index_onboard), "YES", "NO"),
           tracked_flag = if_else(!is.na(x_index_track), "YES", "NO")) |>
    mutate(final_stage_flag = if_else(tracked_flag == "YES", "Platform Onboarded",
                                      if_else(onboarded_flag == "YES", "Udhyam Onboarded",
                                              "Pitched")))
  
  # Vyapaari Location Tracker
  vyapaari_master_all <- vyapaari_master_all |>
    mutate(business_location_final = paste0(x_business_location_latitude_pitch, ", ", x_business_location_longitude_pitch))
  
  # Shop Food Type
  vyapaari_master_all <- vyapaari_master_all |> 
    mutate(business_food_type_pitch = str_replace(str_trim(str_to_upper(business_food_type_pitch)), "AND", "&"))
  
  # Shop Food Type
  vyapaari_master_all <- vyapaari_master_all |> 
    mutate(business_food_type_pitch = str_replace(str_trim(str_to_upper(business_food_type_pitch)), "AND", "&"))
  
  # Create master fields for Dates, Locations, Vyapaaris, FSSAI 
  vyapaari_master_all <- vyapaari_master_all |>
    mutate(start_pitch = as.Date(start_pitch, "%Y-%m-%d"),
           start_onboard = as.Date(start_onboard, "%Y-%m-%d"),
           start_track = as.Date(start_track, "%Y-%m-%d"),
           fssai_issue_date_track = as.Date(fssai_issue_date_track, "%Y-%m-%d")) |>
    mutate(enumerator_location_final = 
             if_else(!(is.na(enumerator_location_pitch) | enumerator_location_pitch == ""), enumerator_location_pitch,
                     if_else(!(is.na(enumerator_location_onboard) | enumerator_location_onboard == ""), enumerator_location_onboard,
                             if_else(!(is.na(enumerator_location_track) | enumerator_location_track == ""), enumerator_location_track,
                                     "bangalore"))),
           enumerator_name_final = 
             if_else(!(is.na(enumerator_name_pitch) | enumerator_name_pitch == ""), enumerator_name_pitch,
                     if_else(!(is.na(enumerator_name_onboard) | enumerator_name_onboard == ""), enumerator_name_onboard,
                             if_else(!(is.na(enumerator_name_track) | enumerator_name_track == ""), enumerator_name_track,
                                     "harshith"))),
           vyapaari_name_final = 
             if_else(!(is.na(vyapaari_name_pitch) | vyapaari_name_pitch == ""), vyapaari_name_pitch,
                     if_else(!(is.na(vyapaari_name_onboard) | vyapaari_name_onboard == ""), vyapaari_name_onboard,
                             vyapaari_name_track)),
           vyapaari_phone_number_final = vyapaari_phone_number_pitch,
           fssai_flag = if_else(fssai_issued_track == "OK", "issued",
                                if_else(fssai_applied_track == "OK", "applied", "")))
  
  # Funder Flags
  vyapaari_master_all <- vyapaari_master_all |>
    mutate(online_onboarding_date_track = as.Date(online_onboarding_date_track, "%Y-%m-%d")) |>
    mutate(funder_flag = if_else(enumerator_location_final == "lucknow", "Not funded", 
                                 if_else(online_onboarding_date_track >= "2024-04-01" & !is.na(online_onboarding_date_track) |
                                   (final_stage_flag == "Udhyam Onboarded" | final_stage_flag == "Pitched"),
                                   "Bikhchandani", "Not funded")))
  
  # Avg Online Revenues and Orders
  vyapaari_master_all <- vyapaari_master_all |>
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
             group_online_tracker_month_nine_1_online_orders_track)), na.rm = TRUE))
  
  # Updated Funnel Data
  # -----------------------------------------------------------------------------
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
  
  # Export the data
  # -----------------------------------------------------------------------------
  
  # Create a .CSV backup
  write_csv(vyapaari_master_all, "data/processed/food_vyapaari_master.csv")
  
  # Upload file for dashboard
  write_sheet(vyapaari_master_all, 
              ss = "https://docs.google.com/spreadsheets/d/1I7s-m2E6xXxP0A-c95lOJ3ebVqO_HoYXHNXIsXLXsXs/",
              sheet = "vyapaari_master_final")
  
  # Tracker Data Analysis
  # -----------------------------------------------------------------------------
  
  # Create Subset Dataset
  vyapaari_master_tracked <- vyapaari_master_all |>
    filter(final_stage_flag == "Platform Onboarded" & phase == "cohort 1") |>
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
           funder_flag)
  
  # Measure Impact
  vyapaari_master_tracked <- vyapaari_master_tracked |>
    mutate(revenue_share = (avg_online_revenue / total_offline_revenue))
  
  # Write to Google Sheet
  write_sheet(vyapaari_master_tracked, 
              ss = "https://docs.google.com/spreadsheets/d/1I7s-m2E6xXxP0A-c95lOJ3ebVqO_HoYXHNXIsXLXsXs/",
              sheet = "vyapaari_master_tracked")
  
  # Ops Data Master
  # -----------------------------------------------------------------------------
  
  # Create Subset Dataset
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
    
  # Write to Google Sheet
  write_sheet(vyapaari_master_ops, 
              ss = "https://docs.google.com/spreadsheets/d/1UXs61qRt0sVRm0o2kfOC9PgdJ1jrpwsXuEFOeiGEduA/",
              sheet = "vyapaari_master_ops")
  
  # Case Management Data
  # -----------------------------------------------------------------------------
  
  # Clean the data
  data_case_vyapaaris <- data_case_vyapaaris |>
    mutate(Visit_Date = as.Date(Visit_Date, "%Y-%m-%d")) |>
    filter(start != 1)
  
  # Write to Google Sheet
  write_sheet(data_case_vyapaaris, 
              ss = "https://docs.google.com/spreadsheets/d/1I7s-m2E6xXxP0A-c95lOJ3ebVqO_HoYXHNXIsXLXsXs/",
              sheet = "data_case_vyapaaris")
  # -----------------------------------------------------------------------------
  # Known issues to be fixed
  # - 10 digit phone numbers
  
  vyapaari_master_all |>
    group_by(enumerator_location_final, funder_flag, final_stage_flag) |>
    summarise(count = n())
  
  
  vyapaari_master_all |>
    group_by(pitched_flag, pitched_flag_detailed) |>
    summarise(n())
  
  vyapaari_master_all |>
    group_by(onboarded_flag, onboarded_flag_detailed) |>
    summarise(n())
  
  
  vyapaari_master_all |>
    filter(enumerator_location_final == "lucknow") |>
    group_by(pitched_flag, pitched_flag_detailed,
             onboarded_flag, onboarded_flag_detailed,
             tracked_flag, tracked_flag_detailed,
             final_stage_flag) |>
    summarise(count = n()) |>
    arrange(desc(count))
