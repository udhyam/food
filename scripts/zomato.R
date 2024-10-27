# Setup Work Environment
# -----------------------------------------------------------------------------

# Clean the worskspace
rm(list = ls())

# Loading Packages
library(readxl)
library(tidyverse)
library(janitor)
library(lubridate)
library(googlesheets4)
library(psych)

# Read Data
# -----------------------------------------------------------------------------

# Login into Google
gs4_auth(cache = gargle::gargle_oauth_cache())

# Authorise Google Account
2

# Resturant Zomato data
# zomato_data <- read_excel("data/raw/zomato_data.xlsx")

zomato_data_aug_2024 <- read_sheet("https://docs.google.com/spreadsheets/d/1xZzTRD8-Pqe62fqcjMggHFMAXdOaP0HgUUGniMZasds/edit?usp=sharing", 
                                sheet = "Raw Data - Arpit")

# Save the data backup
write_csv(zomato_data_aug_2024, "data/processed/zomato_aug_2024.csv")

# Clean Data
# -----------------------------------------------------------------------------

# Standardise field names
zomato_data_aug_2024 <- clean_names(zomato_data_aug_2024)

# Clean numeric field types
zomato_data_aug_2024 <- zomato_data_aug_2024 |>
  mutate(bad_orders = as.double(bad_orders),
         online_percent = as.double(online_percent),
         rated_orders = as.double(rated_orders),
         new_users = as.double(new_users),
         repeat_users = as.double(repeat_users),
         orders_with_offers = as.double(orders_with_offers),
         impressions_to_menu = as.double(impressions_to_menu))

# Transform Data
# -----------------------------------------------------------------------------

# Data Labeling
# Define Labels
zomato_avg_data <- zomato_data_aug_2024 |>
  group_by(name) |>
  summarise(mean_orders = round(mean(delivered_orders)),
            mean_sales = round(mean(sales))) |>
  mutate(order_flag = if_else(mean_orders >= 30, ">=30",
                              if_else((mean_orders < 30 & mean_orders >= 15), ">=15 & <30", "<15")))

# Join to Original data
zomato_data_aug_2024 <- zomato_data_aug_2024 |>
  left_join(zomato_avg_data, by = join_by(name))

# Data Summary
# -----------------------------------------------------------------------------

# Quick Summary
paste0("Mean Sales: ₹", round(mean(zomato_data_aug_2024$sales, na.rm = TRUE), 1))
paste0("Median Sales: ₹", round(median(zomato_data_aug_2024$sales, na.rm = TRUE), 0))
paste0("Mean Orders: ", round(mean(zomato_data_aug_2024$delivered_orders, na.rm = TRUE), 1))
paste0("Median Orders: ", round(median(zomato_data_aug_2024$delivered_orders, na.rm = TRUE), 0))

# Level Summary
zomato_avg_data |>
  group_by(order_flag) |>
  summarise(count = n(),
            mean_sales = mean(mean_sales),
            mean_orders = mean(mean_orders)) |>
  arrange(desc(mean_sales))

# Remove the outlier vyapaari
zomato_data_filtered <- zomato_data_aug_2024 |>
  filter(name != "Gowdru Naati Style")

# Clean average rating field
zomato_data_filtered["average_rating"][zomato_data_filtered["average_rating"] == 0] <- NA

# Monthly Summary
zomato_data_filtered |>
  group_by(period) |>
  summarise(count = n_distinct(name),
            total_sales = sum(sales, na.rm = TRUE),
            total_orders = sum(delivered_orders, na.rm = TRUE),
            mean_sales = mean(sales, na.rm = TRUE),
            mean_orders = mean(delivered_orders, na.rm = TRUE),
            average_rating = mean(average_rating, na.rm = TRUE),
            repeat_users = mean(repeat_users, na.rm = TRUE))

# Data Visualisation
# -----------------------------------------------------------------------------

# Correlation Matrix
## Select Relevant Fields
zomato_analysis <- zomato_data_filtered |>
  select(sales, delivered_orders, average_rating, rated_orders, bad_orders,
         impressions, impressions_to_menu, online_percent,  new_users, repeat_users,
         ads_spend, sales_from_ads, orders_with_offers, discount_given)

# Make all zeros NA
zomato_analysis[zomato_analysis == 0] <- NA

## Visualisation
pairs.panels(zomato_analysis, 
             method = "pearson", # correlation method
             hist.col = "#00AFBB",
             density = TRUE,  # show density plots
             ellipses = TRUE,
            # show correlation ellipses
)

# Review Data
glimpse(zomato_data_aug_2024)
glimpse(zomato_analysis)


# ---
# ---
# ---


# Sankey Chart
# -----------------------------------------------------------------------------

sankey_l1 <- vyapaari_master_all |>
  mutate(pitched_flag = if_else(pitched_flag == "YES", "Pitched", "Not-Pitched"),
         vyapaari_interest_pitch = if_else(is.na(vyapaari_interest_pitch), "Interested",
                                           if_else(vyapaari_interest_pitch == "no", "Not-Interested",
                                                   if_else(vyapaari_interest_pitch == "maybe", "Maybe", "Interested")))) |>
  group_by(pitched_flag, vyapaari_interest_pitch) |>
  summarise(count = n()) |>
  rename(Source = pitched_flag, Destination = vyapaari_interest_pitch, Value = count) |>
  arrange(desc(Value))

sankey_l2 <- vyapaari_master_all |>
  mutate(vyapaari_interest_pitch = if_else(is.na(vyapaari_interest_pitch), "Interested",
                                           if_else(vyapaari_interest_pitch == "no", "Not-Interested",
                                                   if_else(vyapaari_interest_pitch == "maybe", "Maybe", "Interested"))),
         onboarded_flag = if_else(onboarded_flag == "YES", "Udhyam Onboarded", "Not-Onboarded")) |>
  group_by(vyapaari_interest_pitch, onboarded_flag) |>
  summarise(count = n()) |>
  rename(Source = vyapaari_interest_pitch, Destination = onboarded_flag, Value = count) |>
  arrange(desc(Value))

sankey_l3 <- vyapaari_master_all |>
  mutate(onboarded_flag = if_else(onboarded_flag == "YES", "Udhyam Onboarded", "Not-Onboarded"),
         fssai_issued_track = if_else((onboarded_flag == "Not-Onboarded" | online_business_shutdown_track == "yes"), "Dropped",
                                      if_else(is.na(fssai_issued_track), "FSSAI Pending", "FSSAI Issued"))) |>
  group_by(onboarded_flag, fssai_issued_track) |>
  summarise(count = n()) |>
  rename(Source = onboarded_flag, Destination = fssai_issued_track, Value = count) |>
  arrange(desc(Value))

# Combine Sankey data
sankley_master <- bind_rows(sankey_l1, sankey_l2, sankey_l3)

# Upload Sankey to Google Sheet
write_sheet(sankley_master, 
            ss = "https://docs.google.com/spreadsheets/d/1I7s-m2E6xXxP0A-c95lOJ3ebVqO_HoYXHNXIsXLXsXs/",
            sheet = "sankley_master")

