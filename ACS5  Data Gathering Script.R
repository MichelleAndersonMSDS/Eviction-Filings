"""
DATA GATHERING AND PRE-PROCESSING SCRIPT FOR ACS 5-YEAR ESTIMATES

Created on Sat Nov 18 2023
@author: Michelle.Anderson

This code performs the following tasks:
1. Set up Census Bureau API parameters
2. Gather data from Census Bureau API
3. Merge data sets
4. Conform and augmet data
5. Saves out processed data file for analysis

Sources:
https://www.census.gov/data/developers/data-sets/acs-5year.html
"""

################################################################################
# LOAD LIBRARIES
################################################################################

library(censusapi)
library(tidyverse)
library(dtplyr)

################################################################################
# SET FILE PATHS AND API KEYS
################################################################################

filepath = ""

mykey = ""

################################################################################
# DEFINE FUNCTIONS
################################################################################

# Function to set parameters fo Census Bureau API
acs_tract_api <- function(year, vars, type) {
  acs_group <- getCensus(
    name = paste0("acs/acs5/", type),
    key = mykey,
    vintage = year,
    vars = vars,
    region = "tract:*",
    regionin = "state:55+county:079")
}

# Function to gather data from Census Bureau API
ACS_data_gatherer <- function(start_year, end_year, group_vars, table_type, naming_file) {
  
  var_list <- read_csv(paste0(filepath, naming_file))
  df_full <- data.frame(matrix(ncol = nrow(var_list), nrow = 0))
  colnames(df_full) <- as.list(var_list$VARIABLE_NAME)
  df_full <- df_full %>% add_column(ACS_Year)
  year = start_year
  
  while(year < end_year + 1) {
    print(year)
    data <- acs_tract_api(year, group_vars, table_type) %>% mutate(ACS_Year = year)
    names(data) <- names(df_full)
    df_full <- rbind(df_full, data)
    year = year + 1
  }
  
  return(df_full)
}

################################################################################
# DEFINE ACS QUESTION GROUPS
################################################################################

S2501_Qs <- c('GEO_ID', 'NAME', 'S2501_C05_001E', 'S2501_C05_002E', 'S2501_C05_003E', 'S2501_C05_004E', 
              'S2501_C05_005E', 'S2501_C05_006E', 'S2501_C05_007E', 'S2501_C05_008E', 'S2501_C05_009E', 
              'S2501_C05_010E', 'S2501_C05_011E', 'S2501_C05_012E', 'S2501_C05_013E', 'S2501_C05_014E', 
              'S2501_C05_015E', 'S2501_C05_016E', 'S2501_C05_017E', 'S2501_C05_018E', 'S2501_C05_019E', 
              'S2501_C05_020E', 'S2501_C05_021E', 'S2501_C05_022E', 'S2501_C05_023E', 'S2501_C05_024E', 
              'S2501_C05_025E', 'S2501_C05_026E', 'S2501_C05_027E', 'S2501_C05_028E', 'S2501_C05_029E', 
              'S2501_C05_030E', 'S2501_C05_031E', 'S2501_C05_032E', 'S2501_C05_033E', 'S2501_C05_034E', 
              'S2501_C05_035E', 'S2501_C05_036E', 'S2501_C05_037E', 'S2501_C05_038E', 'S2501_C06_001E', 
              'S2501_C06_002E', 'S2501_C06_003E', 'S2501_C06_004E', 'S2501_C06_005E', 'S2501_C06_006E', 
              'S2501_C06_007E', 'S2501_C06_008E', 'S2501_C06_009E', 'S2501_C06_010E', 'S2501_C06_011E', 
              'S2501_C06_012E', 'S2501_C06_013E', 'S2501_C06_014E', 'S2501_C06_015E', 'S2501_C06_016E', 
              'S2501_C06_017E', 'S2501_C06_018E', 'S2501_C06_019E', 'S2501_C06_020E', 'S2501_C06_021E', 
              'S2501_C06_022E', 'S2501_C06_023E', 'S2501_C06_024E', 'S2501_C06_025E', 'S2501_C06_026E', 
              'S2501_C06_027E', 'S2501_C06_028E', 'S2501_C06_029E', 'S2501_C06_030E', 'S2501_C06_031E', 
              'S2501_C06_032E', 'S2501_C06_033E', 'S2501_C06_034E', 'S2501_C06_035E', 'S2501_C06_036E', 
              'S2501_C06_037E', 'S2501_C06_038E')

S2502_Qs <- c('GEO_ID', 'NAME', 'S2502_C05_001E', 'S2502_C05_002E', 'S2502_C05_003E', 'S2502_C05_004E',
              'S2502_C05_005E', 'S2502_C05_006E', 'S2502_C05_007E', 'S2502_C05_008E', 'S2502_C05_009E',
              'S2502_C05_010E', 'S2502_C05_011E', 'S2502_C05_012E', 'S2502_C05_013E', 'S2502_C05_014E',
              'S2502_C05_015E', 'S2502_C05_016E', 'S2502_C05_017E', 'S2502_C05_018E', 'S2502_C05_019E',
              'S2502_C05_020E', 'S2502_C05_021E', 'S2502_C05_022E', 'S2502_C05_023E', 'S2502_C05_024E',
              'S2502_C05_025E', 'S2502_C05_026E', 'S2502_C05_027E', 'S2502_C06_001E', 'S2502_C06_002E',
              'S2502_C06_003E', 'S2502_C06_004E', 'S2502_C06_005E', 'S2502_C06_006E', 'S2502_C06_007E',
              'S2502_C06_008E', 'S2502_C06_009E', 'S2502_C06_010E', 'S2502_C06_011E', 'S2502_C06_012E',
              'S2502_C06_013E', 'S2502_C06_014E', 'S2502_C06_015E', 'S2502_C06_016E', 'S2502_C06_017E',
              'S2502_C06_018E', 'S2502_C06_019E', 'S2502_C06_020E', 'S2502_C06_021E', 'S2502_C06_022E',
              'S2502_C06_023E', 'S2502_C06_024E', 'S2502_C06_025E', 'S2502_C06_026E', 'S2502_C06_027E')

S2503_Qs <- c('GEO_ID', 'NAME', 'S2503_C05_001E', 'S2503_C05_002E', 'S2503_C05_003E', 'S2503_C05_004E', 
              'S2503_C05_005E', 'S2503_C05_006E', 'S2503_C05_007E', 'S2503_C05_008E', 'S2503_C05_009E', 
              'S2503_C05_010E', 'S2503_C05_011E', 'S2503_C05_012E', 'S2503_C05_013E', 'S2503_C05_014E', 
              'S2503_C05_015E', 'S2503_C05_016E', 'S2503_C05_017E', 'S2503_C05_018E', 'S2503_C05_019E', 
              'S2503_C05_020E', 'S2503_C05_021E', 'S2503_C05_022E', 'S2503_C05_023E', 'S2503_C05_024E', 
              'S2503_C05_025E', 'S2503_C05_026E', 'S2503_C05_027E', 'S2503_C05_028E', 'S2503_C05_029E', 
              'S2503_C05_030E', 'S2503_C05_031E', 'S2503_C05_032E', 'S2503_C05_033E', 'S2503_C05_034E', 
              'S2503_C05_035E', 'S2503_C05_036E', 'S2503_C05_037E', 'S2503_C05_038E', 'S2503_C05_039E', 
              'S2503_C05_040E', 'S2503_C05_041E', 'S2503_C05_042E', 'S2503_C05_043E', 'S2503_C05_044E', 
              'S2503_C05_045E', 'S2503_C05_046E', 'S2503_C06_001E', 'S2503_C06_002E', 'S2503_C06_003E', 
              'S2503_C06_004E', 'S2503_C06_005E', 'S2503_C06_006E', 'S2503_C06_007E', 'S2503_C06_008E', 
              'S2503_C06_009E', 'S2503_C06_010E', 'S2503_C06_011E', 'S2503_C06_012E', 'S2503_C06_013E', 
              'S2503_C06_014E', 'S2503_C06_015E', 'S2503_C06_016E', 'S2503_C06_017E', 'S2503_C06_018E', 
              'S2503_C06_019E', 'S2503_C06_020E', 'S2503_C06_021E', 'S2503_C06_022E', 'S2503_C06_023E', 
              'S2503_C06_024E', 'S2503_C06_025E', 'S2503_C06_026E', 'S2503_C06_027E', 'S2503_C06_028E', 
              'S2503_C06_029E', 'S2503_C06_030E', 'S2503_C06_031E', 'S2503_C06_032E', 'S2503_C06_033E', 
              'S2503_C06_034E', 'S2503_C06_035E', 'S2503_C06_036E', 'S2503_C06_037E', 'S2503_C06_038E', 
              'S2503_C06_039E', 'S2503_C06_040E', 'S2503_C06_041E', 'S2503_C06_042E', 'S2503_C06_043E', 
              'S2503_C06_044E', 'S2503_C06_045E', 'S2503_C06_046E')

S2504_Qs <- c('GEO_ID', 'NAME', 'S2504_C05_001E', 'S2504_C05_002E', 'S2504_C05_003E', 'S2504_C05_004E', 
              'S2504_C05_005E', 'S2504_C05_006E', 'S2504_C05_007E', 'S2504_C05_008E', 'S2504_C05_009E', 
              'S2504_C05_010E', 'S2504_C05_011E', 'S2504_C05_012E', 'S2504_C05_013E', 'S2504_C05_014E', 
              'S2504_C05_015E', 'S2504_C05_016E', 'S2504_C05_017E', 'S2504_C05_018E', 'S2504_C05_019E', 
              'S2504_C05_020E', 'S2504_C05_021E', 'S2504_C05_022E', 'S2504_C05_023E', 'S2504_C05_024E', 
              'S2504_C05_025E', 'S2504_C05_026E', 'S2504_C05_027E', 'S2504_C05_028E', 'S2504_C05_029E', 
              'S2504_C05_030E', 'S2504_C05_031E', 'S2504_C05_032E', 'S2504_C05_033E', 'S2504_C05_034E', 
              'S2504_C05_035E', 'S2504_C05_036E', 'S2504_C05_037E', 'S2504_C05_038E', 'S2504_C06_001E', 
              'S2504_C06_002E', 'S2504_C06_003E', 'S2504_C06_004E', 'S2504_C06_005E', 'S2504_C06_006E', 
              'S2504_C06_007E', 'S2504_C06_008E', 'S2504_C06_009E', 'S2504_C06_010E', 'S2504_C06_011E', 
              'S2504_C06_012E', 'S2504_C06_013E', 'S2504_C06_014E', 'S2504_C06_015E', 'S2504_C06_016E', 
              'S2504_C06_017E', 'S2504_C06_018E', 'S2504_C06_019E', 'S2504_C06_020E', 'S2504_C06_021E', 
              'S2504_C06_022E', 'S2504_C06_023E', 'S2504_C06_024E', 'S2504_C06_025E', 'S2504_C06_026E', 
              'S2504_C06_027E', 'S2504_C06_028E', 'S2504_C06_029E', 'S2504_C06_030E', 'S2504_C06_031E', 
              'S2504_C06_032E', 'S2504_C06_033E', 'S2504_C06_034E', 'S2504_C06_035E', 'S2504_C06_036E', 
              'S2504_C06_037E', 'S2504_C06_038E')

Gross_Rent_Qs <- c('GEO_ID', 'NAME', 'B25063_003E', 'B25063_004E', 'B25063_005E', 'B25063_006E',
                   'B25063_007E', 'B25063_008E', 'B25063_009E', 'B25063_010E', 'B25063_011E',
                   'B25063_012E', 'B25063_013E', 'B25063_014E', 'B25063_015E', 'B25063_016E',
                   'B25063_017E', 'B25063_018E', 'B25063_019E', 'B25063_020E', 'B25063_021E',
                   'B25063_022E', 'B25063_023E', 'B25063_024E', 'B25063_025E', 'B25063_026E', 
                   'B25063_027E', 'B25064_001E')

################################################################################
# GATHER DATA
################################################################################

ACS_S2501 <- ACS_data_gatherer(2019, 2021, S2501_Qs, 'subject', 'S2501 Clean Var Names.csv')
ACS_S2502 <- ACS_data_gatherer(2019, 2021, S2502_Qs, 'subject', 'S2502 Clean Var Names.csv')
ACS_S2503 <- ACS_data_gatherer(2019, 2021, S2503_Qs, 'subject', 'S2503 Clean Var Names.csv')
ACS_S2504 <- ACS_data_gatherer(2019, 2021, S2504_Qs, 'subject', 'S2504 Clean Var Names.csv')
ACS_Gross_Rent <- ACS_data_gatherer(2019, 2021, Gross_Rent_Qs, '', 'Gross Rent Clean Var Names.csv')

################################################################################
# MERGE DATASETS
################################################################################

housing_data <- left_join(ACS_S2501, ACS_S2502, by=c('state', 'county', 'tract', 'GEO_ID', 'NAME', 'ACS_Year'), suffix = c("",""))
housing_data <- left_join(housing_data, ACS_S2503, by=c('state', 'county', 'tract', 'GEO_ID', 'NAME', 'ACS_Year'), suffix = c("",""))
housing_data <- left_join(housing_data, ACS_S2504, by=c('state', 'county', 'tract', 'GEO_ID', 'NAME', 'ACS_Year'), suffix = c("",""))
housing_data <- left_join(housing_data, ACS_Gross_Rent, by=c('state', 'county', 'tract', 'GEO_ID', 'NAME', 'ACS_Year'), suffix = c("",""))

################################################################################
# FORMAT DATA
################################################################################

housing_data$GEO_ID <- str_sub(housing_data$GEO_ID, start= -11)  # Conform GEOID
housing_data$NAME <- gsub(', Milwaukee County, Wisconsin', '', housing_data$NAME)  # Conform census tract name
housing_data[housing_data == -666666666] <- NA  # Define NA values

################################################################################
# SAVE DATASET
################################################################################

write_csv(housing_data, paste0(filepath, "MKE ACS Housing Longitudinal Data.csv")

