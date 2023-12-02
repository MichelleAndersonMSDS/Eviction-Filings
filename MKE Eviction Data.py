"""
DATA GATHERING AND PRE-PROCESSING SCRIPT FOR MILWAUKEE COUNTY EVICTION FILINGS

Created on Sat Nov 18 2023
@author: Michelle.Anderson

This code performs the following tasks:
1. Ingests weekly, monthly, and legacy eviction filing data from The Eviction Lab
2. Creates a yearly eviction filing data set
3. Conforms and standardizes data sets for merging
4. Augments data sets with new measures and dimensions
5. Saves out processed data file for analysis

Sources:
https://data-downloads.evictionlab.org
https://www.census.gov/data/developers/data-sets/acs-5year.html
"""

################################################################################
# LOAD LIBRARIES
################################################################################

import pandas as pd
import numpy as np
import requests
import io

################################################################################
# READ IN DATA
################################################################################

# Load American Community Survey 5-Year Estimates CSV file
ACS_full = pd.read_csv(filepath + 'MKE ACS Housing Data.csv')

# Load monthly eviction filing data from the EvictionLab.org
url = 'https://evictionlab.org/uploads/milwaukee_monthly_2020_2021.csv'
urlData = requests.get(url).content
evict_mos = pd.read_csv(io.StringIO(urlData.decode('utf-8')))

# Load weekly eviction filing data from the EvictionLab.org
url = 'https://evictionlab.org/uploads/milwaukee_weekly_2020_2021.csv'
urlData = requests.get(url).content
evict_wk = pd.read_csv(io.StringIO(urlData.decode('utf-8')))

# Load legacy eviction filing data from the EvictionLab.org
url = 'https://eviction-lab-data-downloads.s3.amazonaws.com/legacy-data/unvalidated/block-groups.csv'
urlData = requests.get(url).content
evict_legacy = pd.read_csv(io.StringIO(urlData.decode('utf-8')))

################################################################################
# TRANSFORM ACS DATA
################################################################################

# Select columns to append
ACS = ACS_full[['GEO_ID', 'ACS_Count renter-occupied housing units', 'ACS_Median_gross_rent', 'ACS_CNT_RENTER_HOUSEHOLD INCOME_Median household income (dollars)']]

# Conform column names
ACS.rename(columns={'ACS_Count renter-occupied housing units':'ACS_Tot_Renter_Hu', 'ACS_Median_gross_rent':'ACS_MEDIAN_RENT', 
'ACS_CNT_RENTER_HOUSEHOLD INCOME_Median household income (dollars)':'ACS_MEDIAN_HH_INCOME'}, inplace=True)

################################################################################
# TRANSFORM MONTHLY EVICTION FILING DATA
################################################################################

timesplit = evict_mos['month'].str.split('/', expand = True)  # Split time variable
evict_mos['Time_Unit'] = 'Month'  # Create time unit indicator
evict_mos['Time_Unit_Value'] = timesplit[0].astype(int)  # Create time unit value
evict_mos['Year'] = timesplit[1].astype(int)  # Create year indicator
evict_mos['Date'] = evict_mos['Year'].astype(str) + '-' + evict_mos['Time_Unit_Value'].astype(str) + '-01'  # Create date proxy 
evict_mos.drop(columns = ['month'], inplace = True)  # Drop unused columns

################################################################################
# TRANSFORM WEEKLY EVICTION FILING DATA
################################################################################

timesplit = evict_wk['week_date'].str.split('-', expand = True)  # Split time variable
evict_wk['Time_Unit'] = 'Week'  # Create time unit indicator
evict_wk['Time_Unit_Value'] = evict_wk['week'].astype(int)  # Create time unit value
evict_wk['Year'] = timesplit[0].astype(int)  # Create year indicator
evict_wk['Date'] = evict_wk['week_date']  # Create date proxy for plots 
evict_wk.drop(columns = ['week', 'week_date'], inplace = True)  # Drop unused columns

################################################################################
# CREATE YEARLY EVICTION FILING DATA
################################################################################

evict_yr = evict_mos.groupby(['GEOID', 'Year', 'racial_majority'], as_index=False)['filings_2020'].sum()  # Calculate yearly filing totals
evict_yr['type'] = 'Census Tract'  # Create geography indicator
#evict_yr['filings_avg'] = ''
evict_yr['last_updated'] = '2023-11-05'
evict_yr['Time_Unit'] = 'Year'  # Create time unit indicator
evict_yr['Time_Unit_Value'] = evict_yr['Year']  # Create time unit value
evict_yr['Date'] = evict_yr['Year'].astype(str) + '-01-01'  # Create date proxy for plots

################################################################################
# TRANSFORM LEGACY EVICTION FILING DATA
################################################################################

# Filter for Milwaukee County filings only
evict_legacy = evict_legacy[evict_legacy['parent-location'] == 'Milwaukee County, Wisconsin']

# Select columns for data set
evict_legacy = evict_legacy[['GEOID', 'year', 'eviction-filings', 'eviction-filing-rate', 'renter-occupied-households', 'median-gross-rent', 'median-household-income', 'rent-burden']]

# Conform column names
evict_legacy.rename(columns={'year':'Year', 'eviction-filings':'filings_2020', 'eviction-filing-rate':'Filing_Rate_per_Hundred', 'renter-occupied-households':'ACS_Tot_Renter_Hu', 
'median-gross-rent':'ACS_MEDIAN_RENT', 'median-household-income':'ACS_MEDIAN_HH_INCOME', 'rent-burden':'Rent_to_HH_Income_Ratio'}, inplace=True)

evict_legacy['type'] = 'Census Tract'  # Create geography indicator
evict_legacy['last_updated'] = '2022-06-09'
evict_legacy['Time_Unit'] = 'Year'  # Create time unit indicator
evict_legacy['Time_Unit_Value'] = evict_legacy['Year']  # Create time unit value
evict_legacy['Date'] = evict_legacy['Year'].astype(str) + '-01-01'  # Create date proxy for plots
evict_legacy['Filing_Rate_per_Hundred'] = evict_legacy['Filing_Rate_per_Hundred'] / 100  # Conform filing rate
evict_legacy['Rent_to_HH_Income_Ratio'] = evict_legacy['Rent_to_HH_Income_Ratio'] / 100  # Conform RTI ratio

################################################################################
# COMBINE DATA
################################################################################

evict = pd.concat([evict_mos, evict_wk])
evict = pd.concat([evict, evict_yr])
evict = evict.merge(ACS, left_on=evict['GEOID'].astype(str), right_on=ACS['GEO_ID'].astype(str), how='left')
#evict = pd.concat([evict, evict_legacy]). # On hold

################################################################################
# AUGMENT DATA
################################################################################

# Calculate census tract filing count rank
evict['Filing_Count_Rank'] = evict.groupby(['Year','Time_Unit','Time_Unit_Value'])['filings_2020'].rank(ascending=False)

# Calculate census tract filing rate
evict['Filing_Rate_per_Hundred'] = np.where(evict['filings_2020'] > 0, evict['filings_2020'] / evict['ACS_Tot_Renter_Hu'] * 100, np.nan)

# Calculate rent to income (RTI) ratio
evict['Rent_to_HH_Income_Ratio'] = evict['ACS_MEDIAN_RENT'] / (evict['ACS_MEDIAN_HH_INCOME'] / 12)

# Categorize gross rent groups
evict['Rent_Group'] = np.where(evict['ACS_MEDIAN_RENT'] < 250, 'Less than $250',
                   np.where((evict['ACS_MEDIAN_RENT'] >= 250) & (evict['ACS_MEDIAN_RENT'] < 500), '$250 to $499',
                   np.where((evict['ACS_MEDIAN_RENT'] >= 500) & (evict['ACS_MEDIAN_RENT'] < 600), '$500 to $599',
                   np.where((evict['ACS_MEDIAN_RENT'] >= 600) & (evict['ACS_MEDIAN_RENT'] < 800), '$600 to $799',
                   np.where((evict['ACS_MEDIAN_RENT'] >= 800) & (evict['ACS_MEDIAN_RENT'] < 1250), '$800 to $1249',
                   np.where((evict['ACS_MEDIAN_RENT'] >= 1250) & (evict['ACS_MEDIAN_RENT'] < 2000), '$1250 to $1999',
                   np.where(evict['ACS_MEDIAN_RENT'] >= 2000, '$2000 or more', 
                   'NA')))))))

# Categorize rent burden groups                   
evict['Burden_Group'] = np.where(evict['Rent_to_HH_Income_Ratio'] < .3, 'Not rent burdened',
                     np.where((evict['Rent_to_HH_Income_Ratio'] >= .3) & (evict['Rent_to_HH_Income_Ratio'] < .5), 'Moderately rent burdened',
                     np.where(evict['Rent_to_HH_Income_Ratio'] >= .5, 'Severely rent burdened',
                     'NA')))

# Identify outliers via 3(IQR) method
def iqr_outlier_func(var):
  q1 = np.nanpercentile(var, 25)
  q3 = np.nanpercentile(var, 75)
  iqr = q3 - q1
  upper = q3 + 3.0 * iqr
  print(q1, q3, iqr, upper)
  return(upper)

year_upper = iqr_outlier_func(evict[evict['Time_Unit'] == 'Year']['Filing_Rate_per_Hundred'])
month_upper = iqr_outlier_func(evict[evict['Time_Unit'] == 'Month']['Filing_Rate_per_Hundred'])
week_upper = iqr_outlier_func(evict[evict['Time_Unit'] == 'Week']['Filing_Rate_per_Hundred'])

evict['Filing_Rate_Outlier'] = np.where((evict['Time_Unit'] == 'Year') & (evict['Filing_Rate_per_Hundred'] > year_upper), 1,
                                np.where((evict['Time_Unit'] == 'Month') & (evict['Filing_Rate_per_Hundred'] > month_upper), 1,
                                np.where((evict['Time_Unit'] == 'Week') & (evict['Filing_Rate_per_Hundred'] > week_upper), 1,
                                0)))

# Drop unusued columns                     
evict = evict.drop(['GEO_ID'], axis='columns')

################################################################################
# SAVE DATA AS CSV
################################################################################

evict.to_csv(filepath + 'MKE Eviction Data.csv')
