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
ACS_full = pd.read_csv('/Users/michelleanderson/Documents/Eviction Data/MKE ACS Housing Data.csv')

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
evict_mos.drop(columns = ['month'], inplace = True)

################################################################################
# TRANSFORM WEEKLY EVICTION FILING DATA
################################################################################

timesplit = evict_wk['week_date'].str.split('-', expand = True)  # Split time variable
evict_wk['Time_Unit'] = 'Week'  # Create time unit indicator
evict_wk['Time_Unit_Value'] = evict_wk['week'].astype(int)  # Create time unit value
evict_wk['Year'] = timesplit[0].astype(int)  # Create year indicator
evict_wk['Date'] = evict_wk['week_date']  # Create date proxy 
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
evict_yr['Date'] = evict_yr['Year'].astype(str) + '-01-01'  # Create date proxy 

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

evict_legacy['type'] = 'Census Tract'
evict_legacy['last_updated'] = '2022-06-09'
evict_legacy['Time_Unit'] = 'Year'
evict_legacy['Time_Unit_Value'] = evict_legacy['Year']
evict_legacy['Date'] = evict_legacy['Year'].astype(str) + '-01-01'
evict_legacy['Filing_Rate_per_Hundred'] = evict_legacy['Filing_Rate_per_Hundred'] / 100
evict_legacy['Rent_to_HH_Income_Ratio'] = evict_legacy['Rent_to_HH_Income_Ratio'] / 100

################################################################################
# COMBINE DATA
################################################################################

evict = pd.concat([evict_mos, evict_wk])
evict = pd.concat([evict, evict_yr])
evict = evict.merge(ACS, left_on=evict['GEOID'].astype(str), right_on=ACS['GEO_ID'].astype(str), how='left')
#evict = pd.concat([evict, evict_legacy])

################################################################################
# AUGMENT DATA
################################################################################

evict['Filing_Rate_Rank'] = evict.groupby(['Year','Time_Unit','Time_Unit_Value'])['filings_2020'].rank(ascending=False)

evict['Filing_Rate_per_Hundred'] = evict['filings_2020'] / evict['ACS_Tot_Renter_Hu'] * 100

evict['Rent_to_HH_Income_Ratio'] = evict['ACS_MEDIAN_RENT'] / (evict['ACS_MEDIAN_HH_INCOME'] / 12)

evict['Rent_Group'] = np.where(evict['ACS_MEDIAN_RENT'] < 250, 'Less than $250',
                   np.where((evict['ACS_MEDIAN_RENT'] >= 250) & (evict['ACS_MEDIAN_RENT'] < 500), '$250 to $499',
                   np.where((evict['ACS_MEDIAN_RENT'] >= 500) & (evict['ACS_MEDIAN_RENT'] < 600), '$500 to $599',
                   np.where((evict['ACS_MEDIAN_RENT'] >= 600) & (evict['ACS_MEDIAN_RENT'] < 800), '$600 to $799',
                   np.where((evict['ACS_MEDIAN_RENT'] >= 800) & (evict['ACS_MEDIAN_RENT'] < 1250), '$800 to $1249',
                   np.where((evict['ACS_MEDIAN_RENT'] >= 1250) & (evict['ACS_MEDIAN_RENT'] < 2000), '$1250 to $1999',
                   np.where(evict['ACS_MEDIAN_RENT'] >= 2000, '$2000 or more', 
                   'NA')))))))
                   
evict['Burden_Group'] = np.where(evict['Rent_to_HH_Income_Ratio'] < .3, 'Not rent burdened',
                     np.where((evict['Rent_to_HH_Income_Ratio'] >= .3) & (evict['Rent_to_HH_Income_Ratio'] < .5), 'Moderately rent burdened',
                     np.where(evict['Rent_to_HH_Income_Ratio'] >= .5, 'Severely rent burdened',
                     'NA')))
                     
evict = evict.drop(['GEO_ID'], axis='columns')

################################################################################
# SAVE DATA AS CSV
################################################################################

evict.to_csv('/Users/michelleanderson/Documents/Eviction Data/MKE Eviction Data.csv')









