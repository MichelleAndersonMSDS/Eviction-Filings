import pandas as pd
import numpy as np
import requests
import io

ACS_full = pd.read_csv('/Users/michelleanderson/Documents/Eviction Data/MKE ACS Housing Data.csv')
ACS = ACS_full[['GEO_ID', 'ACS_Count renter-occupied housing units', 'ACS_Median_gross_rent', 'ACS_CNT_RENTER_HOUSEHOLD INCOME_Median household income (dollars)']]

ACS.rename(columns={'ACS_Count renter-occupied housing units':'ACS_Tot_Renter_Hu', 'ACS_Median_gross_rent':'ACS_MEDIAN_RENT', 
'ACS_CNT_RENTER_HOUSEHOLD INCOME_Median household income (dollars)':'ACS_MEDIAN_HH_INCOME'}, inplace=True)

url = 'https://evictionlab.org/uploads/milwaukee_monthly_2020_2021.csv'
urlData = requests.get(url).content
evict_mos = pd.read_csv(io.StringIO(urlData.decode('utf-8')))

url = 'https://evictionlab.org/uploads/milwaukee_weekly_2020_2021.csv'
urlData = requests.get(url).content
evict_wk = pd.read_csv(io.StringIO(urlData.decode('utf-8')))

timesplit = evict_mos['month'].str.split('/', expand = True)
evict_mos['Time_Unit'] = 'Month'
evict_mos['Time_Unit_Value'] = timesplit[0].astype(int)
evict_mos['Year'] = timesplit[1].astype(int)
evict_mos['Date'] = evict_mos['Year'].astype(str) + '-' + evict_mos['Time_Unit_Value'].astype(str) + '-01'
evict_mos.drop(columns = ['month'], inplace = True)

timesplit = evict_wk['week_date'].str.split('-', expand = True)
evict_wk['Time_Unit'] = 'Week'
evict_wk['Time_Unit_Value'] = evict_wk['week'].astype(int)
evict_wk['Year'] = timesplit[0].astype(int)
evict_wk['Date'] = evict_wk['week_date']
evict_wk.drop(columns = ['week', 'week_date'], inplace = True)

evict = pd.concat([evict_mos, evict_wk])

evict_yr = evict[evict['Time_Unit'] == 'Month'].groupby(['GEOID', 'Year', 'racial_majority'], as_index=False)['filings_2020'].sum()
evict_yr['type'] = 'Census Tract'
evict_yr['filings_avg'] = ''
evict_yr['last_updated'] = '2023-11-05'
evict_yr['Time_Unit'] = 'Year'
evict_yr['Time_Unit_Value'] = evict_yr['Year']
evict_yr['Date'] = evict_yr['Year'].astype(str) + '-01-01'

evict = pd.concat([evict, evict_yr])

evict = evict.merge(ACS, left_on=evict['GEOID'].astype(str), right_on=ACS['GEO_ID'].astype(str), how='left')

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
                     
evict = evict.drop('GEO_ID', axis='columns')

evict.to_csv('/Users/michelleanderson/Documents/Eviction Data/MKE Eviction Data.csv')









