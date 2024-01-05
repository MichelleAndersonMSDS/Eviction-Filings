# Milwaukee County Eviction Filings
### Description
This project includes code for the data pipeline that supports the [Milwaukee County eviction filings dashboard](https://public.tableau.com/views/MKERentBurdenandEvictions/Evictions).  The dashboard explores the eviction crisis in Milwaukee County using eviction filing data from the Eviction Tracking System created by The Eviction Lab.  The repository includes the following files:
1.  A python script [MKE Eviction Data.py] to gather, transform, and augment Milwaukee County eviction filing data from [The Eviction Lab](https://evictionlab.org). 
2.  An R script [ACS5 Data Gathering Script.R] to gather, transform, and augment housing-related [American Community Survey](https://www.census.gov/programs-surveys/acs/data.html) data using an US Census Bureau API. 
3.  An R script [MKE Shapefile Gatherer.R] to create a shapefile that includes current and historical geometries. 





