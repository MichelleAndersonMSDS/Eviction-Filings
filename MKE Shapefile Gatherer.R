library(tidyverse)
library(tigris)
library(sf)

filepath = ""

mke_tracts_2019 <- tracts("WI", "Milwaukee", year='2019')
mke_tracts_2021 <- tracts("WI", "Milwaukee", year='2021')

All_MKE_tracts <- rbind(mke_tracts_2019, mke_tracts_2021)
All_MKE_tracts <- distinct(All_MKE_tracts)

plot(mke_tracts_2019$geometry)
plot(mke_tracts_2021$geometry)
plot(All_MKE_tracts$geometry)

write_sf(All_MKE_tracts, paste0(filepath, "All_MKE_tracts.shp"))


