# Address parsing
## Files
address_parsing.py  
cities_db.csv

## Description
The U.S. Census Geocoder's API requires addresses to be broken down into street address, city, state, and ZIP code before submitting.  
address_parsing.py contains a Python class that:
  - parses addresses into the required format for upload to the Census Geocoder API.
  - saves the parsed addresses to a CSV.
  - uploads this CSV to the Census Geocoder API.
  - saves the results from the Census Geocoder API.

cities_db.csv contains a table of, ideally, all possible municipalities that could appear in a U.S. mailing address. It is broken down by state for more efficient lookup.

address_parsing.py uses the city names in cities_db.csv to ensure that the city name is separated from the street name correctly.

## Input Data
address_parsing.py requires five input parameters.
  - A CSV containing a column with addresses.
  - A CSV containing city names, specifically the cities_db.csv file included in this directory.
  - The name of the first CSV's column containing the addresses to be parsed.
  - A destination filename for the parsed addresses.
  - A destination filename for the results from the Census Geocoder.

## Required Packages
  - Pandas
  - usaddress
  - re
  - censusgeocode
