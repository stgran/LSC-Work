# LSC address parsing
The files in this folder are for parsing addresses.

address_parsing.py primarily uses the usaddress library to parse.

address_parsing.py then puts parsed addresses through the Census Geocoder API and saves the Geocoder's results.

cities.csv contains a table of, ideally, all possible municipalities that could appear in a U.S. mailing address. It is broken down by state for more efficient lookup.
