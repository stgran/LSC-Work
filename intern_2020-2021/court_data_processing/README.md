# Preparing Eviction Data for Visualization in Tableau
## Files
CourtData.py  
CourtData.ipynb  

Both files contain the same code.

## Description
This code prepares court data for visualization with Tableau.  
CourtData.py contains a class that reorganizes eviction case data scraped from court websites into chronologically-organized counts of evictions. This data is organized into yearly columns and broken down by either week or month. This code can organize data into three formats: weekly, monthly, and cumulative monthly.  
This class contains seven methods. Three of them are involved in data preprocessing. Three of them are callable to return different data outputs. The last one supports the callable methods.
#### Preprocessing
  - get_info(self, data, parameters): Extracts relevant information from overall court data.
    - data is the main dataset.
    - parameters is a list of strings that tell the function which column to parse and what to look for.
  - datetime_column(self, data)
  - year_columns(self, data)
    
#### Callable
  - get_monthly_counts(self, renter_households)
  - get_weekly_counts(self, start_date, end_date)
  - get_cumulative(self)
    
#### Support for callables
  - join_counts(self, old_table, new_table, column_name)

The CSVs outputted by this code are ready for direct upload to Tableau for visualization.

## Input Data
CourtData.py requires two parameters:
  - CSV containing the desired court data.
  - The state's abbreviation (e.g. 'SC').

## Libraries
  - Pandas
  - datetime
