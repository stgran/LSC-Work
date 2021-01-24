'''
Preparing Eviction Data for Visualization in Tableau
This code is designed to prepare court data for visualization with Tableau.

This code reorganizes eviction case data scraped from court websites into
chronologically-organized counts of evictions. This data is organized into
yearly columns and broken down by either week or month.

This code has functionality to prepare data into three formats: weekly, monthly,
and cumulative monthly.

The CSVs outputted by this code should be ready for direct upload to Tableau for
visualization.
'''

import pandas as pd
from datetime import date, timedelta

class CourtData():
    '''
    This class is designed to process court data from various states to obtain counts of specific case types over time.
    Right now, this class is designed to count eviction cases.
    
    The state must be specified so that the class knows how to process the court data, since court data formatting varies
    from state to state. Right now, this class can process court data from South Carolina and Delaware.
    
    In the future, functionality may be added for additional states.
    
    This class contains seven methods. Three of them are involved in data preprocessing. Three of them are callable
    to return different data outputs. The last one supports the callable methods.
    Preprocessing
    - get_info(self, data, parameters)
        Extracts relevant information from overall court data.
        - data is the main dataset.
        - parameters is a list of strings that tell the function which column to parse and what to look for.
    - datetime_column(self, data)
    - year_columns(self, data)
    
    Callable
    - get_monthly_counts(self, renter_households)
    - get_weekly_counts(self, start_date, end_date)
    - get_cumulative(self)
    
    Support for callables
    - join_counts(self, old_table, new_table, column_name)
    '''
    def __init__(self, filename, state, date_cutoff = None):
        self.data = pd.read_csv(filename)
        self.date_cutoff = date_cutoff
        
        # Get the years included in the dataset
        self.year_ints = list(self.data.year.unique()) # what years are we working with?
        if len(self.year_ints) > 1: # if we are working with multiple years, we will want to break those
            # years into their own columns at some point.
            self.year_ints = sorted(self.year_ints)
            self.year_cols = list(map(str, self.year_ints)) # these year strings will be column names later
        
        # This section dictates how the court data will be processed depending on the state.
        # Filter only the cases we want. The process here depends on how each state organizes its court data.
        # Delaware
        if state == 'DE':
            # Filter cases in New Castle County
            # These cases are identifiable by the Justice of the Peace number, which should be 13 or 9
            self.data = self.data[self.data.apply(lambda x: x['case_description'][10:14] == 'JP13' or x['case_description'][10:13] == 'JP9', axis=1)]
            # These parameters depend on how states organize and label their data
            parameters = ['case_id=', 'case_description', '61 - JP LANDLORD TENANT']
            self.eviction_data = self.get_info(self.data, parameters)
        # South Carolina
        elif state == 'SC':
            parameters = ['case_number=', 'case_information', 'Rule to Vacate']
            self.eviction_data = self.get_info(self.data, parameters)
        
        self.case_data = self.datetime_column(self.eviction_data)
        
        self.count_data = self.case_data['date'].value_counts().rename_axis('date').reset_index(name='counts')
        self.count_data = self.count_data.sort_values(by='date')
        
        if self.date_cutoff:
            self.count_data = self.count_data.loc[(self.count_data['date'] <= self.date_cutoff)]
        
        if len(self.year_ints) > 1:
            self.count_data = self.year_columns(self.count_data)
        
        sdate = date(2020,1,1)
        edate = date(2020,12,31)
        self.date_list = pd.date_range(sdate,edate-timedelta(days=1),freq='d')
        self.count_data['date'] = self.date_list
        self.count_data.set_index(['date'], inplace=True)
        
    def get_info(self, data, parameters):
        '''
        This method parses court data to identify eviction cases. It records Case IDs but as of 12/01/2020, these
        Case IDs are not used elsewhere in this class.
        This class could be expanded to record additional information or cut down to not even include Case IDs.
        '''
        # Select case types of interest
        case_ids = []
        years = []
        months = []
        days = []
        
        identifier = parameters[0]
        column_name = parameters[1]
        case_of_interest = parameters[2]
        delim = ','
        for index, row in data.iterrows():
            info = row[column_name]
            if case_of_interest in info:
                case_id = info[info.index(identifier) + len(identifier):]
                case_id = case_id.partition(delim)[0]
                case_ids.append(case_id)
                years.append(row['year'])
                months.append(row['month'])
                days.append(row['day'])
        
        column_names = ['case_id', 'year', 'month', 'day']
        eviction_data = pd.DataFrame(list(zip(case_ids, years, months, days)), columns=column_names)
        return eviction_data

    def datetime_column(self, data):
        '''
        This function combines the year, month, day columns into one date column.
        '''
        date_data = data[['year', 'month', 'day']]
        date_column = pd.to_datetime(date_data)
        data['date'] = date_column
        data.drop(['year', 'month', 'day'], axis=1, inplace=True)
        return data
    
    def year_columns(self, data):
        '''
        
        '''
        # Create month-day df
        sdate = date(2020,1,1)
        edate = date(2020,12,31)
        self.date_list = pd.date_range(sdate,edate-timedelta(days=1),freq='d')
        month_day_list = [date.strftime('%m-%d') for date in self.date_list]
        month_day_df = pd.DataFrame(month_day_list, columns=['date'])
        month_day_df = month_day_df.set_index('date')
        
        # Break data into year columns
        for year_name, year_number in zip(self.year_cols, self.year_ints):
            start_date = year_name + '-01-01'
            end_date = year_name + '-12-31'
            current_year = data.loc[(data['date'] >= start_date) & (data['date'] <= end_date)]
            month_day = []
            current_year['date'] = current_year['date'].apply(lambda x: x.strftime('%m-%d'))
            current_year = current_year.set_index('date')
            month_day_df = month_day_df.join(current_year, how='left', rsuffix='counts')
            month_day_df[year_name] = month_day_df['counts'].fillna(0)
            month_day_df = month_day_df.drop(['counts'], axis=1)
        avg_column = []
        for index, row in month_day_df.iterrows():
            avg = sum([row[year] for year in self.year_cols[:-1]])/(len(self.year_cols)-1)
            avg_column.append(avg)
        month_day_df['avg'] = avg_column
        return month_day_df
    
    def get_monthly_counts(self, renter_households):
        self.count_data.index = pd.DatetimeIndex(self.count_data.index)
        
        monthly_data = self.count_data.groupby(pd.Grouper(freq='M')).sum()
        
        if len(self.year_ints) > 1:
            for column, renter_household_count in zip(self.count_data.columns, renter_households):
                monthly_data[column] = monthly_data[column].div(renter_household_count)
                monthly_data[column] = monthly_data[column] * 10000
        else:
            monthly_data['counts'] = monthly_data['counts'].div(renter_households)
            monthly_data['counts'] = monthly_data['counts'] * 10000
        
        return monthly_data

    def get_weekly_counts(self, start_date, end_date):
        # done in Tableau
        # we just need the date range, which should be some multiple of 7 days
        case_data = self.count_data.loc[(self.count_data.index >= start_date) & (self.count_data.index <= end_date)]
        return case_data
    
    def join_counts(self, old_table, new_table, column_name):
        '''
        
        '''
        old_table = old_table.join(new_table, how='left')
        old_table = old_table.drop(['dates'], axis=1)
        old_table[column_name] = old_table['counts'].fillna(0)
        old_table = old_table.drop(['counts'], axis=1)
        return old_table
    
    def get_cumulative(self):
        if len(self.year_ints) > 1:
            cumulative_data = self.count_data.copy()
            columns = self.year_cols.copy() + ['avg']
            for year in columns:
                col_name = 'cumulative_' + year
                cumulative_data[col_name] = cumulative_data[year].cumsum()
            cumulative_data = cumulative_data.drop(columns, axis=1)
        else:
            cumulative_data = self.count_data
            cumulative_data['cumulative_counts'] = cumulative_data['count'].cumsum()
            cumulative_data = cumulative_data.drop('count')
        return cumulative_data

greenville = CourtData('Downloads/LSC Work/Data Visualizations/Greenville County/greenville_court_data.csv', 'SC')

greenville_monthly = greenville.get_monthly_counts(renter_households=[65891, 63234, 62260, 60220, 62001.9, 62901.25])

greenville_monthly.to_csv('Downloads/LSC Work/Data Visualizations/Most Recent Data/greenville_monthly.csv')

greenville_cumulative = greenville.get_cumulative()

greenville_cumulative.to_csv('Downloads/LSC Work/Data Visualizations/Most Recent Data/greenville_cumulative.csv')
