'''
Fuzzy Matching of Legal Parties
This script is designed to remove duplicate parties from court data.  

Many times, the same party is included in court records multiple times 
with slightly different names due to a certain term being entered differently 
or abbreviated or simply due to a typo.  

When trying to aggregate court records by party, these slight variations in 
party name for the same party can make aggregation extremely difficult.  

This code uses a combination of text preprocessing, simple regular expression 
methods, and fuzzy matching to identify and group parties that the code believes to be the same, despite variations in the party name.
'''

import pandas as pd
from difflib import SequenceMatcher
import time
from fuzzywuzzy import fuzz

abbreviations = {'apartment': 'apt',
                 'apartments': 'apt',
                 'company': 'co',
                 'furniture': 'fur',
                 'credit': 'cr',
                 'management': 'mgt',
                 'financial': 'fin',
                 'services': 'svc',
                 'acceptance': 'acc',
                 'corporation': 'corp',
                 'property': 'prop',
                 'recovery': 'rec',
                 'holdings': 'hld',
                 'bonding': 'bnd',
                 'collection': 'col',
                 'rental': 'rtl',
                 'rentals': 'rtl',
                 'homes': 'hms',
                 'group': 'grp',
                 'service': 'svc',
                 'realty': 'rlt',
                 'bank': 'bk',
                 'properties': 'prop',
                 'capital': 'cap',
                 'place': 'pl',
                 'manor': 'mn',
                 'insurance': 'ins',
                 'acquisitions': 'acq',
                 'union': 'un'
                }

stopwords = ['llc', 'inc', 'pllc']

class RemoveRepetitiveNames():
    '''
    This class is designed to remove repetitive instances of the same party from court data.
    It does this by first preprocessing the party names, including removing punctuation and optionally numbers,
    making all names lowercase, and optionally removing stopwords and abbreviating common terms.
    Second, it matches identical duplicates.
    Third, it uses fuzzy matching to estimate if similar party names are the same party.
    Repeated parties are then combined into the same party.
    '''
    
    def __init__(self, filename, abbreviations=None, stopwords=None, size=10000, algorithm='seq', remove_numbers=True):
        '''
        Parameters:
        - filename: the filename and path of the data file.
        - abbreviations (default None): if the user wants common terms abbreviated, this should be a dict of those abbreviations.
        - stopwords (default None): if the user wants stopwords removed, this should be a list of those stopwords.
        - size (default 10000): only the first 10000 rows are operated on at the moment.
        - algorithm (default 'seq'): two algorithms for fuzzy matching are available:
            - Ratcliff/Obershelp ('seq'): Computes the doubled number of matching characters divided by the total number
                                          of characters in the two strings
            - Levenshtein ('levenshtein'): Computes the minimum number of edits needed to transform one string into the other
        - remove_numbers (default True): if True, removes the numbers from party_name.
        
        Steps:
        First, we import our data using the provided filename. We then select the first 10000 (based on 'size') rows.
        
        Next, we preprocess our data by adding a new column that is the same as the party_name column
        but with no punctuation or numbers and all lowercase.
        
        
        '''
        loaded_data = pd.read_csv(filename)
        self.abbreviations = abbreviations
        self.stopwords = stopwords
        self.algorithm = algorithm
        self.remove_numbers = remove_numbers
        
        # TIME CHECK MODULE
        start_time = time.time()
        print('timer started')
        
        self.data = loaded_data[:size]
        
        # remove punctuation and optionally numbers
        self.data = self.remove_punc_num(self.data)
        
        # make sure all whitespace is only one space
        self.data.party_name = self.data.party_name.replace('\s+', ' ', regex=True)
        
        # lowercase all party names
        self.data['party_name'] = self.data['party_name'].str.lower()
        
        # optionally remove stopwords like llc, inc, and pllc
        if self.stopwords:
            self.data['party_name'] = self.data['party_name'].apply(lambda x: self.remove_stopwords(x))
        
        # optionally abbreviate common long terms like apartments, company, corporation, etc.
        if self.abbreviations:
            self.data['party_name'] = self.data['party_name'].apply(lambda x: self.abbreviate(x))
        
        # combine and then drop duplicates
        self.data['party_count'] = self.data.groupby(['party_name'])['party_count'].transform('sum')
        self.data.drop_duplicates(subset=['party_name'], inplace=True)
        
        # record how many identical party_names were identified, combined, and removed
        new_size, _ = self.data.shape
        self.dropped_duplicates_count = size - new_size
        
        # TIME CHECK MODULE
        first_interval = time.time()
        print('first interval: ', first_interval - start_time)
        
        position = []
        
        for index, row in self.data.iterrows():
            word_length = len(row['party_name'])
            
            word_sum = self.sum_string(row['party_name'])
            
            word_position = word_sum + word_length*33
            # 33 is the scalar factor between the length and the sum of letters based on a simple regression of the shelby data
            position.append(word_position)
        
        self.data['position'] = position
        
        # TIME CHECK MODULE
        second_interval = time.time()
        print('second interval: ', second_interval - first_interval)
        
        party_names = []
        aliases = []
        party_types = []
        addresses = []
        case_types = []
        years = []
        party_counts = []
        
        removed = []
        
        # TIME CHECK MODULE
        print('for loop starting')
        count = 0
        interval = time.time()
        self.fuzzy_match_count = 0
        
        for index, row in self.data.iterrows():
            # TIME CHECK MODULE
            count += 1
            if count%1000 == 0:
                print('finished thousand rows: ', time.time() - interval)
                interval = time.time()
            if index not in removed:
                removed.append(index)
                
                position_lower = row['position'] * .8
                position_upper = row['position'] * 1.2
                test_set = self.data[(self.data['position']<=position_upper) & (self.data['position']>=position_lower)]
                party_names.append(row['party_name'])
                party_aliases = []
                party_party_types = [row['party_type']]
                party_addresses = [row['party_address']]
                party_case_types = [row['case_type']]
                party_years = [row['year']]
                party_party_counts = row['party_count']

                for index_2, row_2 in test_set.iterrows():
                    if index_2 not in removed:
                        if self.algorithm == 'seq':
                            score = self.seq(row['party_name'], row_2['party_name'])
                        elif self.algorithm == 'levenshtein':
                            score = self.levenshtein(row['party_name'], row_2['party_name'])/100
                        else:
                            score = self.seq(row['party_name'], row_2['party_name'])
                        if score >= .8:
                            self.fuzzy_match_count += 1
                            removed.append(index_2)
                            if row_2['party_name'] not in party_aliases:
                                party_aliases.append(row_2['party_name'])
                            if row_2['party_type'] not in party_party_types:
                                party_party_types.append(row_2['party_type'])
                            if row_2['party_address'] not in party_addresses:
                                party_addresses.append(row_2['party_address'])
                            if row_2['case_type'] not in party_case_types:
                                party_case_types.append(row_2['case_type'])
                            if row_2['year'] not in party_years:
                                party_years.append(row_2['year'])
                            party_party_counts += row_2['party_count']

                aliases.append(party_aliases)
                party_types.append(party_party_types)
                addresses.append(party_addresses)
                case_types.append(party_case_types)
                years.append(party_years)
                party_counts.append(party_party_counts)
        
        # TIME CHECK MODULE
        end_interval = time.time()
        print('finished main function: ', end_interval - start_time)
        print('number of matches: ', self.fuzzy_match_count)
        self.output_df = pd.DataFrame(list(zip(party_names, aliases, party_types, addresses, case_types, years, party_counts)), columns = ['party_name', 'aliases', 'party_types', 'addresses', 'case_types', 'years', 'party_count'])
    
    def remove_punc_num(self, df):
        '''
        This code was taken from https://stackoverflow.com/questions/50444346/fast-punctuation-removal-with-pandas
        It removes punctuation and optionally numbers.
        '''
        if self.remove_numbers == True: # numbers are included in the text to be removed.
            punct = '1234567890!"#$%&\'()*+,-./:;<=>?@[\\]^_`{}~'
        else: # only punctuation is included in the text to be removed.
            punct = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{}~'
        
        transtab = str.maketrans(dict.fromkeys(punct, ''))

        df['party_name'] = '|'.join(df['party_name'].tolist()).translate(transtab).split('|')
        
        return df
    
    def remove_stopwords(self, party_name):
        '''
        This method removes common stopwords such as llc, pllc, and inc.
        Removing these stopwords increases the number of duplicate party names and can also improve fuzzy matching.
        '''
        items = party_name.split(' ')
        
        new_party_name_list = []
        
        for item in items:
            if item not in stopwords:
                new_party_name_list.append(item)
        
        delimiter = ' '
        new_party_name = delimiter.join(new_party_name_list)
        
        return new_party_name
    
    def abbreviate(self, party_name):
        '''
        This method abbreviates common terms such as apartments, corporation, company, etc.
        By abbreviating these large terms, false positive are reduced because terms that might
        be shared by two unrelated parties carry less weight.
        '''
        items = party_name.split(' ')
        
        new_party_name_list = []
        
        for item in items:
            if item in self.abbreviations:
                new_party_name_list.append(self.abbreviations[item])
            else:
                new_party_name_list.append(item)
        
        delimiter = ' '
        new_party_name = delimiter.join(new_party_name_list)
        
        return new_party_name
        
    def sum_string(self, string):
        '''
        Converts letters to numbers (a=1, b=2, etc.) and returns the sum of all letters in a string.
        '''
        return sum(ord(c) - 64 for c in string)
    
    def seq(self, a, b):
        '''
        Returns similarity ratio for two party names using the Ratcliff/Obershelp algorithm.
        '''
        return SequenceMatcher(None, a, b).ratio()
    
    def levenshtein(self, a, b):
        '''
        Returns similarity ratio for two party names using the Levenshtein algorithm.
        '''
        return fuzz.ratio(a, b)


cleaned_data = RemoveRepetitiveNames(filename='Downloads/2020-11-01_tn_shelby_party_counts.csv', abbreviations=abbreviations, stopwords = stopwords, size=10000, algorithm='levenshtein', remove_numbers=True)