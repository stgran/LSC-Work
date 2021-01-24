# Fuzzy Matching of Legal Parties
## Files
FuzzyMatching.py
FuzzyMatching.ipynb

Both files contain the same code, except that the .ipynb file contains tests at the end of the file.

## Description
This script groups duplicate parties from court data.  
Many times, the same party is included in court records multiple times with slightly different names due to a certain term being entered differently or abbreviated or simply due to a typo.  
When trying to aggregate court records by party, these slight variations in party name for the same party can make aggregation extremely difficult.  
This code uses a combination of text preprocessing, simple regular expression methods, and fuzzy matching to identify and group parties that the code believes to be the same, despite variations in the party name.

## Input Data
FuzzyMatching.py requires six input parameters.
  - filename: the filename of the CSV containing the parties to be matched.
  - abbreviations: a dictionary of abbreviations to be implemented in the party names. This dictionary is included in both files.
  - stopwords: a list of words that appear frequently in party names to be removed. This list is included in both files.
  - size: comparing every party name to each other can be extremely complex due to the factorial nature of combinations. The size parameter can limit the number of party names to be compared. It is set to 10,000 by default.
  - algorithm: there are two algorithms for finding similarity ratios included in this code: Levenshtein and Ratcliff/Obershelp. The algorithm parameter is set to 'levenshtein' by default.
  - remove_numbers: determines if numbers will be removed from party names. Set to True by default.

## Required Packages
  - Pandas
  - difflib
  - time
  - fuzzywuzzy
