# Fuzzy Matching of Legal Parties
This script is designed to group duplicate parties from court data.  
Many times, the same party is included in court records multiple times with slightly different names due to a certain term being entered differently or abbreviated or simply due to a typo.  
When trying to aggregate court records by party, these slight variations in party name for the same party can make aggregation extremely difficult.  
This code uses a combination of text preprocessing, simple regular expression methods, and fuzzy matching to identify and group parties that the code believes to be the same, despite variations in the party name.

The .py and .ipynb files are virtually identical, in terms of functionality. The .ipynb file contains tests at the end of the file.
