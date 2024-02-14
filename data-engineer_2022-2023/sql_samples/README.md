# Code Sample: SQL queries  
Language: SQL  
Other technologies: DBT  

## Description  

### `code_sample.sql`  
This SQL script was used to build a DBT model which was accessible as AWS Athena tables.  

The script pulls data from three tables in our relational database. This data is joined on case_key, a unique identifier we used for each case. I have changed the names of the tables to keep the work and partner confidential.    
- party_information  
- standardized_data  
- unstandardized_data  

Other than selecting the requisite columns, the script takes a few data transformation steps:
- It converts array columns to text of lists.  
- It classifies each case as "commercial" if any defendant involved in the case has been classified as an "entity" (as opposed to a person). If not, the case is classified as "non-commercial".  
- It classifies each case as "uncertain" (with regards to the classification in the previous step) if the previous classification of any defendant involved in the case was assigned a certainty score below 0.9. If not, the case is classified as "certain", meaning all parties were confidently classified as entities or people.  
- It strips dollar signs and commas from the attorney fees and casts them as numbers.  

The outputted data has been filtered so that cases must meet the following criteria:
- Eviction casetype
- From the partner's state
- Disposed in 2022
- Attorney fees > 0
- All defendants were classified as people
- All defendant party classifications were certain

These criteria yield a data containing eviction cases which we are confident are residential from the partner's state disposed in 2022 with non-zero attorney fees.  

The outputted table contains the following columns:
- case_key
- All standardized columns (date, location, disposition, etc.)
- eviction_type
- eviction_type_certainty
- defendants
- party_labels
- party_label_scores
- attorney_fees


## Context  
A partner reached out to use for data we had scraped from a county's online court records system. They were interested in better understanding how much money people owe after being evicted. If a person is evicted for failing to pay rent, in addition to the backrent they owe, they also owe money for court fees and attorney fees. Attorney fees cover the cost of the winning party's attorney.  

However, sometimes the evicted party (defendant) never shows up to court. When this happens, the judge makes a "Default" ruling, and the tenant is evicted. Even in "Defaults", the evicted person must pay attorney fees. The partner was interested in the size of attorney fees in eviction cases that receive "Default" judgments.  

This model prepares a dataset containing residential eviction cases disposed in 2022 in the state in which they operate. We collect data from many court systems across the country, and data varies widely, but our ETL pipeline extracts and standardizes the data fields which we use the most. However, the detailed party information and attorney fees data we needed for this request was not part of our standardized data, so the model pulled data from across our standardized table, a specific party-focused table, and the original unstandardized data from the state in question.  

This model was upstream of one more model which selected only the columns needed by the partner.  

## Decisions  
We communicated with our partner to understand specifically which data they needed to conduct analyses. During our communications, we determined that the partner was interested exclusively in residential evictions, not in commercial evictions. Commercial evictions frequently involve large commercial spaces and the fees act as outliers when included with fees from residential evictions.  

On court records websites, all eviction cases share the same case type. We had to decide how to distinguish residential evictions from commercial evictions. 

We run analyses on the names of the parties involved in every case. One analysis classifies each defendant and each plaintiff of a case as an entity or a person based on the presence or absence of commercial terms (ltd, inc, etc.) in the party name. This classification is recorded in a `party_label` column. The match certainty is recorded in a `party_label_score` column.

This column was our best resource for distinguishing residential evictions from commercial evictions: if any defendant were a commercial entity, the eviction was probably commercial. But I had to decide what level of uncertainty we were comfortable with. Based on the fact that our partner was simply analyzing the data, not conducting an outreach program, I determined that as long as we could still provide sufficient data, it was preferable to avoid false positives (i.e. commercial evictions we had mislabeled as non-commercial) than to avoid false negatives. Because the attorney fees commercial eviction would likely act as an outlier compared to those of residential evictions, false positive could easily impact the accuracy of our partner's analysis.  

By observing `party_label` samples at different `party_label_score` levels, I determined that a 0.9 (out of 1) `party_label_score` threshold provided an extremely high level of certainty. Using this threshold, I set up the query to check two things:  
a. The `party_label` of all defendants involved in an eviction case must be non-commercial (i.e. != 'entity').  
b. The `party_label_score` for all these labels should be at least 0.9.  

In other words, if we were not nearly absolutely certain that every defendant involved in an eviction case was non-commercial, then we would not classify the eviction as residential and it would not be included in the dataset.  

I presented this decision to my boss and then the partner and we decided to move forward using this strategy.  