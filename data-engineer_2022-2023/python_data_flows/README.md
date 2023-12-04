# Code Sample: Weekly Data Flows
Language: Python (pandas)  
Other technologies: Prefect

## Description

The code in this sample prepares data for delivery to a partner via the following steps:  
1. Using the model_config information, the script identifies the relevant Athena table.  
2. The script queries the identified Athena table and saves the data file locally.  
3. The script runs the `remove_previous()` function to remove previously delivered data from the new file.  
    a. The function identifies the previously delivered data files.  
    b. The function reads all of the surrogate key IDs from the previous files into a dataframe and removes duplicate keys.  
    c. The function uses boolean masking to remove already delivered data from the new file.  
        - The function creates a boolean series (the mask) where a True means the surrogate key in the new data exists in the previously delivered data.  
        - The function keeps all the new data which had a False in the mask.  
    d. The function overwrites the file with this data.  
4. The script uploads the data file to the staging folder in our cloud storage.  

By adding the `@flow` decorator, I set the `run_flow()` function up as a Prefect Flow so we could schedule it for Monday mornings before work, easily see if it failed, and if necessary rerun the flow.  

Every Monday, I completed a manual inspection checklist for the new data file in the staging folder before moving it into the cloud storage folder shared with the partner.  

## Corresponding Files
flow_config.json: This file contained the Prefect Flow name.  

## Context
Our team received requests from two legal aid organizations for data regarding recent evictees in order to conduct their own outreach projects. The partners both wanted to conduct outreach on a weekly basis. The evictee data was time-sensitive so we sent it to the partners at the beginning of each week to provide them with the most recent, up-to-date data possible.  

Because our data delivery was periodic, we automated the data delivery process. We did so by setting up the delivery preparations as Prefect flows. The Prefect Flows were scheduled to run early on Monday morning so that I could review the prepared data before sharing it with our partners.  

One partner asked that we not send data more than once so that they would not conduct repeated outreach. I wrote a function to scan our previous deliveries and remove any data we had already sent.  

## Decisions  
We communicated with the partners to understand what data they needed and when they wanted it. Because of the nature of their outreach, they wanted as up-to-date data every week as possible.  

However, we had to decide how up-to-date our data would be.  
1. Our data pipeline was not set up to run immediately. Stages of the pipeline were set to run on different days of the week. As a result, the newest data available on a Monday would have been from Thursday.  
2. Data on court records websites is added gradually. Although the case type and parties may be included the day the case is added to the website, other details may not be added until later. Information about the progression of the case as it unfolds is added gradually as well. As a result, we rescraped cases at certain intervals. But the intervals were such that a case added the previous Tuesday would not have been rescraped by the time of the data delivery on Monday, even if more information had been added. This meant we might be excluding cases that could be useful to the partner because our data was out-of-date.  

Adjusted the timeline of the stages of the pipeline would take a significant amount of planning and coordination. It would be easy to adjust all stages but one, either breaking the pipeline or causing an important step to be skipped.  

On the other hand, we could simply adjust the rescraping schedule for the relevant data, meaning that every case we sent would be as up-to-date as our pipeline schedule allowed. This meant we were providing an up-to-date picture of the data available on the website as of Thursday.  

We presented this solution to the partners and they agreed that, based on the standard scheduling procedure for the relevant case type, the lag for cases filed on Fridays (and potentially prior days depending on how quickly data was added to the website) to the following week was acceptable.  

Unfortunately, for one location, cases were sometimes not posted immediately, but there was nothing we could do about that.  