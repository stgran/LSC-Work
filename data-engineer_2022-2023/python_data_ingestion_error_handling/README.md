# Code Sample: Data Ingestion Error Handling
Language: Python (awswrangler, boto3, pandas, pytest)  
Other technologies: Sentry; AWS Lambda, Glue

## Description

### `code_sample.py`  

The code in `code_sample.py` compares the column names of CSV files in AWS S3 using two comparison functions. They both raise assertion errors if they identify inconsistent schema. We used Sentry, a monitoring platform. I set up a monitor to catch AssertionErrors raised when the Lambda function ran.  

The first comparison function is `compare_input_schema()`, which simply gets the column names of the oldest file in the S3 directory and compares them to the column names of the new file.  

The second comparison function is `compare_existing_schema()` , which gets the column names of the oldest file in the S3 directory and gets a list of the column names for all of the files in the directory. It then compares the column names of every file in the directory (included the comparison of the oldest file to itself, a redundancy I decided was not worth removing) to the column names of the oldest file.  

The comparison functions are supported by a helper function `get_oldest_schema()`, which identifies the CSV file in an S3 directory with the oldest last_modified date and returns its column names.  

## Corresponding Files  

### `supplemental.py`  

`supplemental.py` contains an AWS Lambda function which implemented the comparison functions in practice.  The Lambda function calls a function `check_schemas()` which runs both comparisons twice: first for the staging directory, second for the prod directory. After the staging comparisons but before the prod comparisons, `check_schemas()` replaces any spaces in the column names of the new file with underscores.  

### `supplemental_2.py`  

Before this solution, when bad data ended up in a Glue table, the culprit file would be removed from the prod directory but not always from the staging directory. As a result, although prod directories were clean, staging directories were quite messy.  

This meant that checking if existing files aligned with the schema in the staging directory would almost always fail, preventing even clean data from being added to the Glue table.  

`supplemental_2.py` contains a script I wrote to crawl over all directories in the S3 bucket, flag directories that failed checks, and identify the checks and files that failed. I ran this script and then cleaned up the flagged directories before we fully implemented our solution.  

### `code_sample_tests.py`  

`code_sample_tests.py` contains unit tests I wrote for `get_oldest_schema()`, `compare_input_schema()`, and `compare_existing_schema()` during development. They were implemented in pytest.  

## Context  

Our team works with other data, not data we scraped from court records websites, for a variety of reasons. Sometimes we want to be able to query this data via AWS Athena. To access the data on Athena, we upload the CSVs to an AWS S3 bucket specifically for this ad hoc data use.  

An AWS Lambda function is triggered by newly uploaded CSV files in the S3 bucket - it copies the new CSV files from the staging directory, where they are uploaded, to the production directory. Then the Lambda function adds the data in the CSVs to an AWS Glue table.  

Glue crawlers create tables from each subdirectory in S3, not from each CSV file. If there are multiple CSV files in a subdirectory, all their data is added to a single Glue table. But Glue is inflexible about the table schema. The columns of the Glue table are based on the first processed CSV file. Data from other CSV files in the subdirectory is added to the columns of the Glue table whether or not they align.  

Data analysts on our team frequently used this ad-hoc S3 bucket for research and analysis. Occasionally, they uploaded CSVs containing data that did not match the respective subdirectory's Glue table's columns, either by mistake or due to ignorance of the process. As a result, the Athena tables they wanted to use would end up with either missing data or data in the wrong columns, potentially ruining their analyses.  

## Decisions  

The main decision we made in this process was what should happen to any bad data uploaded to S3. Up until then, bad data (i.e., data files whose columns did not align with the existing schema of the AWS Glue table related to the S3 directory) moved from the staging directory to the prod directory and into the related AWS Glue table. This ignored the purpose of the staging-prod directory structure.  

I advocated that the first priority was that bad data not arrive in the Glue table since analysts had already inadvertently queried bad data via AWS Athena, only to have to redo their analyses. My boss agreed.  

We decided to rely on the Lambda function, not the analyst, to check that the new file was good to go. The earliest the Lambda function could check new files was once they arrived in the staging directory. I decided that we should do all our checks then, before it made it to the production directory or Glue table. If any of the four checks failed, the Lambda function would not move the new file out of the staging directory. This meant we would have to keep the staging directory as clean as the prod directory, but my boss and I decided that this was acceptable and encouraged tidiness.  

The second decision was how to flag bad data. We knew that bad data needed to be flagged promptly so analysts could correct their mistake before proceeding with their analysis using incomplete data.  

Up until then, any errors in our data pipeline on AWS had been sent to a dedicated Slack workspace. Detailed errors from multiple parts of the pipeline ended up in the same channel, which received so many messages that it was impractical to filter these time-sensitive error messages.  

We had already explored Sentry and decided that it was the right solution. It offered an immediate response to a data engineer. By tailoring our AssertionErrors, the Sentry monitor specified the culprit file and what checks had failed.  

It was up to the data engineer to reach out to the analyst and notify them that their upload had failed, and why. Because of the cleaning I did (see `supplemental_2.py`) before our solution was implemented, we could be fairly certain that the upload had failed because of the analyst's file (if we kept staging directories clean going forward, at least).  

We would have preferred an even more immediate solution, preferably an AWS solution that notified the analyst immediately after uploading a bad file, but we were satisfied with Sentry and did not have time to explore potentially ideal solutions.  