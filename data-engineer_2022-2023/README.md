# Code Samples
### Data Engineer at Legal Services Corporation
### July 2022 - August 2023

## My Work  
As a Data Engineer, I conducted work in Python, SQL, and R using AWS tools, DBT, Sentry, and Prefect. I collected samples of code I wrote across these languages for different projects to demonstrate my use of these languages for different tasks.  

This repository contains the following samples and explanations of their contexts. Each subfolder contains the code sample and a ReadMe explaining the context and decision-making that led to the code.  

<!--- - Deduplication macro in DBT (SQL, DBT, Jinja) --->
- Ad hoc data ingestion tests (Python; Sentry; Docker; AWS S3, Lambda, Glue)
- Weekly data flows (Python, Prefect)
<!--- - Data analysis (R, R Markdown) --->
<!--- - Web scrapers (Python, AWS SQS) --->

### Languagues / Technologies Used
I worked with Python and SQL nearly every day. Less frequently, but still on at least a dozen occasions, I used R for research and data visualization purposes.

Within Python, I primarily used the pandas, requests, pytest, boto3, and awswrangler libraries. Within R, I primarily used the dplyr and ggplot2 libraries, as well as R Markdown.  

I interacted with a variety of technologies at different frequencies. I have organized them based on frequency below:  
- Nearly daily: [DBT](https://www.getdbt.com/blog/what-exactly-is-dbt), AWS S3 and Athena, Bitbucket.  
- On multiple occasions: [Prefect](https://docs.prefect.io/2.14.3/cloud/), [Sentry](https://docs.sentry.io/platforms/python/), AWS Lambda and Glue.  
- At least once: Docker, AWS Cloudwatch and SQS.  

## Broad Context  
Our team specialized in scraping civil case data from court records websites for counties around the U.S. We primarily focused on eviction and debt case types (although we collected case data from whatever civil case types we could). We frequently used this data for our research but it was also useful to our partners, primarily legal aid organizations from around the U.S., for their own research and projects.  

I occasionally worked with other data on an ad hoc basis, particularly when conducting or assisting with specific research projects.  
  
Our data pipeline for civil case data consisted of the following steps:  
1. Data scraping:  
    - Conducted using Python scripts relying primarily on the requests library to send the appropriate get and post requests to the court data websites.  
    - The HTTP requests were processed as messages by AWS Simple Query Service. We monitored these SQS queues using CloudWatch.  
    - Scraped data arrives in an AWS S3 data lake as both HTML and JSON. Lambda functions are triggered by the arrival of new data to add the data to existing Glue tables.  
2. Data standardization: SQL scripts, compiled and run by DBT, process the data in the Glue tables through three main steps:  
    - Compression of JSON data into ORC data.  
    - Deduplication of data, removing duplicate cases.  
    - Structuring of data into a standardized format.  

    The standardization of the data allows us to query across different locations (and different sources) at once.  
3. Downsteam uses: The standardized data is now available for a variety of research, data delivery, and other downstream purposes.  
