# Code Sample: Data Analysis and Visualization with R
Language: R (dplyr, ggplot2)  
Other technologies: R Markdown  

## Description

### `analysis_sample.r`
This file contains R code which creates an R Markdown file. The standard YAML header and stock setup code have been removed for brevity.  

R Markdown allows one to blend the text of their document with R code using both R and Markdown/HTML syntax. Lines that begin with `#'` are lines of text that will be shown in the output document. If you want to include the value of variable inline with text, you can do so using `` `r [r code]` `` in the line of text.  

The data this script works with was loaded from AWS Athena by a different script (not included). The data is essentially duplicated into two other datasets: one focused on the filing dates of cases and the other focused on the disposition dates of cases. This step is unnecessary, but is helpful since some analyses will focus on cases filed since 2017 others will focus on cases disposed since 2017.  

### `visualization_sample.r`
This file contains R code which creates and saves a stacked bar chart via ggplot2.  

Data was pulled directly from AWS Athena using a helper function.  

## Context
Both samples were part of a large analysis project performed for a partner. The partner reached out with over a dozen questions about eviction in their county. We communicated with them to understand their goals and then, based on that conversation and their original questions, prepared a report designed to explain eviction in their county as fully as our data would allow.  

We decided to prepare the report using R and R Markdown because it allowed us to dynamically update the numbers, tables, and visualizations within the report. If we redesigned a visualization or restructured a table, or even changed a figure in the text, we could do so without have to manually update the visualization, table, or number.  

The report consisted of four sections: Filings, Representation, Outcomes, and Financials. Each section contained written analysis, tables, and visualizations. The R code for the Representation section is contained in `analysis_sample.r`.  

During the preparation of the report, we updated our partner a couple times before sending the final report. After one update, they asked a question about representation and outcomes. `visualization_sample.r` contains the R code used to create a visualization that we sent separately from the report to help answer their question.  
