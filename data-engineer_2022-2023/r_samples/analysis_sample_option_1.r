#################################
# 0. Set up Environment
#################################

suppressPackageStartupMessages({
  
  # Link necessary packages here
  # start up messages must be suppressed otherwise they will be printed to the document  
  library(ggplot2)
  library(dplyr)
  library(here)
  library(glue)
  library(tidyr)
})

# Data prep
county_data <- df %>% filter(county == 'county_name')
county_filed <- county_data %>% filter(year_filed >= 2017)
county_disposed <- county_data %>% filter(year_disposed >= 2017)


########################
# 2. memo
########################


#' # Representation  
#'   
#' ## Representation Rates
closed_cases <- county_disposed %>% filter(year_disposed <= 2022)
closed_count <- nrow(closed_cases)

landlord_represented_count <- nrow(closed_cases %>% filter(any_plaintiff_represented))
tenant_represented_count <- nrow(closed_cases %>% filter(any_defendant_represented))

plaintiff_represented_count <- nrow(closed_cases %>% filter(representation == 'Plaintiff represented'))
neither_represented_count <- nrow(closed_cases %>% filter(representation == 'Neither party represented'))
both_represented_count <- nrow(closed_cases %>% filter(representation == 'Both parties represented'))
defendant_represented_count <- nrow(closed_cases %>% filter(representation == 'Defendant represented'))

representation_df <- closed_cases %>%
  group_by(representation) %>%
  summarize(cases = n()) %>%
  mutate(rep_rate = glue("{round(100 * cases / closed_count, 1)}%")) %>%
  arrange(desc(cases))
colnames(representation_df) <- c("Representation Status", "# Cases", "% of All Cases Closed")
representation_df_t <- as.data.frame(t(representation_df))
names(representation_df_t) <- representation_df_t[1,]
representation_df_t <- representation_df_t[-1,]

#' The rates at which the tenants and landlords^[In the case of Unlawful Detainer (i.e., eviction) cases, the term "plaintiffs" is interchangeable with "landlords" and "defendants" with "tenants".] are represented in UD cases differ drastically. During 2017-2022, `r format(closed_count, big.mark=",", scientific=F)` unlawful detainer cases were closed^["Closed" cases are defined as those with (a) a hearing with a result of 'Default Judgment', 'Judgment', 'Dismissed No Funds', 'Released', or 'Payment Of Garnishment'; (b) a judgment of "Case Dismissed", "Non-suit", or any other non-blank entry ("Other"); OR (c) a documented appeal in the record.] in the County General District Court. Landlords had legal representation in  `r round(100 * landlord_represented_count / closed_count, 1)`% (`r format(landlord_represented_count, big.mark=",", scientific=F)`), while tenants were represented in only `r round(100 * tenant_represented_count / closed_count, 1)`% of cases (`r format(tenant_represented_count, big.mark=",", scientific=F)`).  
#'   
#' In the vast majority of UD cases closed in the county during 2017-2022 (`r round(100 * plaintiff_represented_count / closed_count, 1)`%), landlords had legal representation and tenants did not (Table 1). Both parties had legal representation in only `r round(100 * both_represented_count / closed_count, 1)`% of cases, while neither party had representation in `r round(100 * neither_represented_count / closed_count, 1)`% of cases. Tenants alone were represented in `r round(100 * defendant_represented_count / closed_count, 1)`% of all cases closed during 2017-2022.  
#'   
#'   \n\n
#'   
#' <div custom-style='Caption 1'>**Table 1:** Representation status of parties in unlawful detainer cases closed in the County General District Court during 2017-2022</div>
#'   
#'   \n\n
#'   
representation_df_t
#'   
#'   \n\n
#'   
#' Table 2, shown below, shows the representation rates^[If there were multiple plaintiffs or defendants in a single case, only one would have had to be represented for us to consider the plaintiff or defendant to have been represented in that case. This will remain true as we continue to discuss representation during this memo.] of landlords and tenants in UD cases filed from 2017 through 2022 which are now closed. In these cases, landlords were represented 92% of the time. Tenants were represented in just under 3% of the same cases.  
#'   
#'   \n\n
#'   
#' <div custom-style='Caption 1'>**Table 2:** Yearly UD Representation Rates</div>
#' 
rep_df_table <- county_disposed %>% filter(year_disposed <= 2022) %>%
  group_by(year_disposed) %>%
  summarize(
    dispositions = n(),
    landlords_represented = sum(any_plaintiff_represented),
    tenants_represented = sum(any_defendant_represented)
  ) %>%
  mutate(
    landlord_rep_rate = round(100 * landlords_represented / dispositions, 1),
    tenant_rep_rate = round(100 * tenants_represented / dispositions, 1)
  )

rep_bars <- rep_df_table %>% select(year_disposed, landlord_rep_rate, tenant_rep_rate) %>%
  gather(key = 'party_type', value = 'percent_represented', 2:3) %>%
  mutate(party_type = case_when(
    party_type == 'landlord_rep_rate' ~ 'Landlord',
    party_type == 'tenant_rep_rate' ~ 'Tenant',
    TRUE ~ party_type
  )) %>%
  ggplot(aes(x = year_disposed, y = percent_represented, fill = party_type)) +
  geom_bar(stat = 'identity', position = 'dodge') +
  geom_text(aes(label = glue("{round(percent_represented, 1)}%"))) +
  labs(title = 'Yearly Representation Rates by Party Type', subtitle = '2017 - 2022, Unlawful Detainer Cases', x = 'Year', y = 'Representation Rate (%)', fill = 'Party Type')

rep_bars
#'   
#'   \n\n
#'   
#' For the data underlying Table 2, please refer to Table 5 in the Appendix at the end of this memo.
#'   
#' It was during 2021, when the fewest UD cases were filed, that landlords were represented at their lowest rate from 2017 through 2022 and tenants at their highest. In these cases, landlords were represented in 86% of UD cases and tenants were represented in 7% of UD cases, a gulf of 79 percentage points.   
#'   
#'   \n\n
#'   
#' 
#' \newpage
#' ## Case Duration and Times to Writ  
#'   
#' Table 3, shown below, shows how the length of the case varies based on who is represented for UD cases closed during 2017-2022.  
#'   
#'   \n\n
#'   
#' <div custom-style='Caption 1'>**Table 3:** Case Durations by Representation Type</div>
#'   
#'   \n\n
#'   
duration_by_rep <- closed_cases %>%
  group_by(representation) %>%
  summarize(closed_cases = sum(!is.na(filing_to_judgment)),
            median_duration = median(filing_to_judgment, na.rm = TRUE),
            avg_duration = mean(filing_to_judgment, na.rm = TRUE)) %>%
  mutate(
    median_duration = case_when(
      !is.na(median_duration) ~ glue("{median_duration} days"),
      is.na(median_duration) ~ "n/a"),
    avg_duration = case_when(
      !is.na(avg_duration) ~ glue("{round(avg_duration, 0)} days"),
      is.na(avg_duration) ~ "n/a")
    ) %>%
  arrange(desc(closed_cases))
colnames(duration_by_rep) <- c("Representation", "Cases Closed", "Median Case Duration", "Average Case Duration")
duration_by_rep
#'   
#'   \n\n
#'   
#' Table 4, shown below, shows how the time between the closure of a case and the issuance of the writ of eviction varies based on who is represented for UD cases closed in during 2017-2022. Cases whose writs of eviction were issued prior to the closure of the case have been excluded because we cannot yet account for this discrepancy.  
#'   
#'   \n\n
#'   
#' <div custom-style='Caption 1'>**Table 4:** Days from Closure to Writ of Eviction by Representation Type</div>
#'   
#'   \n\n
#'   
cases_with_writs <- closed_cases %>% filter(days_disposition_to_writ >= 0)
time_to_writ_by_rep <- cases_with_writs %>%
  group_by(representation) %>%
  summarize(writs = n(),
            median_time_to_writ = median(days_disposition_to_writ, na.rm = TRUE),
            avg_time_to_writ = mean(days_disposition_to_writ, na.rm = TRUE)) %>%
  mutate(
    median_time_to_writ = case_when(
      !is.na(median_time_to_writ) ~ glue("{median_time_to_writ} days"),
      is.na(median_time_to_writ) ~ "n/a"),
    avg_time_to_writ = case_when(
      !is.na(avg_time_to_writ) ~ glue("{round(avg_time_to_writ, 0)} days"),
      is.na(avg_time_to_writ) ~ "n/a")
    ) %>%
  arrange(desc(writs))

time_to_writ_by_rep <- rbind(time_to_writ_by_rep, data.frame(representation = 'Defendant represented', writs = 0, median_time_to_writ = 'n/a', avg_time_to_writ = 'n/a'))
time_to_writ_by_rep <- rbind(time_to_writ_by_rep, data.frame(representation = 'Total', writs = nrow(cases_with_writs), median_time_to_writ = glue("{round(median(cases_with_writs$days_disposition_to_writ), 0)} days"), avg_time_to_writ = glue("{round(mean(cases_with_writs$days_disposition_to_writ), 0)} days")))

colnames(time_to_writ_by_rep) <- c("Representation", "Writs of Eviction Issued", "Median Time from Case Close to Writ", "Average Time from Case Close to Writ")
time_to_writ_by_rep
#'   
#'   \n\n
#'   
#' Table 5, shown below, breaks down the time between the closure of a case and the issuance of the writ of eviction by year. Again, cases whose writs of eviction were issued prior to the closure of the case have been excluded because we cannot yet account for this discrepancy.  
#'   
#' <div custom-style='Caption 1'>**Table 5:** Days from Closure to Writ of Eviction by Year</div>  
#'   
#'   \n\n
#'   
time_to_writ_by_year <- cases_with_writs %>%
  group_by(year_disposed) %>%
  summarize(writs = n(),
            median_time_to_writ = median(days_disposition_to_writ, na.rm = TRUE),
            avg_time_to_writ = mean(days_disposition_to_writ, na.rm = TRUE)) %>%
  mutate(
    median_time_to_writ = case_when(
      !is.na(median_time_to_writ) ~ glue("{median_time_to_writ} days"),
      is.na(median_time_to_writ) ~ "n/a"),
    avg_time_to_writ = case_when(
      !is.na(avg_time_to_writ) ~ glue("{round(avg_time_to_writ, 0)} days"),
      is.na(avg_time_to_writ) ~ "n/a")
  )
time_to_writ_by_year <- rbind(time_to_writ_by_year, data.frame(year_disposed = 'Total', writs = nrow(cases_with_writs), median_time_to_writ = glue("{round(median(cases_with_writs$days_disposition_to_writ), 0)} days"), avg_time_to_writ = glue("{round(mean(cases_with_writs$days_disposition_to_writ), 0)} days")))
colnames(time_to_writ_by_year) <- c("Year", "Writs of Eviction Issued", "Median Time from Case Close to Writ", "Average Time from Case Close to Writ")
time_to_writ_by_year
#'   
#'   \n\n
#'   

# The below dataframe is a combination of tables 4 and 5. It shows times from disposition to writ issuance broken down by year and representation status.
suppressMessages(
  time_to_writ_by_year_and_rep <- cases_with_writs %>%
    group_by(year_disposed, representation) %>%
    summarize(writs = n(),
              median_time_to_writ = median(days_disposition_to_writ, na.rm = TRUE),
              avg_time_to_writ = mean(days_disposition_to_writ, na.rm = TRUE)) %>%
    mutate(
      median_time_to_writ = case_when(
        !is.na(median_time_to_writ) ~ glue("{median_time_to_writ} days"),
        is.na(median_time_to_writ) ~ "n/a"),
      avg_time_to_writ = case_when(
        !is.na(avg_time_to_writ) ~ glue("{round(avg_time_to_writ, 0)} days"),
        is.na(avg_time_to_writ) ~ "n/a")
    ) %>%
    arrange(year_disposed, desc(writs))
)
colnames(time_to_writ_by_year_and_rep) <- c("Year", "Representation", "Writs of Eviction Issued", "Median Time from Case Close to Writ", "Average Time from Case Close to Writ")

# We save this table as a CSV instead of adding it to the document.
csvPath <- here('output/partner', toString(Sys.Date()))

# Check if outputFile directory exists, if not, create it
if(!dir.exists(csvPath)){
  dir.create(csvPath)
}

csvFilepath <- file.path(csvPath, paste0('time_to_writ_by_year_and_rep', '.csv'))
write.csv(time_to_writ_by_year_and_rep, csvFilepath, row.names = FALSE)


# Quartiles for Days to Writ by Year Disposed
suppressMessages(
  time_to_writ_yearly_quartiles <- cases_with_writs %>%
    group_by(year_disposed) %>%
    summarize_at(vars(days_disposition_to_writ), list('0%'=~quantile(., probs = 0, na.rm=TRUE), '25%'=~quantile(., probs = 0.25, na.rm=TRUE),
                                                      '50%'=~quantile(., probs = 0.5, na.rm=TRUE), '75%'=~quantile(., probs = 0.75, na.rm=TRUE),
                                                      '100%'=~quantile(., probs = 1, na.rm=TRUE)))
)
time_to_writ_yearly_quartiles_df <- data.frame(time_to_writ_yearly_quartiles, check.names = FALSE)

# We save this table as a CSV instead of adding it to the document.
csvFilepath <- file.path(csvPath, paste0('time_to_writ_yearly_quartiles', '.csv'))
write.csv(time_to_writ_yearly_quartiles_df, csvFilepath, row.names = FALSE)
#' 
#' \newpage

########################
# 3. appendix
########################
#' # Appendix  
#'   
#' Table 6, shown below, shows the number of UD cases closed in from 2017 through 2022, in how many of those cases landlords and tenants were represented, and the resulting representation rates for landlords and tenants in those cases.  
#'   
#' <div custom-style='Caption 1'>**Table 6:** Yearly Representation Rates for UD Cases Closed, 2017-2022</div>  
#'   
#'   \n\n
#'   
rep_df_table <- county_disposed %>% filter(year_disposed <= 2022) %>%
  group_by(year_disposed) %>%
  summarize(
    dispositions = n(),
    landlords_represented = sum(any_plaintiff_represented),
    tenants_represented = sum(any_defendant_represented)
  ) %>%
  mutate(
    landlord_rep_rate = glue("{round(100 * landlords_represented / dispositions, 1)}%"),
    tenant_rep_rate = glue("{round(100 * tenants_represented / dispositions, 1)}%")
  )

rep_df_table <- rbind(rep_df_table, data.frame('year_disposed' = 'Total', 'dispositions' = sum(rep_df_table$dispositions), 'landlords_represented' = sum(rep_df_table$landlords_represented), 'tenants_represented' = sum(rep_df_table$tenants_represented), 'landlord_rep_rate' = glue('{round(100 * sum(rep_df_table$landlords_represented) / sum(rep_df_table$dispositions), 1)}%'), 'tenant_rep_rate' = glue('{round(100 * sum(rep_df_table$tenants_represented)/ sum(rep_df_table$dispositions), 1)}%')))

colnames(rep_df_table) <- c("Year", "UD Cases Disposed", "Landlords Represented", "Tenants Represented", "Landlord Representation Rate", "Tenant Representation Rate")
rep_df_table
#'   
