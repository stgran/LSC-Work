
#################################
# 0. Set up Environment
#################################

library(ggplot2)
library(dplyr)
library(here)
library(glue)

# Pull from Athena
source(here("R", "set_up_athena.R"))

df <-
  connect_db("dbt.partner_evictions", # database and table name in Athena
             "SELECT * FROM dbt.partner_evictions WHERE county = 'partner_county'") # SQL query as you would insert into athena


# Clean data
df['principal_plus_other'] <- 0

df <- df %>%
  mutate(principal_plus_other =
           case_when(
             !is.na(principal_amount) & !is.na(other_amount) ~ principal_amount + other_amount,
             !is.na(principal_amount) ~ principal_amount,
             !is.na(other_amount) ~ other_amount,
             TRUE ~ NA
           )
  ) %>%
  mutate(month_disposed = format(date_disposed, "%m"), year_disposed = format(date_disposed, "%Y"),
         month_filed = format(date_filed, "%m"), year_filed = format(date_filed, "%Y")) %>%
  mutate(representation = case_when(any_defendant_represented & any_plaintiff_represented ~ "Both parties represented",
                                    !any_defendant_represented & !any_plaintiff_represented ~ "Neither party represented",
                                    any_defendant_represented & !any_plaintiff_represented ~ "Defendant represented",
                                    !any_defendant_represented & any_plaintiff_represented ~ "Plaintiff represented")
  )

# Data prep
county_data <- df %>% filter(county == 'partner_county')
county_filed <- county_data %>% filter(year_filed >= 2016) %>%
  mutate(
    judgment = case_when(
      is.na(judgment) ~ 'appealed',
      judgment == 'not found/unserved' ~ 'unserved',
      judgment == 'transfer/change of venue' ~ 'transfer',
      TRUE ~ judgment
    )
  )


cases_w_both_rep <- county_filed %>% filter(year_filed <= 2022 & !is.na(date_disposed)) %>%
  filter(representation == 'Both parties represented')
cases_w_neither_rep <- county_filed %>% filter(year_filed <= 2022 & !is.na(date_disposed)) %>%
  filter(representation == 'Neither party represented')
cases_w_def_rep <- county_filed %>% filter(year_filed <= 2022 & !is.na(date_disposed)) %>%
  filter(representation == 'Defendant represented')
cases_w_pla_rep <- county_filed %>% filter(year_filed <= 2022 & !is.na(date_disposed)) %>%
  filter(representation == 'Plaintiff represented')

both_rep_judgment_count <- nrow(cases_w_both_rep)
neither_rep_judgment_count <- nrow(cases_w_neither_rep)
pla_rep_judgment_count <- nrow(cases_w_pla_rep)
def_rep_judgment_count <- nrow(cases_w_def_rep)


cases_w_both_rep <- cases_w_both_rep %>%
  group_by(judgment, representation) %>%
  summarize(count = n()) %>%
  mutate(
    rate = round(count / both_rep_judgment_count, 3),
    percents = round(100 * count / both_rep_judgment_count, 1)
  )

cases_w_neither_rep <- cases_w_neither_rep %>%
  group_by(judgment, representation) %>%
  summarize(count = n()) %>%
  mutate(
    rate = round(count / neither_rep_judgment_count, 3),
    percents = round(100 * count / neither_rep_judgment_count, 1)
  )

cases_w_def_rep <- cases_w_def_rep %>%
  group_by(judgment, representation) %>%
  summarize(count = n()) %>%
  mutate(
    rate = round(count / def_rep_judgment_count, 3),
    percents = round(100 * count / def_rep_judgment_count, 1)
  )

cases_w_pla_rep <- cases_w_pla_rep %>%
  group_by(judgment, representation) %>%
  summarize(count = n()) %>%
  mutate(
    rate = round(count / pla_rep_judgment_count, 3),
    percents = round(100 * count / pla_rep_judgment_count, 1)
  )

rep_judgments <- rbind(cases_w_both_rep, cases_w_neither_rep, cases_w_def_rep, cases_w_pla_rep)

color_values <- c("#acd8fd", "#b10000", "#860081", "#f28cee", "#de0000", "#36af00", "#004986", "#2a88d4", "#1b4607")

rep_judgments_bars <- rep_judgments %>% ggplot(aes(x = reorder(judgment, -rate), y = rate, fill = judgment)) +
  geom_bar(stat="identity") +
  scale_y_continuous(labels = scales::percent) +
  geom_text(aes(y = rate + 0.05, label = glue("{round(percents, 0)}%")), show.legend = FALSE) +
  scale_fill_manual(values = color_values) +
  theme_bw()+
  facet_wrap(~representation, ncol=1)+
  guides(fill="none") +
  labs(title = "Judgment Outcomes by Representation",
       subtitle = "UD Cases Filed, Since Disposed, 2016-2022",
       x = "Judgment",
       y = "Percent of Judgments (%)")

# rep_judgments_bars
ggsave(rep_judgments_bars, filename="output/stacked_bars.jpg", height = 10, width = 10)