# snowflake_covid-19_queries
Snowflake queries over a Covid-19 data set hosted and maintained by StarSchema.com

The file titled `Worst day.sql` builds a temporary table of [ cases | new_cases | deaths | new_deaths ], one row per combination of state and date. Then it uses known population statistics to calculate daily % increase in these metrics. This allows for computing which date has been the worst so far, for each state.

The file titled `County-level hotspots.sql` is quite large and serves two main purposes: (1) to calculate the US counties which have been the 'hottest' since a given date in the past, and (2) to combine hotspot info with demographic info to determine correlation of socioeconomic factors to hotspot-ness. There are also quite a lot of queries dedicated to calculating distances from county centers to Walmart and Trader Joe's stores. 

There is heavy use of temp tables in `County-level hotspots.sql`, as well as correlated subqueries, window functions, and a few Snowflake tricks like the `UNPIVOT` statement.
