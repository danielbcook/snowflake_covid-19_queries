/*
-- GRANT SELECT ON ALL TABLES IN SCHEMA "PUBLIC" TO ROLE PUBLIC;

-- total of US tests turning out positive
WITH latest_data as 
    (
        SELECT DATE, SUM(IFNULL(TOTAL,0)) as total_tests, sum(IFNULL(POSITIVE,0)) as cases, sum(IFNULL(DEATH,0)) as deaths
        FROM COVID.PUBLIC.CT_US_COVID_TESTS
        WHERE DATE = (SELECT TOP 1 date FROM COVID.PUBLIC.CT_US_COVID_TESTS ORDER BY 1 desc)
        GROUP BY DATE
    )
SELECT
    latest_data.date
  , latest_data.total_tests as total_tested
  , latest_data.cases / total_tested
FROM latest_data
WHERE 1=1
ORDER BY 1 desc
;
*/

DROP TABLE IF EXISTS GEO_DATA.PUBLIC.daily_percentages;
CREATE TABLE GEO_DATA.PUBLIC.daily_percentages as
(
  WITH state_date_matrix as
  (
        WITH all_dates as
        (
            SELECT DISTINCT date
            FROM COVID.PUBLIC.CT_US_COVID_TESTS ORDER BY 1 desc
        ),
        states as
        (
            SELECT DISTINCT PROVINCE_STATE as state, ISO3166_2 as state_code, POSITIVE as cases, IFNULL(death,0) as deaths
            FROM COVID.PUBLIC.CT_US_COVID_TESTS
        )
        SELECT DISTINCT all_dates.date, s.state, s.state_code
        FROM all_dates
        FULL OUTER JOIN states s
  ),
    state_population as
        (
            SELECT state as state_code, SUM(total_population) as total
            FROM COVID.PUBLIC.DEMOGRAPHICS
            GROUP BY state
        )
  SELECT sdm1.state_code as state
        , sdm1.date
        , IFNULL(sd1.POSITIVE,0) as cases
        , IFNULL(sd1.POSITIVE_SINCE_PREVIOUS_DAY,0) as new_cases
        , IFNULL(sd1.DEATH,0) as deaths
        , IFNULL(sd1.DEATH_SINCE_PREVIOUS_DAY,0) as new_deaths
        , state_population.total as total_population
        , new_cases + (3 * new_deaths) as arithmetic_daily_new_events
        , CASE 
            WHEN IFNULL(sd1.POSITIVE,0) = 0 THEN 0.0
            ELSE IFNULL(sd1.POSITIVE_SINCE_PREVIOUS_DAY,0) / sd1.POSITIVE
            END as daily_case_change_percent
        , CASE 
            WHEN IFNULL(sd1.DEATH,0) = 0 THEN 0.0
            ELSE IFNULL(sd1.DEATH_SINCE_PREVIOUS_DAY,0) / sd1.DEATH
            END as daily_death_change_percent
        , IFNULL(sd1.POSITIVE_SINCE_PREVIOUS_DAY,0)/state_population.total as relative_case_growth
        , IFNULL(sd1.DEATH_SINCE_PREVIOUS_DAY,0)/state_population.total as relative_death_growth
        , daily_case_change_percent + daily_death_change_percent as arithmetic_daily_change_percent
        , daily_case_change_percent * daily_death_change_percent as geometric_daily_change_percent
        , relative_case_growth + relative_death_growth as arithmetic_relative_change_percent
        , relative_case_growth * (relative_death_growth) as geometric_relative_change_percent
        FROM state_date_matrix sdm1
            INNER JOIN COVID.PUBLIC.CT_US_COVID_TESTS sd1 ON (sd1.date = sdm1.date AND sd1.ISO3166_2 = sdm1.state_code)
            INNER JOIN state_population ON state_population.state_code = sdm1.state_code
        ORDER BY relative_case_growth desc
);

WITH latest_data as 
    (
        SELECT DATE, ISO3166_2 as state, sum(IFNULL(POSITIVE,0)) as cases, sum(IFNULL(DEATH,0)) as deaths, SUM(IFNULL(TOTAL,0)) as total_tests
        FROM COVID.PUBLIC.CT_US_COVID_TESTS
        WHERE DATE = (SELECT TOP 1 date FROM COVID.PUBLIC.CT_US_COVID_TESTS ORDER BY 1 desc)
        GROUP BY DATE, ISO3166_2
    ),
state_testing as
    (
        SELECT ISO3166_2 as state_code, max(TOTAL) as tested
        FROM COVID.PUBLIC.CT_US_COVID_TESTS
        GROUP BY PROVINCE_STATE, ISO3166_2
    )
SELECT daily_percentages.*
  , (latest_data.cases + latest_data.deaths) / total_population as relative_combined_total
    -- correlated subquery, poor substitute for a true lateral join!
  ,(select max(arithmetic_relative_change_percent) FROM daily_percentages dp2 WHERE dp2.state = daily_percentages.state) as arithmetic_relative_change_percent_join
  , latest_data.cases / state_testing.tested as percent_positive_per_test
  , daily_percentages.state as state_code
  , total_population / state_testing.tested as residents_per_test
  , (select max(arithmetic_daily_new_events) FROM daily_percentages dp2 WHERE dp2.state = daily_percentages.state) as arithmetic_daily_new_events_join
FROM GEO_DATA.PUBLIC.DAILY_PERCENTAGES daily_percentages
    INNER JOIN state_testing ON state_testing.state_code = daily_percentages.state
    INNER JOIN latest_data ON latest_data.state = daily_percentages.state
WHERE 1=1
    AND daily_percentages.arithmetic_daily_new_events = arithmetic_daily_new_events_join
--    AND daily_percentages.arithmetic_relative_change_percent = arithmetic_relative_change_percent_join
--    AND daily_percentages.date = (SELECT TOP 1 date FROM COVID.PUBLIC.CT_US_COVID_TESTS ORDER BY 1 desc) ORDER BY arithmetic_daily_new_events desc -- to see only the states which are "moving worst day"
--    AND daily_percentages.state = 'GA'
ORDER BY
    daily_percentages.state
  , arithmetic_relative_change_percent desc -- 1st tiebreaker
  , arithmetic_daily_new_events desc
  , geometric_relative_change_percent desc -- 2nd tiebreaker
;

