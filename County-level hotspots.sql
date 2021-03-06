/* all distance calculations based on the haversine formula here: http://www.movable-type.co.uk/scripts/latlong.html
-- US County GEOJSON files obtained here: https://eric.clst.org/tech/usgeojson/ (oddly, missing #1 predicted hotspot Oglala Lakota SD)

-- go to line #15 to start rebuilding *all* temp tables
-- go to line #234 for just those tables needing a rebuild upon new COVID data (assumes temp tables prior to #234 haven't been dropped by Snowflake)

-- check latest dates from NYT_US_COVID19 table, build rest of temp tables if the date has moved forward
SELECT DISTINCT nyt.date
FROM COVID.PUBLIC.NYT_US_COVID19 nyt
ORDER BY 1 desc
LIMIT 5
;

--SELECT GET_DDL('table','ZIP_GEODATA_COMPLETE') -- to see the definition of a table, possibly mimic it
-------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.county_leans as
(
  SELECT
      cv.fips
    , PER_GOP_2016
    , PER_DEM_2016
    , PER_POINT_DIFF_2016
    , cv.state_abbr as state_code
    , cv.county_name as county
    , usc.population
    , usc.county_id
    , CASE 
      WHEN PER_GOP_2016 >= PER_DEM_2016 THEN true
      ELSE false
      END as GOP_COUNTY
    FROM GEO_DATA.PUBLIC.COUNTY_VOTING cv
        INNER JOIN GEO_DATA.PUBLIC.US_COUNTIES usc ON REPLACE(usc.GEO_ID,'0500000US','')  = cv.fips
)
;

CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.county_population_centers as
(
  WITH county_population_centers as
  (
    WITH modified_demo as
    (
      SELECT 
        state_code
        , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', ''),' Census Area', ''),' and Borough', ''),' Borough', ''),' Municipality', '') as county
        , population
        , county_id
        , GEO_ID
      FROM GEO_DATA.PUBLIC.US_COUNTIES
    )
    SELECT
      demo.state_code
      , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(demo.COUNTY, ' County', ''),' Parish', ''),' Census Area', ''),' and Borough', ''),' Borough', ''),' Municipality', '') as county
      , demo.population
      , demo.county_id
      , demo.GEO_ID
      , AVG(zgc.latitude) as latitude
      , AVG(zgc.longitude) as longitude
    FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE zgc
        INNER JOIN modified_demo demo ON zgc.county_id = demo.county_id
    GROUP BY 1,2,3,4,5
  )
  SELECT DISTINCT
        gdc.county_id
      , gdc.geo_id
      , gdc.county
      , gdc.state_code
      , gdc.population
      , gdc.latitude
      , gdc.longitude
  FROM county_population_centers gdc
);

-- unless Walmart location data or ZIP/County geolocation data is reloaded, this table doesn't need to be rebuilt
CREATE or REPLACE sequence GEO_DATA.PUBLIC.seq1;
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.walmart_county_distance_matrix as
(
  SELECT 
    cpc.state_code
    , cpc.county
    , cpc.latitude
    , cpc.longitude
    , wfg.ZIP as walmart_zip
    , 7922 * atan2(sqrt(SQUARE(sin(((cpc.latitude - wfg.latitude) * pi()/180)/2)) + cos(wfg.latitude * pi()/180) * cos(cpc.latitude * pi()/180) *  SQUARE(sin(((cpc.longitude - wfg.longitude) * pi()/180)/2))), sqrt(1-SQUARE(sin(((cpc.latitude - wfg.latitude) * pi()/180)/2)) + cos(wfg.latitude * pi()/180) * cos(cpc.latitude * pi()/180) *  SQUARE(sin(((cpc.longitude - wfg.longitude) * pi()/180)/2)))) as distance
    , cpc.county_id
    , cpc.geo_id
    , s.nextval as walmart_seq
  FROM GEO_DATA.PUBLIC.walmart_geocodes wfg, table(getnextval(GEO_DATA.PUBLIC.seq1)) s
    FULL OUTER JOIN GEO_DATA.PUBLIC.county_population_centers cpc
  WHERE distance <= 150 -- there is no county in the lower 48 more than 150 miles from a Walmart!
)
;

CREATE or REPLACE sequence GEO_DATA.PUBLIC.seq1;
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.tjs_county_distance_matrix as
(
  SELECT 
    cpc.state_code
    , cpc.county
    , cpc.latitude
    , cpc.longitude
    , wfg.ZIP as tjs_zip
    , 7922 * atan2(sqrt(SQUARE(sin(((cpc.latitude - wfg.latitude) * pi()/180)/2)) + cos(wfg.latitude * pi()/180) * cos(cpc.latitude * pi()/180) *  SQUARE(sin(((cpc.longitude - wfg.longitude) * pi()/180)/2))), sqrt(1-SQUARE(sin(((cpc.latitude - wfg.latitude) * pi()/180)/2)) + cos(wfg.latitude * pi()/180) * cos(cpc.latitude * pi()/180) *  SQUARE(sin(((cpc.longitude - wfg.longitude) * pi()/180)/2)))) as distance
    , cpc.county_id
    , cpc.geo_id
    , s.nextval as tjs_seq
  FROM GEO_DATA.PUBLIC.tjs_geocodes wfg, table(getnextval(GEO_DATA.PUBLIC.seq1)) s
    FULL OUTER JOIN GEO_DATA.PUBLIC.county_population_centers cpc
  WHERE distance <= 600 -- there is no county in the lower 48 more than 150 miles from a tjs!
)
;

-- unless Walmart location data or ZIP/County geolocation data is reloaded, this table doesn't need to be rebuilt
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.walmart_geodata_matrix as
(
  SELECT
    demo.state_code
    , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', ''),' Census Area', ''),' and Borough', ''),' Borough', ''),' Municipality', '') as county
    , demo.county_id
    , demo.geo_id
    , demo.latitude
    , demo.longitude
    , 7922 * atan2(sqrt(SQUARE(sin(((walmart.latitude - demo.latitude) * pi()/180)/2)) + cos(demo.latitude * pi()/180) * cos(walmart.latitude * pi()/180) *  SQUARE(sin(((walmart.longitude - demo.longitude) * pi()/180)/2))), sqrt(1-SQUARE(sin(((walmart.latitude - demo.latitude) * pi()/180)/2)) + cos(demo.latitude * pi()/180) * cos(walmart.latitude * pi()/180) *  SQUARE(sin(((walmart.longitude - demo.longitude) * pi()/180)/2)))) as distance
  FROM GEO_DATA.PUBLIC.walmart_geocodes walmart
      FULL OUTER JOIN GEO_DATA.PUBLIC.COUNTY_POPULATION_CENTERS demo
  WHERE distance <= 600 -- there is no location in the lower 48 more than about 600 miles from a walmart
);

-- unless TJs location data or ZIP geolocation data is reloaded, this table doesn't need to be rebuilt
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.tjs_geodata_matrix as
(
  SELECT
    demo.state_code
    , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', ''),' Census Area', ''),' and Borough', ''),' Borough', ''),' Municipality', '') as county
    , demo.county_id
    , demo.geo_id
    , demo.latitude
    , demo.longitude
    , 7922 * atan2(sqrt(SQUARE(sin(((tjg.latitude - demo.latitude) * pi()/180)/2)) + cos(demo.latitude * pi()/180) * cos(tjg.latitude * pi()/180) *  SQUARE(sin(((tjg.longitude - demo.longitude) * pi()/180)/2))), sqrt(1-SQUARE(sin(((tjg.latitude - demo.latitude) * pi()/180)/2)) + cos(demo.latitude * pi()/180) * cos(tjg.latitude * pi()/180) *  SQUARE(sin(((tjg.longitude - demo.longitude) * pi()/180)/2)))) as distance
  FROM GEO_DATA.PUBLIC.tjs_geocodes tjg
      FULL OUTER JOIN GEO_DATA.PUBLIC.COUNTY_POPULATION_CENTERS demo
  WHERE distance <= 600 -- there is no county in the lower 48 more than 600 miles from a tjs!
);

-- SELECT COUNT(*) FROM GEO_DATA.PUBLIC.WALMART_GEODATA_MATRIX
-- 4598731 rows with <= 150 criteria
-- SELECT COUNT(*) FROM GEO_DATA.PUBLIC.tjs_geodata_matrix
-- 296393 rows with <= 600 criteria (this number got about 10% smaller, need to investigate)

CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.COUNTY_DISTANCES (
	STATE_CODE VARCHAR(2),
	COUNTY VARCHAR(16777216),
	COUNTY_ID NUMBER(18,0),
	GEO_ID VARCHAR(20),
	LATITUDE FLOAT,
	LONGITUDE FLOAT,
	MIN_DISTANCE_WALMART FLOAT,
	MIN_DISTANCE_TJS FLOAT,
	WALMART_DENSITY NUMBER(18,0),
	TJS_DENSITY NUMBER(18,0)
);

INSERT INTO GEO_DATA.PUBLIC.COUNTY_DISTANCES (STATE_CODE, COUNTY, COUNTY_ID, GEO_ID, LATITUDE, LONGITUDE)
SELECT DISTINCT
  STATE_CODE
  , COUNTY
  , COUNTY_ID
  , GEO_ID
  , LATITUDE
  , LONGITUDE
FROM GEO_DATA.PUBLIC.walmart_county_distance_matrix
;

UPDATE GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
SET cd.MIN_DISTANCE_WALMART = wcdm.min_distance_walmart
FROM
  (
    SELECT DISTINCT 
      wgm.state_code
      , wgm.county
      , wgm.county_id
      , (select min(distance) FROM GEO_DATA.PUBLIC.walmart_county_distance_matrix wg2 WHERE wg2.county_id = wgm.county_id) as min_distance_walmart
    FROM GEO_DATA.PUBLIC.walmart_county_distance_matrix wgm
    WHERE wgm.distance = min_distance_walmart
  ) wcdm
WHERE wcdm.county_id = cd.county_id
;

UPDATE GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
SET cd.MIN_DISTANCE_TJS = tcdm.min_distance_tjs
FROM
  (
    SELECT DISTINCT 
      wgm.state_code
      , wgm.county
      , wgm.county_id
      , (select min(distance) FROM GEO_DATA.PUBLIC.tjs_county_distance_matrix wg2 WHERE wg2.county_id = wgm.county_id) as min_distance_tjs
    FROM GEO_DATA.PUBLIC.tjs_county_distance_matrix wgm
    WHERE wgm.distance = min_distance_tjs
  ) tcdm
WHERE tcdm.county_id = cd.county_id
;

UPDATE GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
SET TJS_DENSITY = tjd.tjs_density
FROM
  (
  SELECT cd.county_id, COUNT(DISTINCT tcdm.tjs_seq) as tjs_density
  FROM GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
    INNER JOIN GEO_DATA.PUBLIC.tjs_county_distance_matrix tcdm ON tcdm.county_id = cd.county_id
  WHERE tcdm.distance <= cd.MIN_DISTANCE_WALMART
  GROUP BY 1
  ) tjd
WHERE tjd.county_id = cd.county_id
;

UPDATE GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
SET WALMART_DENSITY = wmd.walmart_density
FROM
  (
  SELECT cd.county_id, COUNT(DISTINCT wcdm.walmart_seq) as walmart_density
  FROM GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
    INNER JOIN GEO_DATA.PUBLIC.walmart_county_distance_matrix wcdm ON wcdm.county_id = cd.county_id
  WHERE wcdm.distance <= cd.MIN_DISTANCE_TJS
  GROUP BY 1 ORDER BY 2 desc
  ) wmd
WHERE wmd.county_id = cd.county_id
;

UPDATE GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
SET TJS_DENSITY = 0
WHERE TJS_DENSITY IS NULL 
    AND MIN_DISTANCE_TJS IS NOT NULL
    AND MIN_DISTANCE_WALMART IS NOT NULL
    AND MIN_DISTANCE_TJS >= MIN_DISTANCE_WALMART
;
UPDATE GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
SET WALMART_DENSITY = 0
WHERE WALMART_DENSITY IS NULL 
    AND MIN_DISTANCE_TJS IS NOT NULL
    AND MIN_DISTANCE_WALMART IS NOT NULL
    AND MIN_DISTANCE_WALMART >= MIN_DISTANCE_TJS
;
UPDATE GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
SET WALMART_DENSITY = 0 WHERE MIN_DISTANCE_WALMART IS NULL
;
UPDATE GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
SET TJS_DENSITY = 0 WHERE MIN_DISTANCE_TJS IS NULL
;
-- edge case: no TJs within 600 miles, consider the Walmart density = 1
UPDATE GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
SET WALMART_DENSITY = 1 WHERE MIN_DISTANCE_TJS IS NULL AND MIN_DISTANCE_WALMART > 0
;

-------------------------------------------------------------------------------------
-- beginning of tables which should be rebuilt when new COVID data has been loaded --
-- inexact matching on county names from NYT tables
-------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.county_hotspots as
 (
    WITH daily_percentages as
    (
      WITH county_date_matrix as
      (
            WITH all_dates as
            (
                SELECT DISTINCT date
                FROM COVID.PUBLIC.NYT_US_COVID19
            ),
            counties as
            (
              SELECT DISTINCT
                county_id
                , CASE 
                  WHEN GEO_ID IN ('0500000US36005','0500000US36047','0500000US36061','0500000US36081','0500000US36085') THEN 'New York City' -- collapse all 5 counties to NYC, because NYT data demands it
                  ELSE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', ''),' Census Area', ''),' and Borough', ''),' Borough', ''),' Municipality', '')
                  END as county
                , state_code
                , population
              FROM GEO_DATA.PUBLIC.county_population_centers
            )
            SELECT DISTINCT all_dates.date, state_code, county, county_id, population
            FROM all_dates
            	FULL OUTER JOIN counties
      )
      SELECT
        cdm1.state_code
        , cdm1.county_id
        , cdm1.county
        , cdm1.date
        , CASE
          WHEN cdm1.county_id IN (1852,1859,1869,1831,1871) THEN IFNULL(nyt1.CASES,0)/5
          ELSE IFNULL(nyt1.CASES,0) 
          END as cases
        , CASE
          WHEN cdm1.county_id IN (1852,1859,1869,1831,1871) THEN IFNULL(nyt1.CASES_SINCE_PREV_DAY,0)/5
          ELSE IFNULL(nyt1.CASES_SINCE_PREV_DAY,0)
          END as new_cases
        , CASE
          WHEN cdm1.county_id IN (1852,1859,1869,1831,1871) THEN IFNULL(nyt1.DEATHS,0)/5
          ELSE IFNULL(nyt1.DEATHS,0)
          END as deaths
        , CASE
          WHEN cdm1.county_id IN (1852,1859,1869,1831,1871) THEN IFNULL(nyt1.DEATHS_SINCE_PREV_DAY,0)/5
          ELSE IFNULL(nyt1.DEATHS_SINCE_PREV_DAY,0)
          END as new_deaths
        , cdm1.population as total_population
        , new_cases + new_deaths as arithmetic_daily_new_events
        , CASE 
            WHEN cases = 0 THEN 0.0
            ELSE new_cases / cases
            END as daily_case_change_percent
        , CASE 
            WHEN deaths = 0 THEN 0.0
            ELSE new_deaths / deaths
            END as daily_death_change_percent
        , new_cases/cdm1.population as relative_case_growth
        , new_deaths/cdm1.population as relative_death_growth
        , daily_case_change_percent + daily_death_change_percent as arithmetic_daily_change_percent
        , daily_case_change_percent * daily_death_change_percent as geometric_daily_change_percent
        , relative_case_growth + relative_death_growth as arithmetic_relative_change_percent
        , relative_case_growth * (relative_death_growth) as geometric_relative_change_percent
        FROM county_date_matrix cdm1
            LEFT OUTER JOIN COVID.PUBLIC.NYT_US_COVID19 nyt1 ON (nyt1.date = cdm1.date AND nyt1.ISO3166_2 = cdm1.state_code AND REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(nyt1.COUNTY, ' County', ''),' Parish', ''),' Census Area', ''),' and Borough', ''),' Borough', ''),' Municipality', '') = cdm1.county)
    )
		SELECT DISTINCT 
			daily_percentages.state_code
			, daily_percentages.county_id
			, daily_percentages.county
			, daily_percentages.date
			, daily_percentages.cases
			, daily_percentages.new_cases
			, daily_percentages.deaths
			, daily_percentages.new_deaths
			, daily_percentages.total_population
			, daily_percentages.arithmetic_relative_change_percent
			, daily_percentages.geometric_relative_change_percent
			, daily_percentages.arithmetic_daily_new_events
		FROM daily_percentages
);

-- count the GOP vs. Democratic cases and deaths
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.red_vs_blue_counties as
(
  SELECT  
      date
    , GOP_COUNTY
    , CASE
      WHEN county_hotspots.state_code IN ('AL','AK','AZ','AR','FL','GA','ID','IN','IA','KS','KY','LA','MI','MS','MT','NE','MO','OK','PA','TN','TX','UT','WV','WY','WI','NC','ND','SD','OH','SC') THEN true
      WHEN county_hotspots.state_code IN ('CA','CO','CT','DC','DE','HI','IL','ME','MA','MN','NH','NM','NY','WA','MD','NV','NJ','RI','OR','VA','VT') THEN false
      END as GOP_state
    , SUM(new_cases) as cases
    , SUM(new_deaths) as deaths
  FROM GEO_DATA.PUBLIC.county_hotspots
      INNER JOIN GEO_DATA.PUBLIC.county_leans cl ON cl.county_id = county_hotspots.county_id
  WHERE date >= '2020-01-26'
--    AND date = '2020-04-06'
  GROUP BY 1,2,3
  ORDER BY cases desc
);

-- find the TJs closest to a county hotspot
-- correlated subquery runs *a lot* faster than a Window function to determine distance
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.tjs_hotspot_proximity as
SELECT DISTINCT 
  county_hotspots.state_code
  , county_hotspots.county
  , tjm.county_id
  , (select min(distance) FROM GEO_DATA.PUBLIC.tjs_geodata_matrix tj2 WHERE tj2.county_id = tjm.county_id) as min_distance_tjs
FROM GEO_DATA.PUBLIC.county_hotspots
    INNER JOIN GEO_DATA.PUBLIC.tjs_geodata_matrix tjm ON (tjm.county_id = county_hotspots.county_id)
WHERE tjm.distance = min_distance_tjs
ORDER BY min_distance_tjs desc
;

-- find the Walmart closest to a county hotspot
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.walmart_hotspot_proximity as
SELECT DISTINCT 
  county_hotspots.state_code
  , county_hotspots.county
  , wgm.county_id
  , (select min(distance) FROM GEO_DATA.PUBLIC.walmart_geodata_matrix wg2 WHERE wg2.county_id = wgm.county_id) as min_distance_walmart
FROM GEO_DATA.PUBLIC.county_hotspots
    INNER JOIN GEO_DATA.PUBLIC.walmart_geodata_matrix wgm ON (wgm.county_id = county_hotspots.county_id)
WHERE wgm.distance = min_distance_walmart
ORDER BY min_distance_walmart desc
;

-------------------------------
-- calculate recent hotspots --
-------------------------------
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.recent_hotspots as
SELECT DISTINCT
  county_hotspots.state_code
  , county_hotspots.county
  , county_hotspots.county_id
  , county_hotspots.ARITHMETIC_RELATIVE_CHANGE_PERCENT
  , county_hotspots.Date
  , county_hotspots.cases
  , county_hotspots.new_cases
  , county_hotspots.deaths
  , county_hotspots.new_deaths
  , county_hotspots.total_population
  , county_hotspots.ARITHMETIC_DAILY_NEW_EVENTS
  , thp.min_distance_tjs
  , whp.min_distance_walmart
  , CASE 
    WHEN thp.min_distance_tjs <= whp.min_distance_walmart THEN 1
    ELSE 0
    END as which_is_closer
FROM GEO_DATA.PUBLIC.county_hotspots
    LEFT OUTER JOIN GEO_DATA.PUBLIC.tjs_hotspot_proximity thp ON thp.county_id = county_hotspots.county_id
    LEFT OUTER JOIN GEO_DATA.PUBLIC.walmart_hotspot_proximity whp ON whp.county_id = county_hotspots.county_id
ORDER BY
	  arithmetic_relative_change_percent desc -- 1st tiebreaker
	, county_hotspots.state_code
  , county_hotspots.county
;

-------------------------------------------------------------------------------
-- end of tables which should be rebuilt when new COVID data has been loaded --
-------------------------------------------------------------------------------
*/

-- which date has the most "weight" among hotspot calculations?
SELECT DATE, SUM(arithmetic_relative_change_percent)
FROM GEO_DATA.PUBLIC.recent_hotspots
GROUP BY 1 ORDER BY 2 desc
;

-- which state has the most "weight" among hotspot calculations?
-- remember to rebuild DAILY_PERCENTAGES
SELECT state, SUM(arithmetic_relative_change_percent) as weight
  , CASE
    WHEN DAILY_PERCENTAGES.state IN ('AL','AK','AZ','AR','FL','GA','ID','IN','IA','KS','KY','LA','MI','MS','MT','NE','MO','OK','PA','TN','TX','UT','WV','WY','WI','NC','ND','SD','OH','SC') THEN true
    WHEN DAILY_PERCENTAGES.state IN ('CA','CO','CT','DC','DE','HI','IL','ME','MA','MN','NH','NM','NY','WA','MD','NV','NJ','RI','OR','VA','VT') THEN false
    END as GOP_state
FROM DAILY_PERCENTAGES 
WHERE date IN (SELECT DISTINCT TOP 7 date FROM DAILY_PERCENTAGES ORDER BY 1 desc) -- to see the data from the X most recent dates
GROUP BY state, GOP_state
ORDER BY weight desc
;

/* -- former state calculation
SELECT state_code, SUM(arithmetic_relative_change_percent)
FROM GEO_DATA.PUBLIC.recent_hotspots
WHERE date IN (SELECT DISTINCT TOP 7 date FROM GEO_DATA.PUBLIC.recent_hotspots ORDER BY 1 desc) -- to see the data from the X most recent dates
GROUP BY 1 ORDER BY SUM(arithmetic_relative_change_percent) desc
; */

-- which county has the most "weight" among hotspot calculations?
SELECT 
    recent_hotspots.state_code
    , recent_hotspots.county
    , GOP_COUNTY
    , CASE
      WHEN recent_hotspots.state_code IN ('AL','AK','AZ','AR','FL','GA','ID','IN','IA','KS','KY','LA','MI','MS','MT','NE','MO','OK','PA','TN','TX','UT','WV','WY','WI','NC','ND','SD','OH','SC') THEN true
      WHEN recent_hotspots.state_code IN ('CA','CO','CT','DC','DE','HI','IL','ME','MA','MN','NH','NM','NY','WA','MD','NV','NJ','RI','OR','VA','VT') THEN false
      END as GOP_state
    , SUM(arithmetic_relative_change_percent)
FROM GEO_DATA.PUBLIC.recent_hotspots
    INNER JOIN GEO_DATA.PUBLIC.county_leans cl ON cl.county_id = recent_hotspots.county_id
WHERE recent_hotspots.date IN (SELECT DISTINCT TOP 7 date FROM GEO_DATA.PUBLIC.recent_hotspots ORDER BY 1 desc) -- to see the data from the X most recent dates
GROUP BY 1,2,3,4 ORDER BY SUM(arithmetic_relative_change_percent) desc
LIMIT 100
;

-- data for the spreadsheet tabs in 'county_hotspots.xlsx'
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.WEIGHTED_HOTSPOTS as
(
  SELECT
      state_code
      , county
      , county_id
      , SUM(ARITHMETIC_RELATIVE_CHANGE_PERCENT) as weight
      , min_distance_walmart
      , min_distance_tjs
      , which_is_closer
  FROM GEO_DATA.PUBLIC.recent_hotspots
  WHERE 1=1
--	AND recent_hotspots.date <= '2020-04-05' -- for the tab titled 'Hotspots > .0005, early April'
    AND recent_hotspots.date IN (SELECT DISTINCT TOP 7 date FROM GEO_DATA.PUBLIC.recent_hotspots ORDER BY 1 desc) -- to see the data from the X most recent dates
  GROUP BY state_code, county, county_id, min_distance_walmart, min_distance_tjs, which_is_closer
  ORDER BY weight desc
--LIMIT 200
);

-- pick some of these to fill the table in the Google doc, April 26th section
SELECT
	County
	, state_code
	, date
	, ARITHMETIC_RELATIVE_CHANGE_PERCENT
	, cases, new_cases
	, deaths, new_deaths
	, total_population
FROM GEO_DATA.PUBLIC.recent_hotspots
WHERE 1=1
    AND recent_hotspots.date IN (SELECT DISTINCT TOP 7 date FROM GEO_DATA.PUBLIC.recent_hotspots ORDER BY 1 desc) -- to see the data from the X most recent dates
ORDER BY ARITHMETIC_RELATIVE_CHANGE_PERCENT desc
LIMIT 100;

-- data for the choropleth tabs in 'county_hotspots.xlsx'
SELECT
	CASE 
		WHEN recent_hotspots.state_code = 'LA' THEN CONCAT(usc.county,' Parish')
		ELSE CONCAT(usc.county,' County')
		END as county
	, recent_hotspots.state_code
	, recent_hotspots.county_id
	, usc.GEO_ID
	, SUM(arithmetic_relative_change_percent)
	, MAX(cases)
	, MAX(new_cases)
	, MAX(deaths)
	, MAX(new_deaths)
	, total_population
FROM GEO_DATA.PUBLIC.recent_hotspots
    INNER JOIN GEO_DATA.PUBLIC.US_COUNTIES usc ON usc.county_id = recent_hotspots.county_id
WHERE 1=1
    AND recent_hotspots.date IN (SELECT DISTINCT TOP 7 date FROM GEO_DATA.PUBLIC.recent_hotspots ORDER BY 1 desc) -- to see the data from the X most recent dates
GROUP BY 1,2,3,4,total_population
ORDER BY SUM(arithmetic_relative_change_percent) desc
LIMIT 500
;

--------------------------------------
-- county preparedness metric, F-19 --
--------------------------------------
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.F19 as
  WITH collapsed_counties as
  (
    SELECT DISTINCT
          cg.GEO_ID as geoid10
          , REPLACE(cg.GEO_ID,'0500000US','') as geoid_trunc
          , usc.county_id
          , CASE 
              WHEN usc.GEO_ID IN ('0500000US36005','0500000US36047','0500000US36061','0500000US36081','0500000US36085') THEN 'New York City' -- collapse all 5 boroughs to "NYC", because NYT data requires it
              ELSE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(usc.COUNTY, ' County', ''),' Parish', ''),' Census Area', ''),' and Borough', ''),' Borough', ''),' Municipality', '')
              END as county
          , usc.state_code
          , usc.population
          , population_65 / usc.population as elder_pop
          , 1 - (population_white / usc.population) as nonwhite_pop
          , pl.POVERTY_LEVEL
          , ui.PERCENT_UNINSURED
          , cd.WALMART_DENSITY
          , cd.TJS_DENSITY
  FROM COUNTY_GEOIDS cg
    INNER JOIN GEO_DATA.PUBLIC.US_COUNTIES usc ON usc.GEO_ID = cg.GEO_ID
    INNER JOIN GEO_DATA.PUBLIC.UNINSURED ui ON ui.GEO_ID = cg.GEO_ID
    INNER JOIN GEO_DATA.PUBLIC.POVERTY_LEVEL pl ON pl.GEO_ID = cg.GEO_ID
    INNER JOIN GEO_DATA.PUBLIC.COUNTY_DISTANCES cd ON cd.county_id = usc.county_id
    INNER JOIN GEO_DATA.PUBLIC.COUNTY_HOTSPOTS ch ON ch.county_id = cd.county_id
  )
  SELECT 
      cc.*
      , SUM(rh.ARITHMETIC_RELATIVE_CHANGE_PERCENT) as weight
  FROM GEO_DATA.PUBLIC.recent_hotspots rh
      INNER JOIN collapsed_counties cc ON cc.county_id = rh.county_id
  WHERE 1=1
  --	AND rh.date <= '2020-03-05' -- for the tab titled 'Hotspots > .0005, early April'
  --	AND rh.date >= '2020-05-15' -- for the tab titled 'Hotspots > .0005, late April'
  GROUP BY 
    geoid10
      , geoid_trunc
      , cc.county_id
      , cc.county
      , cc.state_code
      , cc.WALMART_DENSITY
      , cc.TJS_DENSITY
      , cc.population
      , cc.elder_pop
      , cc.nonwhite_pop
      , cc.POVERTY_LEVEL
      , cc.PERCENT_UNINSURED
  ;

  -- rank each US county from 1 to 3100+, for each of the co-factors
  -- the RANK() function is 'sparse' rather than dense. For example if 3 counties are all tied in some metric, the next county after than will be ranked #4
  -- if I used DENSE_RANK() instead it would pack all ranks together such that the highest would be 3100 or so.
  CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.aggregate_rankings as
  (
    WITH county_density as
    ( -- debatable whether density calc should be total area, or just land area. Manhattan is apparently 30% water!
      SELECT DISTINCT usc.COUNTY, usc.STATE, usc.GEO_ID, usc.population / cla.area as density, usc.population, cla.area
      FROM county_land_area cla
          INNER JOIN us_counties usc ON usc.GEO_ID = cla.GEO_ID
      WHERE cla.area >0
      ORDER BY density desc
    )
    SELECT DISTINCT
      cg.GEO_ID
      , REPLACE(usc.GEO_ID,'0500000US','') as geoid_trunc
      , cg.COUNTY
      , cg.STATE
      , IFNULL(f19.WEIGHT,0) as weight
      , cp.POPULATION
      , cla.DENSITY
      , cla.DENSITY / (SELECT MAX(DENSITY) FROM county_density) as density_rank
      , RANK() OVER (ORDER BY cla.DENSITY desc) AS density_rank_sequential
      , ui.PERCENT_UNINSURED as UNINSURED
      , ui.PERCENT_UNINSURED / (SELECT MAX(PERCENT_UNINSURED) FROM GEO_DATA.PUBLIC.UNINSURED) AS uninsured_rank
      , RANK() OVER (ORDER BY ui.PERCENT_UNINSURED desc) AS uninsured_rank_sequential
      , pl.POVERTY_LEVEL
      , pl.POVERTY_LEVEL / (SELECT MAX(POVERTY_LEVEL) FROM GEO_DATA.PUBLIC.POVERTY_LEVEL) AS poverty_rank
      , RANK() OVER (ORDER BY pl.POVERTY_LEVEL desc) AS poverty_rank_sequential
      , nw.PERCENT_NON_WHITE
      , nw.PERCENT_NON_WHITE / (SELECT MAX(PERCENT_NON_WHITE) FROM GEO_DATA.PUBLIC.NON_WHITE nw) AS non_white_rank
      , RANK() OVER (ORDER BY nw.PERCENT_NON_WHITE desc) AS non_white_rank_sequential
      , ahs.AVG_SIZE
      , ahs.AVG_SIZE / (SELECT MAX(AVG_SIZE) FROM GEO_DATA.PUBLIC.AVG_HOUSEHOLD_SIZE) AS household_size_rank
      , RANK() OVER (ORDER BY ahs.AVG_SIZE desc) AS household_size_rank_sequential
      , cd.PERCENT_DISABILITY
      , cd.PERCENT_DISABILITY / (SELECT MAX(PERCENT_DISABILITY) FROM GEO_DATA.PUBLIC.COUNTY_DISABILITY) AS disability_rank
      , RANK() OVER (ORDER BY cd.PERCENT_DISABILITY desc) AS disability_rank_sequential
      , hgr.LESS_THAN_HS_GRADUATE
      , hgr.LESS_THAN_HS_GRADUATE / (SELECT MAX(LESS_THAN_HS_GRADUATE) FROM GEO_DATA.PUBLIC.HS_GRADUATE_RATE) AS graduate_rank
      , RANK() OVER (ORDER BY hgr.LESS_THAN_HS_GRADUATE desc) AS graduate_rank_sequential
      , usc.POPULATION_65 / IFNULL(usc.POPULATION,1) as elderly
      , (usc.POPULATION_65 / IFNULL(usc.POPULATION,1)) / (SELECT MAX(POPULATION_65 / IFNULL(POPULATION,1)) FROM GEO_DATA.PUBLIC.US_COUNTIES) AS over65_rank
      , RANK() OVER (ORDER BY usc.POPULATION_65 / IFNULL(usc.POPULATION,1) desc) AS over65_rank_sequential
      , IFNULL(f19.WALMART_DENSITY,0) as walmart_density
      , IFNULL(f19.WALMART_DENSITY,0) / (SELECT MAX(WALMART_DENSITY) FROM GEO_DATA.PUBLIC.F19) AS walmart_rank
      , RANK() OVER (ORDER BY IFNULL(f19.WALMART_DENSITY,0) desc) AS walmart_rank_sequential
      , IFNULL(f19.TJS_DENSITY,0) as TJS_density
      , IFNULL(f19.TJS_DENSITY,0) / (SELECT MAX(TJS_DENSITY) FROM GEO_DATA.PUBLIC.F19) AS TJS_rank
      , RANK() OVER (ORDER BY IFNULL(f19.TJS_DENSITY,0) desc) AS TJS_rank_sequential
      , ((1.0*IFNULL(non_white_rank,0)) + (0.69*IFNULL(household_size_rank,0)) + (0.55*IFNULL(density_rank,0)) + (0.43*IFNULL(uninsured_rank,0)) + (0.43*IFNULL(poverty_rank,0)) + (0.39*IFNULL(graduate_rank,0)) + (0.29*IFNULL(walmart_rank,0)) + (0.25*IFNULL(disability_rank,0)) + (0.24*IFNULL(over65_rank,0))) as average_rank
      , ((1.0*weight*IFNULL(non_white_rank,0)) + (0.69*weight*IFNULL(household_size_rank,0)) + (0.55*weight*IFNULL(density_rank,0)) + (0.43*weight*IFNULL(uninsured_rank,0)) + (0.43*weight*IFNULL(poverty_rank,0)) + (0.39*weight*IFNULL(graduate_rank,0)) + (0.29*weight*IFNULL(walmart_rank,0)) + (0.25*weight*IFNULL(disability_rank,0)) + (0.24*weight*IFNULL(over65_rank,0))) as weighted_average_rank
    FROM COUNTY_GEOIDS cg
      LEFT OUTER JOIN GEO_DATA.PUBLIC.US_COUNTIES usc ON usc.GEO_ID = cg.GEO_ID
      LEFT OUTER JOIN GEO_DATA.PUBLIC.UNINSURED ui ON ui.GEO_ID = cg.GEO_ID
      LEFT OUTER JOIN GEO_DATA.PUBLIC.POVERTY_LEVEL pl ON pl.GEO_ID = cg.GEO_ID
      LEFT OUTER JOIN GEO_DATA.PUBLIC.COUNTY_POPULATION cp ON cp.GEO_ID = cg.GEO_ID
      LEFT OUTER JOIN GEO_DATA.PUBLIC.NON_WHITE nw ON nw.GEO_ID = cg.GEO_ID
      LEFT OUTER JOIN GEO_DATA.PUBLIC.AVG_HOUSEHOLD_SIZE ahs ON ahs.GEO_ID = cg.GEO_ID
      LEFT OUTER JOIN GEO_DATA.PUBLIC.COUNTY_DISABILITY cd ON cd.GEO_ID = cg.GEO_ID
      LEFT OUTER JOIN GEO_DATA.PUBLIC.HS_GRADUATE_RATE hgr ON hgr.GEO_ID = cg.GEO_ID
      LEFT OUTER JOIN GEO_DATA.PUBLIC.ELDERLY_POPULATION ep ON ep.GEO_ID = cg.GEO_ID
      LEFT OUTER JOIN GEO_DATA.PUBLIC.F19 ON F19.GEOID10 = cg.GEO_ID
      INNER JOIN county_density cla ON cla.GEO_ID = cg.GEO_ID
  --  WHERE IFNULL(f19.WEIGHT,0)>0
    ORDER BY average_rank desc
  )
  ;

  -- which co-factors are the most relevant? (lower average rank() value means stronger influence by that co-factor)
  WITH top_hotspots as
  (
    SELECT *
    FROM aggregate_rankings  
    WHERE weight >= (SELECT MEDIAN(weight) + StdDev(weight) FROM aggregate_rankings) -- debating whether to use MEDIAN() or AVG()
  ), pivoted as
  (
    SELECT
       CAST(MEDIAN(DENSITY_RANK_SEQUENTIAL) as float) as DENSITY_AVERAGE -- debating whether to use MEDIAN() or AVG()
      ,CAST(MEDIAN(UNINSURED_RANK_SEQUENTIAL) as float) as UNINSURED_AVERAGE
      ,CAST(MEDIAN(POVERTY_RANK_SEQUENTIAL) as float) as POVERTY_AVERAGE
      ,CAST(MEDIAN(NON_WHITE_RANK_SEQUENTIAL) as float) as NON_WHITE_AVERAGE
      ,CAST(MEDIAN(HOUSEHOLD_SIZE_RANK_SEQUENTIAL) as float) as HOUSEHOLD_SIZE_AVERAGE
      ,CAST(MEDIAN(DISABILITY_RANK_SEQUENTIAL) as float) as DISABILITY_AVERAGE
      ,CAST(MEDIAN(GRADUATE_RANK_SEQUENTIAL) as float) as GRADUATE_AVERAGE
      ,CAST(MEDIAN(OVER65_RANK_SEQUENTIAL) as float) as OVER65_AVERAGE
      ,CAST(MEDIAN(WALMART_RANK_SEQUENTIAL) as float) as WALMART_AVERAGE
  --	,CAST(MEDIAN(TJS_RANK) as float) as TJS_AVERAGE
     FROM top_hotspots
  )
  SELECT * FROM pivoted
      UNPIVOT(average for factor in (DENSITY_AVERAGE, UNINSURED_AVERAGE, POVERTY_AVERAGE, NON_WHITE_AVERAGE, HOUSEHOLD_SIZE_AVERAGE, DISABILITY_AVERAGE, GRADUATE_AVERAGE, OVER65_AVERAGE, WALMART_AVERAGE))
  ORDER BY average
  ;

-- to load the choropleth data into Plotly for predictive hotspots
SELECT GEO_ID, COUNTY, STATE, average_rank / (SELECT MAX(average_rank) from aggregate_rankings) as norm_WEIGHT
FROM aggregate_rankings
WHERE norm_WEIGHT IS NOT NULL
ORDER BY GEO_ID
ORDER BY norm_WEIGHT desc
;

-- see how broadly the data is distributed among the co-factors
WITH DISTS as
(
  SELECT
       COUNT(DISTINCT DENSITY_RANK_SEQUENTIAL) as DENSITY_DIST -- debating whether to use either AVG() or COUNT(DISTINCT )
      ,COUNT(DISTINCT UNINSURED_RANK_SEQUENTIAL) as UNINSURED_DIST
      ,COUNT(DISTINCT POVERTY_RANK_SEQUENTIAL) as POVERTY_DIST
      ,COUNT(DISTINCT NON_WHITE_RANK_SEQUENTIAL) as NON_WHITE_DIST
      ,COUNT(DISTINCT HOUSEHOLD_SIZE_RANK_SEQUENTIAL) as HOUSEHOLD_SIZE_DIST
      ,COUNT(DISTINCT DISABILITY_RANK_SEQUENTIAL) as DISABILITY_DIST
      ,COUNT(DISTINCT GRADUATE_RANK_SEQUENTIAL) as GRADUATE_DIST
      ,COUNT(DISTINCT OVER65_RANK_SEQUENTIAL) as OVER65_DIST
      ,COUNT(DISTINCT WALMART_RANK_SEQUENTIAL) as WALMART_DIST
  FROM aggregate_rankings
)
SELECT * FROM DISTS
    UNPIVOT(average for factor in (DENSITY_DIST, UNINSURED_DIST, POVERTY_DIST, NON_WHITE_DIST, HOUSEHOLD_SIZE_DIST, DISABILITY_DIST, GRADUATE_DIST, OVER65_DIST, WALMART_DIST))
ORDER BY average
;

-- weighted average rank
SELECT *
FROM aggregate_rankings
WHERE average_rank IS NOT NULL
ORDER BY average_rank desc
;

------------------------------------------------------------
-- general statistics on counties, distances, cases, etc. --
------------------------------------------------------------

-- all US counties and their nearest TJs and Walmart
SELECT DISTINCT 
	demo.state_code
	, demo.county
	, (select min(distance) FROM GEO_DATA.PUBLIC.walmart_geodata_matrix wg2 WHERE wg2.state_code = wgm.state_code AND wg2.county = wgm.county) as min_distance_walmart
	, (select min(distance) FROM GEO_DATA.PUBLIC.tjs_geodata_matrix tg2 WHERE tg2.state_code = wgm.state_code AND tg2.county = wgm.county) as min_distance_tjs
	, CASE 
		WHEN tgm.distance <= wgm.distance THEN 1
		ELSE 0
		END as which_is_closer
FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo
  INNER JOIN GEO_DATA.PUBLIC.walmart_geodata_matrix wgm ON (wgm.state_code = demo.state_code AND wgm.county = demo.county)
  INNER JOIN GEO_DATA.PUBLIC.tjs_geodata_matrix tgm ON (tgm.state_code = demo.state_code AND tgm.county = demo.county)
WHERE 1=1
	AND wgm.distance = min_distance_walmart
	AND tgm.distance = min_distance_tjs
ORDER BY 1,2
;

-- counties with highest density of walmarts
WITH high_density_counties as
	(
	  SELECT demo.state_code, demo.county, COUNT(*) as num_walmarts
	  FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo
	    INNER JOIN GEO_DATA.PUBLIC.WALMART_ZIPS wmz ON wmz.ZIP = demo.ZIP
	  GROUP BY 1,2 ORDER BY num_walmarts desc
	),
	county_population as
	(
	  SELECT demo.state_code, demo.county, SUM(demo.population) as population
	  FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo 
	  GROUP BY 1,2 ORDER BY 3 desc
	  LIMIT 100
	)
SELECT demo.state_code, demo.county, demo.population / hdc.num_walmarts as density
FROM high_density_counties hdc
  INNER JOIN county_population demo ON (demo.state_code = hdc.state_code AND demo.county = hdc.county)
ORDER BY 3
;

-- which ZIP has the greatest local density of Walmarts in the US? 100+ is pretty common
WITH ALL_COUNTIES_NEAREST_TJ AS
	(
	  SELECT DISTINCT
	    zip
	    , population
	    , (select min(distance) FROM GEO_DATA.PUBLIC.tjs_geodata_matrix tg2 WHERE tg2.target_zip = demo.zip) as min_distance_tjs
	  FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo
	  WHERE min_distance_tjs IS NOT NULL
	  ORDER BY 3 desc
	)
SELECT
  ALL_COUNTIES_NEAREST_TJ.zip
  , ALL_COUNTIES_NEAREST_TJ.population
  , min_distance_tjs
  , COUNT(DISTINCT wgm.zip)
  , COUNT(DISTINCT wgm.zip) / min_distance_tjs as density
FROM ALL_COUNTIES_NEAREST_TJ
    INNER JOIN GEO_DATA.PUBLIC.walmart_geodata_matrix wgm ON wgm.target_zip = ALL_COUNTIES_NEAREST_TJ.zip
WHERE wgm.distance <= ALL_COUNTIES_NEAREST_TJ.min_distance_tjs
GROUP BY 1,2,3
HAVING COUNT(DISTINCT wgm.zip) >= 50
ORDER BY density desc
;

-- which ZIP has the greatest local density of TJs in the US?
WITH ALL_COUNTIES_NEAREST_WALMART AS
(
  SELECT DISTINCT
    zip
    , population
    , (select min(distance) FROM GEO_DATA.PUBLIC.WALMART_GEODATA_MATRIX wg2 WHERE wg2.target_zip = demo.zip) as min_distance_walmarts
  FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo
  WHERE min_distance_walmarts IS NOT NULL
  ORDER BY 3 desc
)
SELECT
  ALL_COUNTIES_NEAREST_WALMART.zip
  , ALL_COUNTIES_NEAREST_WALMART.population
  , min_distance_walmarts
  , COUNT(DISTINCT tgm.zip)
  , COUNT(DISTINCT tgm.zip) / min_distance_walmarts as density
FROM ALL_COUNTIES_NEAREST_WALMART
    INNER JOIN GEO_DATA.PUBLIC.TJS_GEODATA_MATRIX tgm ON tgm.target_zip = ALL_COUNTIES_NEAREST_WALMART.zip
WHERE tgm.distance <= ALL_COUNTIES_NEAREST_WALMART.min_distance_walmarts
GROUP BY 1,2,3
HAVING COUNT(DISTINCT tgm.zip) >= 15
ORDER BY density desc
;

-- locations which are farthest from a Walmart
SELECT 
	demo.city
	, wmz.county
	, wmz.state_code
	, wmz.target_zip
	, wmz.zip
	, (select min(distance) FROM GEO_DATA.PUBLIC.walmart_geodata_matrix wm2 WHERE wm2.target_zip = wmz.target_zip) as min_distance_join
FROM GEO_DATA.PUBLIC.ZIP_GEODATA zgd
	INNER JOIN GEO_DATA.PUBLIC.walmart_geodata_matrix wmz ON wmz.target_zip = zgd.ZIP
	INNER JOIN GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo ON demo.ZIP = zgd.ZIP
WHERE wmz.distance = min_distance_join
	AND wmz.state_code <> 'AK'
ORDER BY min_distance_join desc
LIMIT 100
;

-- locations which are farthest from a TJ
SELECT
	demo.city
	, wmz.county
	, wmz.state_code
	, wmz.target_zip
	, wmz.zip
	, (select min(distance) FROM GEO_DATA.PUBLIC.tjs_geodata_matrix wm2 WHERE wm2.target_zip = wmz.target_zip) as min_distance_join
FROM GEO_DATA.PUBLIC.ZIP_GEODATA zgd
	INNER JOIN GEO_DATA.PUBLIC.tjs_geodata_matrix wmz ON wmz.target_zip = zgd.ZIP
	INNER JOIN GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo ON demo.ZIP = zgd.ZIP
WHERE wmz.distance = min_distance_join
	AND wmz.state_code <> 'AK'
ORDER BY min_distance_join desc
LIMIT 100
;

-- # of TJs closer to a target ZIP than the nearest Walmart is to that ZIP
WITH NEAREST_WALMART AS
(
  SELECT target_zip, zip, distance
  FROM GEO_DATA.PUBLIC.walmart_geodata_matrix
  WHERE target_zip = '90210'
  ORDER BY distance
  LIMIT 1
)
  SELECT COUNT(DISTINCT radius.zip)
  FROM GEO_DATA.PUBLIC.tjs_geodata_matrix radius
    INNER JOIN NEAREST_WALMART ON NEAREST_WALMART.target_ZIP = radius.target_zip
  WHERE radius.distance <= NEAREST_WALMART.distance
;
  
-- # of Walmarts closer to a target ZIP than the nearest TJs is to that ZIP
WITH NEAREST_TJ AS
(
  SELECT target_zip, zip, distance
  FROM GEO_DATA.PUBLIC.tjs_geodata_matrix
  WHERE target_zip = '62448'
  ORDER BY distance
  LIMIT 1
)
  SELECT COUNT(DISTINCT radius.zip)
  FROM GEO_DATA.PUBLIC.walmart_geodata_matrix radius
    INNER JOIN NEAREST_TJ ON NEAREST_TJ.target_ZIP = radius.target_zip
  WHERE radius.distance <= NEAREST_TJ.distance
;

-- closest large metro area to a given state
WITH pop_center_nearest_county as
(
  WITH county_pop_center_matrix as
  (
    WITH pop_centers as
        (
          SELECT 
            state
            , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', ''),' Census Area', ''),' and Borough', ''),' Borough', ''),' Municipality', '') as county
            , total_population
            , latitude
            , longitude
          FROM COVID.PUBLIC.DEMOGRAPHICS
          WHERE total_population >= 1000000
          ORDER BY total_population desc
        )
    SELECT DISTINCT
            demo.state_name as state
            , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(demo.COUNTY, ' County', ''),' Parish', ''),' Census Area', ''),' and Borough', ''),' Borough', ''),' Municipality', '') as county
            , tll.latitude as t_lat
            , tll.longitude as t_long
            , tll.county as nearest_population_center
            , 7922 * atan2(sqrt(SQUARE(sin(((AVG(demo.latitude) - t_lat) * pi()/180)/2)) + cos(t_lat * pi()/180) * cos(AVG(demo.latitude) * pi()/180) *  SQUARE(sin(((AVG(demo.longitude) - t_long) * pi()/180)/2))), sqrt(1-SQUARE(sin(((AVG(demo.latitude) - t_lat) * pi()/180)/2)) + cos(t_lat * pi()/180) * cos(AVG(demo.latitude) * pi()/180) *  SQUARE(sin(((AVG(demo.longitude) - t_long) * pi()/180)/2)))) as distance
            , AVG(demo.latitude) as c_lat
            , AVG(demo.longitude) as c_long
    FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo
      FULL OUTER JOIN pop_centers tll
    WHERE demo.COUNTY IS NOT NULL -- AND demo.state = 'NY'
    GROUP BY 1,2,3,4,5
  ORDER BY COUNTY, DISTANCE
    )
  SELECT ctm.*, (select min(distance) FROM county_pop_center_matrix ctm2 WHERE ctm2.state = ctm.state AND ctm2.county = ctm.county) as min_distance_join
  FROM county_pop_center_matrix ctm
  WHERE ctm.distance = min_distance_join
)
SELECT DISTINCT
    pcnc.state
    , pcnc.county
    , pcnc.nearest_population_center
    , pcnc.distance
FROM GEO_DATA.PUBLIC.pop_center_nearest_county pcnc
	INNER JOIN GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo ON (demo.state_name = pcnc.state AND demo.county = pcnc.county)
WHERE demo.state_name = 'Montana'
ORDER BY distance 
;

-- median distance from a county center to a walmart. Might need to adjust Google/LinkedIn docs according to these results
-- hotspot proximity calcs may suffer the zip vs. county thing
WITH county_walmart_distances as
(
  SELECT DISTINCT
      wcdm.GEO_ID
      , wcdm.state_code
      , wcdm.county
      , (select min(distance) FROM GEO_DATA.PUBLIC.walmart_county_distance_matrix wg2 WHERE wg2.GEO_ID = wcdm.GEO_ID) as min_distance_walmart
  FROM GEO_DATA.PUBLIC.walmart_geodata_matrix wcdm
      WHERE wcdm.distance = min_distance_walmart
      AND state_code != 'AK'
)
SELECT
    MEDIAN(min_distance_walmart) as amdw
FROM county_walmart_distances
--WHERE state_code NOT IN ('AK','AZ','CA','CO','HI','ID','KS','MT','NE','NV','NM','ND','OR','SD','TX','UT','WA','WY') -- Western states much more spread out
;

-- all of the walmarts within a radius of a given county center, closer than the nearest TJs
SELECT DISTINCT 
    wcdm.state
    , wcdm.city
    , zgd.county
    , wcdm.latitude
    , wcdm.longitude
    , 7922 * atan2(sqrt(SQUARE(sin(((wcdm.latitude - cd.latitude) * pi()/180)/2)) + cos(cd.latitude * pi()/180) * cos(wcdm.latitude * pi()/180) *  SQUARE(sin(((wcdm.longitude - cd.longitude) * pi()/180)/2))), sqrt(1-SQUARE(sin(((wcdm.latitude - cd.latitude) * pi()/180)/2)) + cos(cd.latitude * pi()/180) * cos(wcdm.latitude * pi()/180) *  SQUARE(sin(((wcdm.longitude - cd.longitude) * pi()/180)/2)))) as distance
FROM GEO_DATA.PUBLIC.COUNTY_DISTANCES cd
    FULL OUTER JOIN GEO_DATA.PUBLIC.walmart_geocodes wcdm
    INNER JOIN GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE zgd ON zgd.ZIP = wcdm.ZIP
WHERE cd.state_code = 'IL' AND cd.county = 'Jasper'
AND distance <= cd.min_distance_tjs
ORDER BY distance
;

-- what is the ZIP of the closest TJs to a given county
SELECT *
FROM GEO_DATA.PUBLIC.TJS_COUNTY_DISTANCE_MATRIX cd
WHERE cd.state_code = 'IL' AND cd.county = 'Jasper'
ORDER BY distance
LIMIT 1
;

-- US counties by population
SELECT *
FROM GEO_DATA.PUBLIC.US_COUNTIES
ORDER BY population desc
;

-- counties which are in US Census imported data but not in Snowflake's COVID.PUBLIC.DEMOGRAPHICS data
-- some are because we don't compute TJ/Walmart distances for counties in AK which are super-remote.
-- but some are due to naming issues, or because cities (e.g. in Virginia) are not counties
WITH modified_demo as
    (
      SELECT 
        state_code
        , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', ''),' Census Area', ''),' and Borough', ''),' Borough', ''),' Municipality', '') as county
        , county_id
        , GEO_ID
      FROM GEO_DATA.PUBLIC.COUNTY_DISTANCES
      ORDER BY state_code, county
)
SELECT usc.*
FROM GEO_DATA.PUBLIC.us_counties usc
    LEFT OUTER JOIN modified_demo md ON md.GEO_ID = usc.GEO_ID
WHERE md.county IS NULL
ORDER BY state_code, county
;

-- latest # of cases by date & state
SELECT date, state, sum(cases)
FROM COVID.PUBLIC.NYT_US_COVID19
WHERE date = (SELECT DISTINCT date FROM COVID.PUBLIC.NYT_US_COVID19 ORDER BY date desc LIMIT 1)
GROUP BY 1,2
ORDER BY 1,2
;

-- # of cases by date, entire US, from NYT_US_COVID19 data set
SELECT date, sum(cases)
FROM COVID.PUBLIC.NYT_US_COVID19
GROUP BY 1
ORDER BY 1 desc
;

-- # of cases by date, entire US, from CT_US_COVID_TESTS data set
SELECT
	sd1.date
	, SUM(IFNULL(sd1.POSITIVE,0)) as cases
	, SUM(IFNULL(sd1.POSITIVE_SINCE_PREVIOUS_DAY,0)) as new_cases
	, SUM(IFNULL(sd1.DEATH,0)) as deaths
	, SUM(IFNULL(sd1.DEATH_SINCE_PREVIOUS_DAY,0)) as new_deaths
FROM COVID.PUBLIC.CT_US_COVID_TESTS sd1
GROUP BY 1 ORDER BY 1 desc
LIMIT 100
;

/*
-- attempt to designate a county_id for every ZIP in the US
-- potential ZIP-to-county mapping from https://www.huduser.gov/portal/datasets/usps_crosswalk.html, but not convinced the data is accurate in all cases

UPDATE GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE cd
SET COUNTY_ID = NULL;
UPDATE GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE cd
SET COUNTY_ID = COUNTY_MAPPING.COUNTY_ID
FROM   (
	WITH all_counties as
		(
          SELECT *
          FROM GEO_DATA.PUBLIC.US_COUNTIES
          ORDER BY STATE, COUNTY
		)
		SELECT GEO_ID, COUNTY_ID, STATE_CODE, COUNTY
		FROM all_counties
		ORDER BY STATE_CODE, COUNTY
	) COUNTY_MAPPING
WHERE LOWER(COUNTY_MAPPING.STATE_CODE) = LOWER(cd.state_code) AND LOWER(COUNTY_MAPPING.county) = LOWER(cd.county)
;
-- code to add a column to a static table and then populate that column
--ALTER TABLE GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE ADD COLUMN ZIP_ID INTEGER;
--CREATE or REPLACE sequence GEO_DATA.PUBLIC.seq1;
--UPDATE GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE zgc
SET ZIP_ID = zip_id_mapping.ZIP_ID_SEQ
FROM   (
	WITH all_zips as
		(
          SELECT ZIP
          FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE
          ORDER BY ZIP    
		)
		SELECT ZIP, s.nextval as ZIP_ID_SEQ
		FROM all_zips, table(getnextval(GEO_DATA.PUBLIC.seq1)) s
		ORDER BY ZIP    
	) zip_id_mapping
WHERE zip_id_mapping.zip = zgc.zip;

-- to find counties where the land area is not in the relevant census table
-- for some reason there are counties, like Oglala Lakota SD, which are not in my GEO_JSON file
SELECT DISTINCT usc.COUNTY, usc.STATE, usc.GEO_ID, usc.population
FROM us_counties usc
    LEFT OUTER JOIN county_land_area cla ON usc.GEO_ID = cla.GEO_ID
WHERE cla.GEO_ID IS NULL

--INSERT INTO county_land_area (COUNTY, GEO_ID, AREA) VALUES ('Kusilvak Census Area, AK', '0500000US02158', 19673.0)

*/

SELECT *
FROM aggregate_rankings  
WHERE weight >= (SELECT AVG(weight) + StdDev(weight) FROM aggregate_rankings) -- debating whether to use MEDIAN() or AVG()
--ORDER BY weighted_average_rank desc
ORDER BY weight desc

SELECT * FROM county_hotspots
WHERE date IN (SELECT DISTINCT date FROM county_hotspots ORDER BY 1 desc LIMIT 7 )
--AND state_code = 'MD'
ORDER BY ARITHMETIC_RELATIVE_CHANGE_PERCENT desc

SELECT distinct county, state, fips
FROM COVID.PUBLIC.NYT_US_COVID19 nyt
where FIPS IS NULL

SELECT DISTINCT COUNTRY_REGION, PROVINCE_STATE, LAST_UPDATED_DATE
FROM COVID.PUBLIC.CT_US_COVID_TESTS sd1

-- which states are trending up in positive case rates?
CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.STATE_CASE_TRENDS as
(
  SELECT DATE, COUNTRY_REGION, PROVINCE_STATE, POSITIVE, POSITIVE_SINCE_PREVIOUS_DAY, TOTAL, TOTAL_SINCE_PREVIOUS_DAY, POSITIVE_SINCE_PREVIOUS_DAY /  TOTAL_SINCE_PREVIOUS_DAY AS POSITIVE_GROWTH
  FROM COVID.PUBLIC.CT_US_COVID_TESTS sd1
  WHERE 1=1
    AND TOTAL_SINCE_PREVIOUS_DAY > 0
    AND POSITIVE_SINCE_PREVIOUS_DAY /  TOTAL_SINCE_PREVIOUS_DAY < 1.0
)

SELECT *
FROM GEO_DATA.PUBLIC.STATE_CASE_TRENDS
WHERE date = '2020-08-03'
ORDER BY positive_growth desc

CREATE OR REPLACE TEMPORARY TABLE GEO_DATA.PUBLIC.STATE_PER_CAPITA_WEIGHT as
(
  SELECT state_code, SUM(cases / (total_population/100000)) as cases100
  FROM GEO_DATA.PUBLIC.recent_hotspots
  WHERE 1=1
    AND COUNTY_ID NOT IN (1871,1831,1859,1869,1852)
    AND date IN (SELECT DISTINCT date FROM county_hotspots ORDER BY 1 desc LIMIT 1 )
  GROUP BY state_code
  ORDER BY cases100 desc
)

SELECT StdDev(weight) FROM aggregate_rankings
SELECT AVG(weight) FROM aggregate_rankings

-- Red states
SELECT DISTINCT
    state_code
--    , state
FROM GEO_DATA.PUBLIC.US_COUNTIES
WHERE state IN ('Alabama','Alaska','Arizona','Arkansas','Florida','Georgia','Idaho','Indiana','Iowa','Kansas','Kentucky','Louisiana','Michigan','Mississippi','Missouri','Montana','Nebraska','North Carolina','North Dakota','Ohio','Oklahoma','Pennsylvania','South Carolina','South Dakota','Tennessee','Texas','Utah','West Virginia','Wisconsin','Wyoming')

-- Blue states
SELECT DISTINCT
    state_code
--    , state
FROM GEO_DATA.PUBLIC.US_COUNTIES
WHERE state IN ('California','Colorado','Connecticut','Delaware','District of Columbia','Hawaii','Illinois','Maine','Maryland','Massachusetts','Minnesota','Nevada','New Hampshire','New Jersey','New Mexico','New York','Oregon','Rhode Island','Vermont','Virginia','Washington')

-- Red states, codes
'AL','AK','AZ','AR','FL','GA','ID','IN','IA','KS','KY','LA','MI','MS','MT','NE','MO','OK','PA','TN','TX','UT','WV','WY','WI','NC','ND','SD','OH','SC'
-- Blue states, codes
'CA','CO','CT','DC','DE','HI','IL','ME','MA','MN','NH','NM','NY','WA','MD','NV','NJ','RI','OR','VA','VT'

SELECT *
FROM COVID.PUBLIC.NYT_US_COVID19
LIMIT 100

-- most hardcore Republican counties
SELECT *
FROM GEO_DATA.PUBLIC.COUNTY_VOTING cv
    INNER JOIN GEO_DATA.PUBLIC.US_COUNTIES usc ON REPLACE(usc.GEO_ID,'0500000US','')  = cv.fips
WHERE PER_GOP_2016 > PER_DEM_2016
ORDER BY PER_POINT_DIFF_2016

-- most hardcore Democratic counties
SELECT *
FROM GEO_DATA.PUBLIC.COUNTY_VOTING cv
    INNER JOIN GEO_DATA.PUBLIC.US_COUNTIES usc ON REPLACE(usc.GEO_ID,'0500000US','')  = cv.fips
WHERE PER_GOP_2016 < PER_DEM_2016
ORDER BY PER_POINT_DIFF_2016 desc

-- relative populations of all the GOP vs. Democratic counties (172M Democratic, 144M GOP)
SELECT GOP_COUNTY, SUM(population)
FROM GEO_DATA.PUBLIC.recent_hotspots
    INNER JOIN GEO_DATA.PUBLIC.county_leans cl ON cl.county_id = recent_hotspots.county_id
--WHERE date = '2020-03-25'
WHERE date IN (SELECT DISTINCT TOP 1 date FROM GEO_DATA.PUBLIC.recent_hotspots ORDER BY 1 desc) -- to see the data from the X most recent dates
GROUP BY GOP_COUNTY
ORDER BY 2 desc

-- relative populations of Blue vs. Red states (REd 179.6 vs Blue 138.8)
SELECT   
  CASE
    WHEN ISO3166_2 IN ('AL','AK','AZ','AR','FL','GA','ID','IN','IA','KS','KY','LA','MI','MS','MT','NE','MO','OK','PA','TN','TX','UT','WV','WY','WI','NC','ND','SD','OH','SC') THEN true
    WHEN ISO3166_2 IN ('CA','CO','CT','DC','DE','HI','IL','ME','MA','MN','NH','NM','NY','WA','MD','NV','NJ','RI','OR','VA','VT') THEN false
    END as GOP_state
, SUM(total_population)
FROM COVID.PUBLIC.DEMOGRAPHICS
GROUP BY GOP_state
ORDER BY 2 desc
;

-- compare counties by state and county leanings
SELECT 
    county_hotspots.state_code
  , county_hotspots.county
  , CASE
    WHEN county_hotspots.state_code IN ('AL','AK','AZ','AR','FL','GA','ID','IN','IA','KS','KY','LA','MI','MS','MT','NE','MO','OK','PA','TN','TX','UT','WV','WY','WI','NC','ND','SD','OH','SC') THEN true
    WHEN county_hotspots.state_code IN ('CA','CO','CT','DC','DE','HI','IL','ME','MA','MN','NH','NM','NY','WA','MD','NV','NJ','RI','OR','VA','VT') THEN false
    END as GOP_state
  , sum(new_cases) as cases
FROM GEO_DATA.PUBLIC.county_hotspots
    INNER JOIN GEO_DATA.PUBLIC.county_leans cl ON cl.county_id = county_hotspots.county_id
WHERE 1=1
  AND date >= '2020-05-19'
--  AND date IN (SELECT DISTINCT TOP 7 date FROM GEO_DATA.PUBLIC.county_hotspots ORDER BY 1 desc) -- to see the data from the X most recent dates
  AND GOP_COUNTY = true
  AND GOP_STATE = false
--  AND population > 500000
GROUP BY 
      county_hotspots.state_code
    , county_hotspots.county
    , GOP_STATE
--HAVING sum(new_cases) >= 1000
ORDER BY cases desc

-- compare counties by state and county leanings
SELECT 
    CASE
    WHEN county_hotspots.state_code IN ('AL','AK','AZ','AR','FL','GA','ID','IN','IA','KS','KY','LA','MI','MS','MT','NE','MO','OK','PA','TN','TX','UT','WV','WY','WI','NC','ND','SD','OH','SC') THEN true
    WHEN county_hotspots.state_code IN ('CA','CO','CT','DC','DE','HI','IL','ME','MA','MN','NH','NM','NY','WA','MD','NV','NJ','RI','OR','VA','VT') THEN false
    END as GOP_state
  , sum(population) as pop
FROM GEO_DATA.PUBLIC.county_hotspots
    INNER JOIN GEO_DATA.PUBLIC.county_leans cl ON cl.county_id = county_hotspots.county_id
WHERE 1=1
  AND date IN (SELECT DISTINCT TOP 1 date FROM GEO_DATA.PUBLIC.county_hotspots ORDER BY 1 desc) -- to see the data from the X most recent dates
  AND GOP_COUNTY = true
  AND GOP_STATE = false
GROUP BY GOP_STATE
ORDER BY pop desc

-- 113.082294 in red/red
-- 67.699837 in red state, blue county
-- 104.691469 in blue/blue
-- 31.751677 in blue state, red county
-- should normalize the per-county stats so that they are cases-per-1000 or similar. Otherwise the results are misleading due to the above numbers

-- newest hotspots based purely on count density
SELECT new_cases, cl.*
FROM GEO_DATA.PUBLIC.county_hotspots
    INNER JOIN GEO_DATA.PUBLIC.county_leans cl ON cl.county_id = county_hotspots.county_id
WHERE date IN (SELECT DISTINCT TOP 1 date FROM GEO_DATA.PUBLIC.recent_hotspots ORDER BY 1 desc) -- to see the data from the X most recent dates
--AND GOP_COUNTY = true
ORDER BY new_cases/cl.population desc
;

-- A tale of two Countries, by state
WITH pivoted as (
  SELECT 
      CASE
      WHEN ISO3166_2 IN ('AL','AK','AZ','AR','FL','GA','ID','IN','IA','KS','KY','LA','MI','MS','MT','NE','MO','OK','PA','TN','TX','UT','WV','WY','WI','NC','ND','SD','OH','SC') THEN true
      WHEN ISO3166_2 IN ('CA','CO','CT','DC','DE','HI','IL','ME','MA','MN','NH','NM','NY','WA','MD','NV','NJ','RI','OR','VA','VT') THEN false
      END as GOP_state
    , date
    , sum(cases) as cases
    , sum(deaths) as deaths
  FROM COVID.PUBLIC.NYT_US_COVID19
  WHERE 1=1
    AND date >= '2020-01-26'
    AND GOP_STATE IS NOT NULL
  GROUP BY date, GOP_STATE
  ORDER BY date, GOP_STATE
)
SELECT p1.date
    , p1.cases as red_cases
    , p1.deaths as red_deaths
    , p2.cases as blue_cases
    , p2.deaths as blue_deaths
FROM pivoted p1
    INNER JOIN pivoted p2 ON p1.date = p2.date
WHERE 1=1
    AND (p1.GOP_STATE = true)
    AND (p2.GOP_STATE = false)
ORDER BY date
;

-- A tale of two Countries, by county
WITH pivoted as (
  SELECT date, GOP_STATE, GOP_COUNTY, sum(cases) as cases, sum(deaths) as deaths
  FROM red_vs_blue_counties
  WHERE 1=1
  GROUP BY date, GOP_STATE, GOP_COUNTY
  ORDER BY GOP_STATE, GOP_COUNTY, date
)
SELECT p1.date
    , p1.cases/113.082294 as true_red_cases
    , p1.deaths/113.082294 as true_red_deaths
    , p2.cases/104.691469 as true_blue_cases
    , p2.deaths/104.691469 as true_blue_deaths
    , p3.cases/67.699837 as misplaced_blue_cases
    , p3.deaths/67.699837 as misplaced_blue_deaths
    , p4.cases/31.751677 as misplaced_red_cases
    , p4.deaths/31.751677 as misplaced_red_deaths
FROM pivoted p1
    INNER JOIN pivoted p2 ON p1.date = p2.date
    INNER JOIN pivoted p3 ON p1.date = p3.date
    INNER JOIN pivoted p4 ON p1.date = p4.date
WHERE 1=1
    AND (p1.GOP_STATE = true AND p1.GOP_COUNTY = true)
    AND (p2.GOP_STATE = false AND p2.GOP_COUNTY = false)
    AND (p3.GOP_STATE = true AND p3.GOP_COUNTY = false)
    AND (p4.GOP_STATE = false AND p4.GOP_COUNTY = true)
ORDER BY date
;
