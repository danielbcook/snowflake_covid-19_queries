/* distance calculations based on the haversine formula here: http://www.movable-type.co.uk/scripts/latlong.html
-- go to line #90 for the temp tables needing a rebuild upon new COVID data

-- unless the demographic data changes, this table doesn't need to be rebuilt
-- nor is it actually used in JOIN statements elsewhere!
DROP TABLE IF EXISTS GEO_DATA.PUBLIC.pop_center_nearest_county;
CREATE TEMPORARY TABLE GEO_DATA.PUBLIC.pop_center_nearest_county as
	(
	  WITH county_pop_center_matrix as
	  (
	    WITH pop_centers as
	        (
	          SELECT DISTINCT
							state_code
							, CASE 
								WHEN (state_code = 'NY' AND REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', '') IN ('New York','Kings','Bronx','Richmond','Queens')) THEN 'New York City'
								ELSE REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', '')
								END as county
							, SUM(population) as total_population
							, AVG(latitude) as latitude, AVG(longitude) as longitude
	          FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo
	          GROUP BY 1,2
	          HAVING total_population >= 1000000
					)
	    SELECT DISTINCT
				demo.state_name as state
				, CASE 
					WHEN (demo.state_code = 'NY' AND REPLACE(REPLACE(demo.COUNTY, ' County', ''),' Parish', '') IN ('New York','Kings','Bronx','Richmond','Queens')) THEN 'New York City'
					ELSE REPLACE(REPLACE(demo.COUNTY, ' County', ''),' Parish', '')
					END as county
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
	);
    
-- unless Walmart location data or ZIP geolocation data is reloaded, this table doesn't need to be rebuilt
DROP TABLE IF EXISTS GEO_DATA.PUBLIC.walmart_geodata_matrix;
CREATE TEMPORARY TABLE GEO_DATA.PUBLIC.walmart_geodata_matrix as
(
  SELECT demo.state_code
    , CASE 
      WHEN (state_code = 'NY' AND REPLACE(REPLACE(demo.COUNTY, ' County', ''),' Parish', '') IN ('New York','Kings','Bronx','Richmond','Queens')) THEN 'New York City'
      ELSE REPLACE(REPLACE(demo.COUNTY, ' County', ''),' Parish', '')
      END as county
    , wmz.ZIP, demo.ZIP as target_zip, zgd.latitude, zgd.longitude
    , 7922 * atan2(sqrt(SQUARE(sin(((zgd.latitude - demo.latitude) * pi()/180)/2)) + cos(demo.latitude * pi()/180) * cos(zgd.latitude * pi()/180) *  SQUARE(sin(((zgd.longitude - demo.longitude) * pi()/180)/2))), sqrt(1-SQUARE(sin(((zgd.latitude - demo.latitude) * pi()/180)/2)) + cos(demo.latitude * pi()/180) * cos(zgd.latitude * pi()/180) *  SQUARE(sin(((zgd.longitude - demo.longitude) * pi()/180)/2)))) as distance
  FROM GEO_DATA.PUBLIC.WALMART_ZIPS wmz
      INNER JOIN GEO_DATA.PUBLIC.ZIP_GEODATA zgd ON zgd.ZIP = wmz.ZIP
      FULL OUTER JOIN GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo
  WHERE distance <= 150 -- there is no location in the lower 48 more than 150 miles from a Walmart!
);

-- unless TJs location data or ZIP geolocation data is reloaded, this table doesn't need to be rebuilt
DROP TABLE IF EXISTS GEO_DATA.PUBLIC.tjs_geodata_matrix;
CREATE TEMPORARY TABLE GEO_DATA.PUBLIC.tjs_geodata_matrix as
(
  SELECT demo.state_code
    , CASE 
      WHEN (state_code = 'NY' AND REPLACE(REPLACE(demo.COUNTY, ' County', ''),' Parish', '') IN ('New York','Kings','Bronx','Richmond','Queens')) THEN 'New York City'
      ELSE REPLACE(REPLACE(demo.COUNTY, ' County', ''),' Parish', '')
      END as county
    , tjs.ZIP, demo.ZIP as target_zip, zgd.latitude, zgd.longitude
    , 7922 * atan2(sqrt(SQUARE(sin(((zgd.latitude - demo.latitude) * pi()/180)/2)) + cos(demo.latitude * pi()/180) * cos(zgd.latitude * pi()/180) *  SQUARE(sin(((zgd.longitude - demo.longitude) * pi()/180)/2))), sqrt(1-SQUARE(sin(((zgd.latitude - demo.latitude) * pi()/180)/2)) + cos(demo.latitude * pi()/180) * cos(zgd.latitude * pi()/180) *  SQUARE(sin(((zgd.longitude - demo.longitude) * pi()/180)/2)))) as distance
  FROM GEO_DATA.PUBLIC.TJS_ZIPS tjs
      INNER JOIN GEO_DATA.PUBLIC.ZIP_GEODATA zgd ON zgd.ZIP = tjs.ZIP
      FULL OUTER JOIN GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo
  WHERE distance <= 600 -- there is no location in the lower 48 more than about 600 miles from a TJs
);

-- SELECT COUNT(*) FROM GEO_DATA.PUBLIC.WALMART_GEODATA_MATRIX  LIMIT 100
-- 4241867 rows with <= 150 criteria
-- SELECT COUNT(*) FROM GEO_DATA.PUBLIC.tjs_geodata_matrix  LIMIT 100
-- 3281977 rows with <= 600 criteria

-------------------------------------------------------------------------------------
-- beginning of tables which should be rebuilt when new COVID data has been loaded --
-------------------------------------------------------------------------------------
DROP TABLE IF EXISTS GEO_DATA.PUBLIC.county_hotspots;
CREATE TEMPORARY TABLE GEO_DATA.PUBLIC.county_hotspots as
 (
    WITH daily_percentages as
    (
      WITH county_date_matrix as
      (
            WITH all_dates as
            (
                SELECT DISTINCT date
                FROM COVID.PUBLIC.CT_US_COVID_TESTS
            ),
            counties as
            (
              SELECT DISTINCT
								state_name as state
								, CASE 
									WHEN (state_code = 'NY' AND REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', '') IN ('New York','Kings','Bronx','Richmond','Queens')) THEN 'New York City'
									ELSE REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', '')
									END as county
								, state_code
								FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo              
            )
            SELECT DISTINCT all_dates.date, c.state_code, c.county
            FROM all_dates
            	FULL OUTER JOIN counties c
            ORDER BY c.state_code, c.county, date desc
      ),
        county_population as
				(
					SELECT DISTINCT
						state_code
						, CASE 
							WHEN (state_code = 'NY' AND REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', '') IN ('New York','Kings','Bronx','Richmond','Queens')) THEN 'New York City'
							ELSE REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', '')
							END as county
						, SUM(population) as total
						FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo
						GROUP BY 1,2
				)
      SELECT
      	cdm1.state_code
				, cdm1.county
				, cdm1.date
				, IFNULL(nyt1.CASES,0) as cases
				, IFNULL(nyt1.CASES_SINCE_PREV_DAY,0) as new_cases
				, IFNULL(nyt1.DEATHS,0) as deaths
				, IFNULL(nyt1.DEATHS_SINCE_PREV_DAY,0) as new_deaths
				, county_population.total as total_population
				, new_cases + new_deaths as arithmetic_daily_new_events
				, CASE 
			    WHEN IFNULL(nyt1.CASES,0) = 0 THEN 0.0
			    ELSE IFNULL(nyt1.CASES_SINCE_PREV_DAY,0) / nyt1.CASES
			    END as daily_case_change_percent
				, CASE 
			    WHEN IFNULL(nyt1.DEATHS,0) = 0 THEN 0.0
			    ELSE IFNULL(nyt1.DEATHS_SINCE_PREV_DAY,0) / nyt1.DEATHS
			    END as daily_death_change_percent
				, IFNULL(nyt1.CASES_SINCE_PREV_DAY,0)/county_population.total as relative_case_growth
				, IFNULL(nyt1.DEATHS_SINCE_PREV_DAY,0)/county_population.total as relative_death_growth
				, daily_case_change_percent + daily_death_change_percent as arithmetic_daily_change_percent
				, daily_case_change_percent * daily_death_change_percent as geometric_daily_change_percent
				, relative_case_growth + relative_death_growth as arithmetic_relative_change_percent
				, relative_case_growth * (relative_death_growth) as geometric_relative_change_percent
				FROM county_date_matrix cdm1
				  LEFT OUTER JOIN COVID.PUBLIC.NYT_US_COVID19 nyt1 ON (nyt1.date = cdm1.date AND nyt1.ISO3166_2 = cdm1.state_code AND REPLACE(REPLACE(nyt1.county, ' County', ''),' Parish', '') = cdm1.county)
				  LEFT OUTER JOIN county_population ON (county_population.state_code = cdm1.state_code AND county_population.county = cdm1.county)
    )
		SELECT DISTINCT 
			daily_percentages.state_code
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

-- Trader Joe's closest to a hotspot
DROP TABLE IF EXISTS GEO_DATA.PUBLIC.tjs_hotspot_proximity;
CREATE TEMPORARY TABLE GEO_DATA.PUBLIC.tjs_hotspot_proximity as
SELECT DISTINCT county_hotspots.state_code
  , county_hotspots.county
  , tgm.ZIP
  ,(select min(distance) FROM GEO_DATA.PUBLIC.tjs_geodata_matrix tg2 WHERE tg2.state_code = tgm.state_code AND tg2.county = tgm.county) as min_distance_tj
FROM GEO_DATA.PUBLIC.county_hotspots
    INNER JOIN GEO_DATA.PUBLIC.tjs_geodata_matrix tgm ON (tgm.state_code = county_hotspots.state_code AND tgm.county = county_hotspots.county)
WHERE tgm.distance = min_distance_tj
ORDER BY min_distance_tj desc;

-- SELECT * FROM GEO_DATA.PUBLIC.tjs_hotspot_proximity ORDER BY min_distance_tj desc

-- Walmart closest to a hotspot
DROP TABLE IF EXISTS GEO_DATA.PUBLIC.wm_hotspot_proximity;
CREATE TEMPORARY TABLE GEO_DATA.PUBLIC.wm_hotspot_proximity as
SELECT DISTINCT
	county_hotspots.state_code
	, county_hotspots.county
	, wgm.ZIP
	,(select min(distance) FROM GEO_DATA.PUBLIC.walmart_geodata_matrix wg2 WHERE wg2.state_code = wgm.state_code AND wg2.county = wgm.county) as min_distance_walmart
FROM GEO_DATA.PUBLIC.county_hotspots
    INNER JOIN GEO_DATA.PUBLIC.walmart_geodata_matrix wgm ON (wgm.state_code = county_hotspots.state_code AND wgm.county = county_hotspots.county)
WHERE wgm.distance = min_distance_walmart
ORDER BY min_distance_walmart desc;

-- SELECT * FROM GEO_DATA.PUBLIC.wm_hotspot_proximity WHERE state_code <> 'AK' ORDER BY min_distance_walmart desc

-------------------------------
-- calculate recent hotspots --
-------------------------------
DROP TABLE IF EXISTS GEO_DATA.PUBLIC.recent_hotspots;
CREATE TEMPORARY TABLE GEO_DATA.PUBLIC.recent_hotspots as
SELECT DISTINCT
	county_hotspots.county
	, county_hotspots.state_code
	, county_hotspots.ARITHMETIC_RELATIVE_CHANGE_PERCENT
	, county_hotspots.Date
	, county_hotspots.cases
	, county_hotspots.new_cases
	, county_hotspots.deaths
	, county_hotspots.new_deaths
	, county_hotspots.total_population
	, county_hotspots.ARITHMETIC_DAILY_NEW_EVENTS
	, thp.ZIP as tj_zip
	, thp.min_distance_tj
	, whp.ZIP as wm_zip
	, whp.min_distance_walmart
	, CASE 
		WHEN thp.min_distance_tj <= whp.min_distance_walmart THEN 1
		ELSE 0
		END as which_is_closer
FROM GEO_DATA.PUBLIC.county_hotspots
--	INNER JOIN GEO_DATA.PUBLIC.pop_center_nearest_county cnt ON (cnt.state = county_hotspots.state_code AND cnt.county = county_hotspots.county)
    INNER JOIN GEO_DATA.PUBLIC.tjs_hotspot_proximity thp ON thp.state_code = county_hotspots.state_code AND thp.county = county_hotspots.county
    INNER JOIN GEO_DATA.PUBLIC.wm_hotspot_proximity whp ON whp.state_code = county_hotspots.state_code AND whp.county = county_hotspots.county
WHERE arithmetic_relative_change_percent >= 0.0005
ORDER BY
	  arithmetic_relative_change_percent desc -- 1st tiebreaker
	, 1, 2
LIMIT 1000;

*/

-- which date has the most "weight" among hotspot calculations?
SELECT DATE, SUM(arithmetic_relative_change_percent)
FROM GEO_DATA.PUBLIC.recent_hotspots
GROUP BY 1 ORDER BY 2 desc
;

-- which state has the most "weight" among hotspot calculations?
SELECT state_code, SUM(arithmetic_relative_change_percent)
FROM GEO_DATA.PUBLIC.recent_hotspots
WHERE recent_hotspots.date >= '2020-04-19' -- for the tab titled 'Hotspots > .0005, late April'
GROUP BY 1 ORDER BY 2 desc
;

-- data for the spreadsheet tabs in 'county_hotspots.xlsx'
SELECT DISTINCT state_code, county, min_distance_walmart, min_distance_tj, which_is_closer
FROM GEO_DATA.PUBLIC.recent_hotspots
WHERE 1=1
--	AND recent_hotspots.date <= '2020-04-05' -- for the tab titled 'Hotspots > .0005, early April'
--	AND recent_hotspots.date >= '2020-04-19' -- for the tab titled 'Hotspots > .0005, late April'
ORDER BY 1, 2
LIMIT 1000;

-- pick some of these to fill the table in the Google doc
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
--	AND recent_hotspots.date < '2020-04-01' -- for the tab titled 'Hotspots > .0005, early April'
ORDER BY ARITHMETIC_RELATIVE_CHANGE_PERCENT desc
LIMIT 100;

-- data for the choropleth tabs
SELECT
	CASE 
		WHEN state_code = 'LA' THEN CONCAT(county,' Parish')
		ELSE CONCAT(county,' County')
		END as county
	, state_code
	, SUM(arithmetic_relative_change_percent)
	, MAX(cases)
	, MAX(new_cases)
	, MAX(deaths)
	, MAX(new_deaths)
	, total_population
FROM GEO_DATA.PUBLIC.recent_hotspots
GROUP BY 1,2,8
ORDER BY SUM(arithmetic_relative_change_percent) desc
;

-- US counties by population
SELECT state_code, COUNTY, SUM(population)
FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo
GROUP BY state_code, COUNTY
HAVING SUM(population) > 0
ORDER BY 3
LIMIT 100
;

-- average county population, by state
WITH county_populations as
(
  SELECT DISTINCT
		state_code
		, CASE 
			WHEN (state_code = 'NY' AND REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', '') IN ('New York','Kings','Bronx','Richmond','Queens')) THEN 'New York City'
			ELSE REPLACE(REPLACE(COUNTY, ' County', ''),' Parish', '')
			END as county
		, SUM(population) as total
  FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo
  GROUP BY 1,2
)
SELECT state_code, AVG(total)
FROM county_populations
GROUP BY state_code ORDER BY state_code
;

-- checking latest dates from NYT_US_COVID19 table
SELECT date, COUNT(*)
FROM COVID.PUBLIC.NYT_US_COVID19
GROUP BY 1
ORDER BY date desc
LIMIT 5
;

-- checking latest dates from CT_US_COVID_TESTS table
SELECT date, COUNT(*)
FROM COVID.PUBLIC.CT_US_COVID_TESTS
GROUP BY 1
ORDER BY date desc
LIMIT 5
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
  WHERE target_zip = '34661'
  ORDER BY distance
  LIMIT 1
)
  SELECT COUNT(DISTINCT radius.zip)
  FROM GEO_DATA.PUBLIC.walmart_geodata_matrix radius
    INNER JOIN NEAREST_TJ ON NEAREST_TJ.target_ZIP = radius.target_zip
  WHERE radius.distance <= NEAREST_TJ.distance
;

-- closest large metro area to a given state
SELECT *
FROM GEO_DATA.PUBLIC.pop_center_nearest_county pcnc
	INNER JOIN GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE demo ON (demo.state_name = pcnc.state AND demo.county = pcnc.county)
WHERE demo.state_name = 'Puerto Rico'
ORDER BY distance 
;
