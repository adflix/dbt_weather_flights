-- In a table `mart_faa_stats.sql` we want to see **for each airport over all time**:

-- just checking source table
-- SELECT * FROM prep_flights
---------------------------------------------------
/* THE ACTUAL QUERY */

-- unique number of departures connections
WITH departures AS (
					SELECT origin AS faa
							,COUNT(origin) AS nunique_from 
							,COUNT(sched_dep_time) AS dep_planned -- how many flight were planned in total (departures)
							,SUM(cancelled) AS dep_cancelled -- how many flights were canceled in total (departures)
							,SUM(diverted) AS dep_diverted -- how many flights were diverted in total (departures)
							,COUNT(arr_time) AS dep_n_flights-- how many flights actually occured in total (departures)
							,COUNT(DISTINCT tail_number) AS dep_nunique_tails -- *(optional) how many unique airplanes travelled on average*
							,COUNT(DISTINCT airline) AS dep_nunique_airlines -- *(optional) how many unique airlines were in service  on average* 
					FROM {{ref('prep_flights')}}
					GROUP BY origin
),
-- unique number of arrival connections
arrivals AS (
					SELECT dest AS faa
							,COUNT(dest) AS nunique_to 
							,COUNT(sched_dep_time) AS arr_planned -- how many flight were planned in total (arrivals)
							,SUM(cancelled) AS arr_cancelled -- how many flights were canceled in total (arrivals)
							,SUM(diverted) AS arr_diverted -- how many flights were diverted in total (arrivals)
							,COUNT(arr_time)  AS arr_n_flights -- how many flights actually occured in total (arrivals)
							,COUNT(DISTINCT tail_number) AS arr_nunique_tails -- *(optional) how many unique airplanes travelled on average*
							,COUNT(DISTINCT airline) AS arr_nunique_airlines -- *(optional) how many unique airlines were in service  on average* 
					FROM {{ref('prep_flights')}}
					GROUP BY dest
),
total_stats AS (
					SELECT faa
							,nunique_to
							,nunique_from
							,dep_planned + arr_planned AS total_planed
							,dep_cancelled + arr_cancelled AS total_canceled
							,dep_diverted + arr_diverted AS total_diverted
							,dep_n_flights + arr_n_flights AS total_flights
					FROM departures
					JOIN arrivals
					-- ON arrivals.faa = departures.faa
					USING (faa)
)
-- add city, country and name of the airport
SELECT city  
		,country
		,name
		,total_stats.*
FROM {{ref('prep_flights')}}
RIGHT JOIN total_stats
USING (faa)
ORDER BY city;