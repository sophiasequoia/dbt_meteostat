/*#### 5.2 Flight Route Stats

In a table `mart_route_stats.sql` we want to see **for each route over all time**:

- origin airport code
- destination airport code 
- total flights on this route
- unique airplanes (count of, s.s.)
- unique airlines (count of, s.s.)
- on average what is the actual elapsed time
- on average what is the delay on arrival
- what was the max delay?
- what was the min delay? (because it can be negative, s.s.)
- total number of cancelled 
- total number of diverted
- add city, country and name for both, origin and destination, airports*/


WITH routes AS (
	SELECT DISTINCT 
		   origin
		   , dest -- capture all distinct routes in dataset
		   , CONCAT(origin, '-->', dest) AS route -- create own column for all distinct routes in dataset
		   , COUNT(*) AS route_total_flight_count
		   , COUNT(DISTINCT tail_number) AS nunique_tailnum
		   , COUNT(DISTINCT airline) AS nunique_airline
		   , AVG(actual_elapsed_time_interval) AS avg_actual_elapsed_time_intvl
		   , AVG(dep_delay_interval) AS avg_dep_delay_intvl -- added this even though it wasnt actually asked for in task
		   , MAX(dep_delay_interval) AS max_dep_delay_intvl -- "-"
		   , MIN(dep_delay_interval) AS min_dep_delay_intvl -- "-" 
		   , AVG(arr_delay_interval) AS avg_arr_delay_intvl
		   , MAX(arr_delay_interval) AS max_arr_delay_intvl
		   , MIN(arr_delay_interval) AS min_arr_delay_intvl
		   , SUM(cancelled) AS total_cancelled
		   , SUM(diverted) AS total_diverted
	FROM {{ref('prep_flights')}}
	GROUP BY origin, dest
	ORDER BY origin
)
SELECT r.route
	   , r.origin
	   , ao.city AS orig_city
	   , ao.country AS orig_country
	   , ao.name AS orig_name
	   , r.dest
	   , ad.city AS dest_city
	   , ad.country AS dest_country
	   , ad.name AS dest_name
	   , r.route_total_flight_count
	   , r.nunique_tailnum
	   , r.nunique_airline
	   , r.avg_actual_elapsed_time_intvl
	   , r.avg_dep_delay_intvl
	   , r.max_dep_delay_intvl 
	   , r.min_dep_delay_intvl
	   , r.avg_arr_delay_intvl
	   , r.max_arr_delay_intvl
	   , r.min_arr_delay_intvl
	   , r.total_cancelled
	   , r.total_diverted
FROM routes r
JOIN {{ref('prep_airports')}} ao -- for join on flights ORIGIN column
ON r.origin = ao.faa
JOIN airports ad -- for join on flights DEST column
ON r.dest = ad.faa
ORDER BY r.route


-- Here as straightforward query if someone wants to test it:
/*WITH routes AS (
	SELECT DISTINCT 
		   origin
		   , dest -- capture all distinct routes in dataset
		   , CONCAT(origin, '-->', dest) AS route -- create own column for all distinct routes in dataset
		   , COUNT(*) AS route_total_flight_count
		   , COUNT(DISTINCT tail_number) AS nunique_tailnum
		   , COUNT(DISTINCT airline) AS nunique_airline
		   , AVG(actual_elapsed_time_interval) AS avg_actual_elapsed_time_intvl
		   , AVG(dep_delay_interval) AS avg_dep_delay_intvl -- added this even though it wasnt actually asked for in task
		   , MAX(dep_delay_interval) AS max_dep_delay_intvl -- "-"
		   , MIN(dep_delay_interval) AS min_dep_delay_intvl -- "-" 
		   , AVG(arr_delay_interval) AS avg_arr_delay_intvl
		   , MAX(arr_delay_interval) AS max_arr_delay_intvl
		   , MIN(arr_delay_interval) AS min_arr_delay_intvl
		   , SUM(cancelled) AS total_cancelled
		   , SUM(diverted) AS total_diverted
	FROM prep_flights
	GROUP BY origin, dest
	ORDER BY origin
)
SELECT r.route
	   , r.origin
	   , ao.city AS orig_city
	   , ao.country AS orig_country
	   , ao.name AS orig_name
	   , r.dest
	   , ad.city AS dest_city
	   , ad.country AS dest_country
	   , ad.name AS dest_name
	   , r.route_total_flight_count
	   , r.nunique_tailnum
	   , r.nunique_airline
	   , r.avg_actual_elapsed_time_intvl
	   , r.avg_dep_delay_intvl
	   , r.max_dep_delay_intvl 
	   , r.min_dep_delay_intvl
	   , r.avg_arr_delay_intvl
	   , r.max_arr_delay_intvl
	   , r.min_arr_delay_intvl
	   , r.total_cancelled
	   , r.total_diverted
FROM routes r
JOIN prep_airports ao -- for join on flights ORIGIN column
ON r.origin = ao.faa
JOIN prep_airports ad -- for join on flights DEST column
ON r.dest = ad.faa
ORDER BY r.route
;*/





