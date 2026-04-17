/*#### 5.1 Airport Stats

In a table `mart_faa_stats.sql` we want to see **for each airport over all time**:

- unique number of departures connections

- unique number of arrival connections

- how many flight were planned in total (departures & arrivals)

- how many flights were canceled in total (departures & arrivals)

- how many flights were diverted in total (departures & arrivals)

- how many flights actually occured in total (departures & arrivals)

- *(optional) how many unique airplanes travelled on average*

- *(optional) how many unique airlines were in service  on average* 

- add city, country and name of the airport*/


WITH 
departures AS ( -- airport as origin/departure
	SELECT origin
		   , COUNT(DISTINCT dest) AS n_unique_dests
		   , COUNT(sched_dep_time) AS dep_planned
		   , SUM(cancelled) AS dep_cancelled
		   , SUM(diverted) AS dep_diverted
		   , COUNT(sched_dep_time) - SUM(cancelled) AS dep_n_flights_calc 
		   , COUNT(CASE WHEN cancelled = 0 THEN 1 END) AS dep_n_flights_calc2 --two options for the same calc
		   , COUNT(DISTINCT tail_number) AS n_unique_tailnum_as_origin
		   , COUNT(DISTINCT airline) AS n_unique_airlines_as_origin
	FROM {{ref('prep_flights')}}
	GROUP BY origin
),
	arrivals AS ( -- airport as destination/arrival
	SELECT dest
		   , COUNT(DISTINCT origin) AS n_unique_origins
		   , COUNT(sched_dep_time) AS arr_planned
		   , SUM(cancelled) AS arr_cancelled
		   , SUM(diverted) AS arr_diverted
		   , COUNT(sched_arr_time) - SUM(cancelled)
		   , COUNT(sched_arr_time) - SUM(cancelled) AS arr_n_flights_calc 
		   , COUNT(CASE WHEN cancelled = 0 THEN 1 END) AS arr_n_flights_calc2 --two options for the same calc
		   , COUNT(DISTINCT tail_number) AS n_unique_tailnum_as_dest
		   , COUNT(DISTINCT airline) AS n_unique_airlines_as_dest
	FROM {{ref('prep_flights')}}
	GROUP BY dest
),
total_stats AS ( -- for practice, put both together
SELECT origin AS faa
	   , n_unique_dests
	   , n_unique_origins
	   , dep_planned + arr_planned AS total_planned
	   , dep_cancelled + arr_cancelled AS total_cancelled
	   , dep_diverted + arr_diverted AS total_diverted
	   , dep_n_flights_calc + arr_n_flights_calc AS total_n_flights_calc
	   , n_unique_tailnum_as_origin
	   , n_unique_tailnum_as_dest
	   , n_unique_airlines_as_origin
	   , n_unique_airlines_as_dest
FROM departures d 
JOIN arrivals a 
ON d.origin = a.dest 
)
SELECT pa.city
	   , pa.country
	   , pa.name
	   , ts.* 
FROM total_stats ts
JOIN {{ref('prep_airports')}} pa
USING (faa)




-- Here as straightforward query in case one wants to test:
/*WITH 
departures AS ( -- airport as origin/departure
	SELECT origin
		   , COUNT(DISTINCT dest) AS n_unique_dests
		   , COUNT(sched_dep_time) AS dep_planned
		   , SUM(cancelled) AS dep_cancelled
		   , SUM(diverted) AS dep_diverted
		   , COUNT(sched_dep_time) - SUM(cancelled) AS dep_n_flights_calc 
		   , COUNT(CASE WHEN cancelled = 0 THEN 1 END) AS dep_n_flights_calc2 --two options for the same calc
		   , COUNT(DISTINCT tail_number) AS n_unique_tailnum_as_origin
		   , COUNT(DISTINCT airline) AS n_unique_airlines_as_origin
	FROM prep_flights
	GROUP BY origin
),
	arrivals AS ( -- airport as destination/arrival
	SELECT dest
		   , COUNT(DISTINCT origin) AS n_unique_origins
		   , COUNT(sched_dep_time) AS arr_planned
		   , SUM(cancelled) AS arr_cancelled
		   , SUM(diverted) AS arr_diverted
		   , COUNT(sched_arr_time) - SUM(cancelled)
		   , COUNT(sched_arr_time) - SUM(cancelled) AS arr_n_flights_calc 
		   , COUNT(CASE WHEN cancelled = 0 THEN 1 END) AS arr_n_flights_calc2 --two options for the same calc
		   , COUNT(DISTINCT tail_number) AS n_unique_tailnum_as_dest
		   , COUNT(DISTINCT airline) AS n_unique_airlines_as_dest
	FROM prep_flights
	GROUP BY dest
),
total_stats AS ( -- for practice, put both together
SELECT origin AS faa
	   , n_unique_dests
	   , n_unique_origins
	   , dep_planned + arr_planned AS total_planned
	   , dep_cancelled + arr_cancelled AS total_cancelled
	   , dep_diverted + arr_diverted AS total_diverted
	   , dep_n_flights_calc + arr_n_flights_calc AS total_n_flights_calc
	   , n_unique_tailnum_as_origin
	   , n_unique_tailnum_as_dest
	   , n_unique_airlines_as_origin
	   , n_unique_airlines_as_dest
FROM departures d 
JOIN arrivals a 
ON d.origin = a.dest 
)
SELECT pa.city
	   , pa.country
	   , pa.name
	   , ts.* 
FROM total_stats ts
JOIN prep_airports pa
USING (faa)*/
