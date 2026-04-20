/*#### 5.4 Weekly weather

In a table `mart_weather_weekly.sql` we want to see **all** weather stats from the `prep_weather_daily` model aggregated weekly. 

- consider whether the metric should be Average, Maximum, Minimum, Sum or [Mode](https://wiki.postgresql.org/wiki/Aggregate_Mode)*/


-- could think about joining to prep_weather_hourly to aggregate wind stuff which doesn't show up in daily table, s.s.



SELECT airport_code AS faa
	   , DATE_TRUNC('week', date)::DATE AS week_of
	   , ROUND(AVG(avg_temp_c), 2) AS avg_avg_temp_c
	   , ROUND(AVG(min_temp_c), 2) AS avg_min_temp_c
	   , ROUND(AVG(max_temp_c), 2) AS avg_max_temp_c
	   , SUM(precipitation_mm) AS sum_precip_mm
	   , ROUND(SUM(max_snow_mm), 2) AS sum_max_snow_mm --> not sure if this makes sense as SUM... would need to check how measurement works compared to hourly
	   , ROUND(AVG(avg_wind_direction), 2) AS avg_avg_wind_direction --> only nulls on DAILY weather table, should consider joining to HOURLY weather table
	   , ROUND(AVG(avg_wind_speed_kmh), 2) AS avg_avg_wind_speed_kmh 
	   , MAX(wind_peakgust_kmh) AS max_wind_peakgust_kmh
	   , ROUND(AVG(avg_pressure_hpa), 2) AS avg_avg_pressure_hpa
	   , SUM(sun_minutes) AS sum_sun_minutes --> only nulls?
FROM {{ref('prep_weather_daily')}}
GROUP BY faa, week_of
ORDER BY faa, week_of
;


-- as regular query for testing:
/*

SELECT airport_code AS faa
	   , DATE_TRUNC('week', date)::DATE AS week_of
	   , ROUND(AVG(avg_temp_c), 2) AS avg_avg_temp_c
	   , ROUND(AVG(min_temp_c), 2) AS avg_min_temp_c
	   , ROUND(AVG(max_temp_c), 2) AS avg_max_temp_c
	   , SUM(precipitation_mm) AS sum_precip_mm
	   , ROUND(SUM(max_snow_mm), 2) AS sum_max_snow_mm --> not sure if this makes sense as SUM... would need to check how measurement works compared to hourly
	   , ROUND(AVG(avg_wind_direction), 2) AS avg_avg_wind_direction --> only nulls on DAILY weather table, should consider joining to HOURLY weather table
	   , ROUND(AVG(avg_wind_speed_kmh), 2) AS avg_avg_wind_speed_kmh 
	   , MAX(wind_peakgust_kmh) AS max_wind_peakgust_kmh
	   , ROUND(AVG(avg_pressure_hpa), 2) AS avg_avg_pressure_hpa
	   , SUM(sun_minutes) AS sum_sun_minutes --> only nulls?
FROM prep_weather_daily
GROUP BY faa, week_of
ORDER BY faa, week_of
;

*/




















