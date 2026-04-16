WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date               		-- only date (year-month-day) as DATE data type
		, timestamp::TIME AS time               		-- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour  		-- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMMon') AS month_name   	-- month name as a TEXT
        , TO_CHAR(timestamp, 'FMDy') AS weekday        	-- weekday name as TEXT        
		, DATE_PART('day', timestamp) AS date_day 		-- number of the day of month
		, DATE_PART('month', timestamp) AS date_month 	-- number of the month of year
		, DATE_PART('year', timestamp) AS date_year 		-- number of year
		, DATE_PART('week', timestamp) AS cw 
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE 
			WHEN time BETWEEN '22:00:00' AND '23:00:00' THEN 'night' -- given that the timestamp is measured in hourly steps, we can end on whole hours, s.s.
			WHEN time BETWEEN '00:00:00' AND '05:00:00' THEN 'night'
			WHEN time BETWEEN '06:00:00' AND '17:00:00'THEN 'day'
			WHEN time BETWEEN '18:00:00' AND '21:00:00' THEN 'evening'
		END) AS day_part
    FROM add_features
)
SELECT *
FROM add_more_features