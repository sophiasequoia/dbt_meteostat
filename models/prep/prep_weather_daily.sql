-- In your prep directory create a file prep_weather_daily.sql that will add some information to your table created in the staging layer.

-- Our prep_weather_daily table wants to also include several informations derived from the "date" column.

-- Hint: use the DATE_PART and TO_CHAR functions.

-- We also want to add a column bucketizing the data in 4 seasons: winter, spring, summer, autumn.

-- Hint: use the CASE WHEN statements.

-- Use this skeleton and fill in the blanks ... to make it work.


WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
		, DATE_PART('day', date) AS date_day 		-- number of the day of month
		, DATE_PART('month', date) AS date_month 	-- number of the month of year
		, DATE_PART('year', date) AS date_year 		-- number of year
		, DATE_PART('week', date) AS cw 			-- number of the week of year
		, TO_CHAR(date, 'FMMon') AS month_name 	    -- name of the month
		, TO_CHAR(date, 'FMDy') AS weekday 		    -- name of the weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN month_name in ('Dec', 'Jan', 'Feb') THEN 'winter'
			WHEN month_name in ('Mar', 'Apr', 'May')  THEN 'spring'
            WHEN month_name in ('June', 'Jul', 'Aug')  THEN 'summer'
            WHEN month_name in ('Sep', 'Oct', 'Nov')  THEN 'autumn'
		END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date
