-- We might only want to reorder the table the way that the column region comes after country. 
-- We would need to name all the columns from the staging table in the prep model's SELECT statement
-- Hint: You'll find the column names in DBeaver SELECT * from staging_airports;


WITH airports_reorder AS (
    SELECT faa
    	   , name
           , city
    	   , country
           , region
           , tz
           , dst
           , alt
           , lat
           , lon
    FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder