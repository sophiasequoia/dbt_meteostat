/*- Use jinja source function to pull data from the raw tables.

- Rename columns to snake_case.

- Cast dates and numbers to the correct types.

- Keep only relevant columns.*/


WITH source_data AS ( -- this source_data cte thing is apparently just dbt convention, according to Hilal
    SELECT *
    FROM {{source('northwind_data', 'categories')}}
)
SELECT categoryid AS category_id
	   , categoryname AS category_name
	   , description
	   , picture
FROM source_data


-- as query for testing:
/*

SELECT * FROM northwind.categories;

SELECT categoryid AS category_id
	   , categoryname AS category_name
	   , description
	   , picture
FROM northwind.categories
;
*/
