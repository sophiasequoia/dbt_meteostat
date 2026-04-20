/*- Use `{{ source() }}` to pull data from the raw tables.

- Rename columns to snake_case.

- Cast dates and numbers to the correct types.

- Keep only relevant columns.*/


WITH source_data AS ( -- this source_data cte thing is apparently just dbt convention, according to Hilal
    SELECT *
    FROM {{ source('northwind', 'order_details') }}
)
SELECT orderid AS order_id
	   , productid AS product_id
	   , unitprice::NUMERIC AS unit_price
	   , quantity::INT AS quantity
	   , discount::NUMERIC
FROM source_data


-- as query for testing:
/*

SELECT * FROM northwind.order_details;

SELECT orderid AS order_id
	   , productid AS product_id
	   , unitprice::NUMERIC AS unit_price
	   , quantity::INT AS quantity
	   , discount::NUMERIC
FROM northwind.order_details
;
*/

