/*- Use `{{ source() }}` to pull data from the raw tables.

- Rename columns to snake_case.

- Cast dates and numbers to the correct types.

- Keep only relevant columns.*/


WITH source_data AS ( -- this source_data cte thing is apparently just dbt convention, according to Hilal
    SELECT *
    FROM {{ source('northwind_data', 'orders') }}
)
SELECT orderid AS order_id
	   , customerid AS customer_id
	   , employeeid AS employee_id
	   , orderdate::DATE AS order_date
	   , requireddate::DATE AS required_date
	   , shippeddate::DATE AS shipped_date
	   , shipvia AS ship_via
	   , freight
	   , shipname AS ship_name
	   , shipaddress AS ship_address
	   , shipcity AS ship_city
	   , shipregion AS ship_region
	   , shippostalcode AS ship_postal_code
	   , shipcountry AS ship_country
FROM source_data


-- as query for testing:

/*

SELECT orderid AS order_id
	   , customerid AS customer_id
	   , employeeid AS employee_id
	   , orderdate::DATE AS order_date
	   , requireddate::DATE AS required_date
	   , shippeddate::DATE AS shipped_date
	   , shipvia AS ship_via
	   , freight
	   , shipname AS ship_name
	   , shipaddress AS ship_address
	   , shipcity AS ship_city
	   , shipregion AS ship_region
	   , shippostalcode AS ship_postal_code
	   , shipcountry AS ship_country
FROM northwind.orders
;*/




