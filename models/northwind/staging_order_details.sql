
WITH source_data AS ( -- this source_data cte thing is apparently just dbt convention, according to Hilal
    SELECT *
    FROM {{source('northwind_data', 'order_details')}}
)
SELECT orderid AS order_id
	   , productid AS product_id
	   , unitprice::NUMERIC AS unit_price
	   , quantity::INT AS quantity
	   , discount::NUMERIC
FROM source_data


