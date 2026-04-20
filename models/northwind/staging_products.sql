/*- Use `{{ source() }}` to pull data from the raw tables.

- Rename columns to snake_case.

- Cast dates and numbers to the correct types.

- Keep only relevant columns.*/

-- this source_data cte thing is apparently just dbt convention, according to Hilal
WITH source_data AS (
    SELECT *
    FROM {{source('northwind_data', 'products')}}
)
SELECT productid AS product_id
	   , productname AS product_name
	   , supplierid AS supplier_id
	   , categoryid AS category_id
	   , quantityperunit AS quantity_per_unit
	   , unitprice::NUMERIC AS unit_price
	   , unitsinstock::INT AS units_in_stock
	   , unitsonorder::INT AS units_on_order
	   , reorderlevel AS reorder_level
	   , discontinued
FROM source_data



-- as query for testing:
/*

SELECT productid AS product_id
	   , productname AS product_name
	   , supplierid AS supplier_id
	   , categoryid AS category_id
	   , quantityperunit AS quantity_per_unit
	   , unitprice::NUMERIC AS unit_price
	   , unitsinstock::INT AS units_in_stock
	   , unitsonorder::INT AS units_on_order
	   , reorderlevel AS reorder_level
	   , discontinued
FROM northwind.products
;
*/



