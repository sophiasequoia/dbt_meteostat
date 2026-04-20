/*
Goal: Join your clean staging tables and calculate key business metrics.
This model will:

Join staging_orders, staging_order_details, and staging_products

Calculate new columns:

revenue = unit_price * quantity * (1 - discount)

order_year, order_month

(Optional) Add category_name by joining to staging_categories

Use jinja ref of staging_orders to reference your staging models.

You can join on order_id and product_id.

For order_year use:

EXTRACT(year FROM order_date) or
DATE_PART('year', order_date)
Keep only relevant columns!

This model should now start to look like a single “sales” dataset.
*/

SELECT o.order_id
	   , o.customer_id
	   , o.order_date
	   , p.product_id
--	   , p.supplier_id
	   , p.product_name
	   , p.category_id
	   , c.category_name
	   , p.unit_price
	   , d.quantity
	   , d.discount
	   , d.unit_price * d.quantity * (1 - d.discount) AS revenue
--	   , o.required_date
--	   , o.shipped_date
--	   , o.ship_via
--	   , o.ship_city
--	   , o.ship_country
--	   , o.employee_id
	   , DATE_PART('year', order_date)::TEXT AS order_year
	   , DATE_PART('month', order_date)::TEXT AS order_month
FROM {{ref('staging_orders')}} o
JOIN  {{ref('staging_order_details')}} d USING (order_id)
JOIN {{ref('staging_products')}} p USING (product_id)
LEFT JOIN {{ref('staging_categories')}} c USING (category_id)



-- as query for testing:
/*
SELECT o.order_id
	   , o.customer_id
	   , o.order_date
	   , p.product_id
	   , p.supplier_id
	   , p.product_name
	   , p.category_id
	   , c.category_name
	   , p.unit_price
	   , d.quantity
	   , d.discount
	   , d.unit_price * d.quantity * (1 - d.discount) AS revenue
	   , o.required_date
	   , o.shipped_date
	   , o.ship_via
	   , o.ship_city
	   , o.ship_country
	   , o.employee_id
	   , DATE_PART('year', order_date)::TEXT AS order_year
	   , DATE_PART('month', order_date)::TEXT AS order_month
FROM staging_orders o
JOIN staging_order_details d
USING (order_id)
JOIN staging_products p
USING (product_id)
LEFT JOIN staging_categories c
USING (category_id)
;*/


/*
 Solution from alex:
 
 WITH orders AS (
    SELECT * FROM {{ ref('staging_orders') }}
),
order_details as (
    SELECT * FROM {{ ref('staging_order_details') }}
),
products as (
    SELECT * FROM {{ ref('staging_products') }}
),
categories as (
    SELECT * FROM {{ ref('staging_categories') }}
),
joined as (
    SELECT
        o.order_id,
        o.customer_id,
        p.product_name,
        c.category_name,
        od.unit_price,
        od.quantity,
        od.discount,
        (od.unit_price * od.quantity * (1 - od.discount)) AS revenue,
        EXTRACT(year FROM  o.order_date) AS order_year,
        EXTRACT(month FROM o.order_date) AS order_month
    FROM orders o
    JOIN order_details od USING (order_id)
    JOIN products p USING (product_id)
    LEFT JOIN categories c USING (category_id)
)
SELECT * FROM joined
 */










