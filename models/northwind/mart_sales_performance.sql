/*
Goal: Create a summarized table for sales performance over time.

This file should:

Aggregate by order_year, order_month, and category_name

Show:

total revenue

total number of orders

average revenue per order (s.s.: make sure to divide by unique order id, otherwise it is average across rows rather than across orders)

This is the layer that a BI tool would use.

Hints:
query now from prep_sales model → use {{ ref('prep_sales') }}

Use sum(), count(distinct order_id), avg()

there is a shorter alternative to the GROUP BY first_column, second_column, third_column : instead you can use → GROUP BY 1, 2, 3

You can order the output for readability
*/


WITH sales AS ( 
	SELECT *
	FROM {{ref('prep_sales')}}
)
SELECT order_year
	   , order_month
	   , category_name
	   , ROUND(SUM(revenue), 2) AS total_revenue
	   , COUNT(order_id) AS total_n_of_orders
	   , ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2) AS avg_revenue_per_order
FROM sales
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3



-- as query for testing:
/*

WITH sales AS ( 
	SELECT *
	FROM prep_sales
)
SELECT order_year
	   , order_month
	   , category_name
	   , ROUND(SUM(revenue), 2) AS total_revenue
	   , COUNT(order_id) AS total_n_of_orders
	   , ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2) AS avg_revenue_per_order
FROM sales
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;

SELECT * FROM prep_sales

*/







