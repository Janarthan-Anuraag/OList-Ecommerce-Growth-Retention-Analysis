CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE raw.orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(30),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

SELECT * FROM raw.orders LIMIT 5;
SELECT * FROM raw.orders LIMIT 5

CREATE TABLE raw.order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price NUMERIC,
    freight_value NUMERIC
);

SELECT COUNT(*) FROM raw.order_items;
SELECT COUNT(DISTINCT order_id) FROM raw.order_items;



SELECT COUNT(*) AS distinct_orders_in_orders
FROM raw.orders;

SELECT COUNT(*) AS distinct_orders_in_order_items
FROM raw.order_items;

SELECT
    order_id,
    COUNT(*) AS item_count
FROM raw.order_items
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY item_count DESC
LIMIT 10;



CREATE TABLE raw.customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);

SELECT COUNT(*) FROM raw.customers;



CREATE TABLE raw.products (
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

SELECT COUNT(*) FROM raw.products;

CREATE TABLE raw.sellers (
    seller_id VARCHAR(50),
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);
SELECT COUNT(*) FROM raw.sellers;

---Module 1: Data Modelling and creating tables---

--STEP 1:--
--Create analytics.fact_order_items

CREATE TABLE analytics.fact_order_items (
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date DATE,
    item_revenue NUMERIC,
    freight_value NUMERIC
);

--Populate fact_order_items
INSERT INTO analytics.fact_order_items
SELECT
    oi.order_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,
    DATE(o.order_purchase_timestamp) AS order_date,
    oi.price AS item_revenue,
    oi.freight_value
FROM raw.order_items oi
JOIN raw.orders o
    ON oi.order_id = o.order_id;
--One row represents one product in one order.
--check:
SELECT COUNT(*) FROM analytics.fact_order_items;
SELECT *
FROM analytics.fact_order_items
LIMIT 5;


--STEP 2--
--Create analytics.dim_customers

CREATE TABLE analytics.dim_customers AS
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM raw.customers;


--Create analytics.dim_products

CREATE TABLE analytics.dim_products AS
SELECT DISTINCT
    product_id,
    product_category_name,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM raw.products;

--Create analytics.dim_sellers

CREATE TABLE analytics.dim_sellers AS
SELECT DISTINCT
    seller_id,
    seller_city,
    seller_state
FROM raw.sellers;


--Check:
SELECT COUNT(*) FROM analytics.dim_customers;
SELECT COUNT(*) FROM analytics.dim_products;
SELECT COUNT(*) FROM analytics.dim_sellers;


---Module 2: Business KPIs and SQL Analysis---


--(A): Growth & Revenue

--KPI 1: Total Revenue(Foundation KPI)

SELECT SUM(item_revenue) AS total_revenue
FROM analytics.fact_order_items

--KPI 2: Total Orders, Customers, & AOV

--total orders
SELECT COUNT(DISTINCT order_id) AS total_orders
FROM raw.orders

--total customers
SELECT COUNT(DISTINCT customer_id) AS total_customers
FROM raw.orders

--Average Order Value (AOV): Total Revenue/Total Orders
WITH revenue AS
    (SELECT SUM(item_revenue) AS total_revenue
    FROM analytics.fact_order_items),
orders AS 
    (SELECT COUNT(DISTINCT order_id) AS total_orders
    FROM raw.orders)
SELECT Round(total_revenue / total_orders,2) AS average_order_value
FROM revenue, orders

--KPI 3: Time-Based Analysis: Revenue & Orders Over Time(Monthly)

--Monthly revenue trend
SELECT DATE_TRUNC('month', order_date) AS month, SUM(item_revenue) AS monthly_revenue
FROM analytics.fact_order_items
GROUP BY month
ORDER BY month

--Orders over time(Monthly)
SELECT DATE_TRUNC('month', order_purchase_timestamp) AS month,
       COUNT(DISTINCT order_id) AS monthly_orders
FROM raw.orders
GROUP BY month
ORDER BY month

--Combining monthly revenue and orders 
WITH monthly_revenue AS 
    (SELECT DATE_TRUNC('month', order_date) AS month, SUM(item_revenue) AS revenue
    FROM analytics.fact_order_items
    GROUP BY month),
monthly_orders AS 
    (SELECT DATE_TRUNC('month', order_purchase_timestamp) AS month,
            COUNT(DISTINCT order_id) AS orders
    FROM raw.orders
    GROUP BY month)
SELECT r.month,r.revenue,o.orders,r.revenue / o.orders AS avg_order_value
FROM monthly_revenue r
JOIN monthly_orders o
    ON r.month = o.month
ORDER BY r.month

--(B): Customer Behaviour
---OLIST anonymized customer data to protect privacy.So they:
--Re-generated customer_id for each order/address
--Preserved customer_unique_id to track real customers

--KPI 1: Count of orders by Repeat vs one-time customers
WITH customer_orders AS 
    (SELECT c.customer_unique_id, COUNT(DISTINCT o.order_id) AS order_count
    FROM raw.orders o
    JOIN raw.customers c
        ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id)
SELECT CASE WHEN order_count = 1 THEN 'One-time' ELSE 'Repeat'
       END AS customer_type,
       COUNT(*) AS customer_count
FROM customer_orders
GROUP BY customer_type

--KPI 2: Revenue by customer type
WITH customer_orders AS 
    (SELECT c.customer_unique_id, COUNT(DISTINCT o.order_id) AS order_count
    FROM raw.orders o
    JOIN raw.customers c
        ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id),
customer_type AS (
    SELECT customer_unique_id,
        CASE WHEN order_count = 1 THEN 'One-time' ELSE 'Repeat'
        END AS customer_type
    FROM customer_orders)
SELECT ct.customer_type, SUM(foi.item_revenue) AS total_revenue
FROM analytics.fact_order_items foi
JOIN raw.customers c
    ON foi.customer_id = c.customer_id
JOIN customer_type ct
    ON c.customer_unique_id = ct.customer_unique_id
GROUP BY ct.customer_type

--Customer behaviour overtime: when and how customers repeat

--KPI 3: First Order date per customer
WITH customer_orders AS 
    (SELECT c.customer_unique_id, o.order_id,
           DATE(o.order_purchase_timestamp) AS order_date
    FROM raw.orders o
    JOIN raw.customers c
        ON o.customer_id = c.customer_id)
SELECT customer_unique_id, MIN(order_date) AS first_order_date
FROM customer_orders
GROUP BY customer_unique_id

--KPI 4: Days to repeat the order by each customer
WITH customer_orders AS
    (SELECT c.customer_unique_id, o.order_id, DATE(o.order_purchase_timestamp) AS order_date
    FROM raw.orders o
    JOIN raw.customers c
        ON o.customer_id = c.customer_id,
first_orders AS 
    (SELECT customer_unique_id, MIN(order_date) AS first_order_date
    FROM customer_orders
    GROUP BY customer_unique_id),
repeat_orders AS 
    (SELECT co.customer_unique_id, co.order_date, fo.first_order_date,
        co.order_date - fo.first_order_date AS days_to_repeat
    FROM customer_orders co
    JOIN first_orders fo
        ON co.customer_unique_id = fo.customer_unique_id
    WHERE co.order_date > fo.first_order_date)

SELECT days_to_repeat, COUNT(*) AS repeat_order_count
FROM repeat_orders
GROUP BY days_to_repeat
ORDER BY days_to_repeat

--KPI 5: Bucketing the reorder counts 
WITH customer_orders AS 
    (SELECT c.customer_unique_id, DATE(o.order_purchase_timestamp) AS order_date
    FROM raw.orders o
    JOIN raw.customers c ON o.customer_id = c.customer_id),
first_orders AS (
    (SELECT customer_unique_id, MIN(order_date) AS first_order_date
    FROM customer_orders
    GROUP BY customer_unique_id),
repeat_orders AS 
    (SELECT co.customer_unique_id, co.order_date - fo.first_order_date AS days_to_repeat
    FROM customer_orders co
    JOIN first_orders fo ON co.customer_unique_id = fo.customer_unique_id
    WHERE co.order_date > fo.first_order_date)
	
SELECT CASE WHEN days_to_repeat <= 7 THEN '0–7 days'
        WHEN days_to_repeat <= 30 THEN '8–30 days'
        WHEN days_to_repeat <= 60 THEN '31–60 days'
        ELSE '60+ days' END AS repeat_bucket,
    COUNT(*) AS repeat_orders
FROM repeat_orders
GROUP BY repeat_bucket
ORDER BY repeat_orders DESC

--(C): Product Category & Repeat behaviour analysis

--Which product categories are driving repeat behavior — and which are one-and-done?
--This matters because:
--Some categories naturally encourage repeat purchases
--Others are acquisition-only and need different strategies
--Treating all categories the same is a growth mistake


--KPI 1: Customer Type (One-Time vs Repeat) — Reusable Logic


WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM raw.orders o
    JOIN raw.customers c
        ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
),
customer_type AS (
    SELECT
        customer_unique_id,
        CASE
            WHEN order_count = 1 THEN 'One-time'
            ELSE 'Repeat'
        END AS customer_type
    FROM customer_orders
)
SELECT *
FROM customer_type
LIMIT 5;

--KPI 2: Repeat Orders by Product Categories
WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM raw.orders o
    JOIN raw.customers c
        ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
),
customer_type AS (
    SELECT
        customer_unique_id,
        CASE
            WHEN order_count = 1 THEN 'One-time'
            ELSE 'Repeat'
        END AS customer_type
    FROM customer_orders
)
SELECT
    p.product_category_name AS category,
    ct.customer_type,
    COUNT(*) AS order_item_count
FROM analytics.fact_order_items foi
JOIN raw.customers c
    ON foi.customer_id = c.customer_id
JOIN customer_type ct
    ON c.customer_unique_id = ct.customer_unique_id
JOIN analytics.dim_products p
    ON foi.product_id = p.product_id
GROUP BY p.product_category_name, ct.customer_type
ORDER BY order_item_count DESC;

--KPI 3: Repeat share by product categories
WITH customer_orders AS 
    (SELECT c.customer_unique_id, COUNT(DISTINCT o.order_id) AS order_count
    FROM raw.orders o
    JOIN raw.customers c ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id),
customer_type AS 
    (SELECT customer_unique_id,
        CASE WHEN order_count = 1 THEN 'One-time' ELSE 'Repeat' END AS customer_type
    FROM customer_orders),
category_orders AS 
    (SELECT p.product_category_name AS category, ct.customer_type, COUNT(*) AS order_items
    FROM analytics.fact_order_items foi
    JOIN raw.customers c
        ON foi.customer_id = c.customer_id
    JOIN customer_type ct
        ON c.customer_unique_id = ct.customer_unique_id
    JOIN analytics.dim_products p
        ON foi.product_id = p.product_id
    GROUP BY p.product_category_name, ct.customer_type)

SELECT
    category,
    SUM(order_items) AS total_order_items,
    SUM(CASE WHEN customer_type = 'Repeat' THEN order_items ELSE 0 END) AS repeat_items,
    ROUND(SUM(CASE WHEN customer_type = 'Repeat' THEN order_items ELSE 0 END)*100.0::NUMERIC
        / SUM(order_items),3) AS repeat_share_perc
FROM category_orders
GROUP BY category
ORDER BY repeat_share_perc DESC

--(D) : Revenue Concentration
--we want to know:
--Is our revenue diversified or concentrated?
--Are we over-dependent on a small set of categories/products?
--Where is the biggest business risk?


--KPI 1: Revenue by Product Category
WITH category_revenue AS 
    (SELECT p.product_category_name AS category, SUM(foi.item_revenue) AS revenue
    FROM analytics.fact_order_items foi
    JOIN analytics.dim_products p ON foi.product_id = p.product_id
    GROUP BY p.product_category_name),
total_revenue AS 
    (SELECT SUM(revenue) AS total_rev
    FROM category_revenue)
	
SELECT cr.category, cr.revenue,ROUND(cr.revenue / tr.total_rev, 4) AS revenue_share
FROM category_revenue cr
CROSS JOIN total_revenue tr
ORDER BY cr.revenue DESC

--KPI 2: Cumulative Revenue Share
WITH category_revenue AS 
    (SELECT p.product_category_name AS category, SUM(foi.item_revenue) AS revenue
    FROM analytics.fact_order_items foi
    JOIN analytics.dim_products p ON foi.product_id = p.product_id
    GROUP BY p.product_category_name),
ranked_categories AS 
    (SELECT category, revenue,
        SUM(revenue) OVER (ORDER BY revenue DESC) AS cumulative_revenue,
        SUM(revenue) OVER () AS total_revenue
    FROM category_revenue)
	
SELECT category,revenue,ROUND(cumulative_revenue / total_revenue, 4) AS cumulative_revenue_share
FROM ranked_categories
ORDER BY revenue DESC

--FILES FOR DASHBOARD
--1. Executive KPIs
WITH revenue AS 
    (SELECT SUM(item_revenue) AS total_revenue
    FROM analytics.fact_order_items),
orders AS 
    (SELECT COUNT(DISTINCT order_id) AS total_orders
    FROM raw.orders),
repeat_revenue AS 
    (SELECT SUM(foi.item_revenue) AS repeat_revenue
    FROM analytics.fact_order_items foi
    JOIN raw.customers c ON foi.customer_id = c.customer_id
    JOIN (SELECT c.customer_unique_id
        FROM raw.orders o
        JOIN raw.customers c
            ON o.customer_id = c.customer_id
        GROUP BY c.customer_unique_id
        HAVING COUNT(DISTINCT o.order_id) > 1) repeat_customers
        ON c.customer_unique_id = repeat_customers.customer_unique_id)
		
SELECT r.total_revenue,o.total_orders,
    Round(r.total_revenue / o.total_orders,2) AS average_order_value,
    Round((rr.repeat_revenue / r.total_revenue)*100.00,2) AS repeat_revenue_share_perc
FROM revenue r
CROSS JOIN orders o
CROSS JOIN repeat_revenue rr

--2. Monthly Growth Metrics
WITH monthly_revenue AS 
    (SELECT DATE_TRUNC('month', order_date)::DATE AS month, SUM(item_revenue) AS revenue
    FROM analytics.fact_order_items
    GROUP BY 1),
monthly_orders AS 
    (SELECT DATE_TRUNC('month', order_purchase_timestamp)::DATE AS month,COUNT(DISTINCT order_id) AS orders
    FROM raw.orders
    GROUP BY 1)

SELECT r.month, r.revenue, o.orders, Round(r.revenue / o.orders,2) AS average_order_value
FROM monthly_revenue r
JOIN monthly_orders o ON r.month = o.month
ORDER BY r.month

--3. Repeat Timing 
WITH customer_orders AS 
    (SELECT c.customer_unique_id,DATE(o.order_purchase_timestamp) AS order_date
    FROM raw.orders o
    JOIN raw.customers c ON o.customer_id = c.customer_id),
first_orders AS (
    SELECT
        customer_unique_id,
        MIN(order_date) AS first_order_date
    FROM customer_orders
    GROUP BY customer_unique_id
),
repeat_orders AS (
    SELECT co.customer_unique_id,co.order_date - fo.first_order_date AS days_to_repeat
    FROM customer_orders co
    JOIN first_orders fo ON co.customer_unique_id = fo.customer_unique_id
    WHERE co.order_date > fo.first_order_date)

SELECT CASE WHEN days_to_repeat <= 7 THEN '0–7 days'
        WHEN days_to_repeat <= 30 THEN '8–30 days'
        WHEN days_to_repeat <= 60 THEN '31–60 days'
        ELSE '60+ days' END AS repeat_bucket,
     COUNT(*) AS repeat_orders
FROM repeat_orders
GROUP BY repeat_bucket
ORDER BY repeat_orders DESC

--4. Repeat by product category

WITH customer_orders AS (
    SELECT c.customer_unique_id, COUNT(DISTINCT o.order_id) AS order_count
    FROM raw.orders o
    JOIN raw.customers c ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id),
customer_type AS (
    SELECT customer_unique_id,
        CASE WHEN order_count = 1 THEN 'One-time' ELSE 'Repeat' END AS customer_type
    FROM customer_orders),
category_orders AS 
    (SELECT p.product_category_name, ct.customer_type, COUNT(*) AS order_items
    FROM analytics.fact_order_items foi
    JOIN raw.customers c ON foi.customer_id = c.customer_id
    JOIN customer_type ct ON c.customer_unique_id = ct.customer_unique_id
    JOIN analytics.dim_products p ON foi.product_id = p.product_id
    GROUP BY p.product_category_name, ct.customer_type)
	
SELECT product_category_name, SUM(order_items) AS total_order_items,
    SUM(CASE WHEN customer_type = 'Repeat' THEN order_items ELSE 0 END) AS repeat_order_items,
    ROUND(SUM(CASE WHEN customer_type = 'Repeat' THEN order_items ELSE 0 END)*100.00 / SUM(order_items),2) AS repeat_share_percentage
FROM category_orders
GROUP BY product_category_name
ORDER BY repeat_share_percentage DESC

--5. Cateory Revenue Pareto
WITH category_revenue AS 
    (SELECT p.product_category_name, SUM(foi.item_revenue) AS revenue
    FROM analytics.fact_order_items foi
    JOIN analytics.dim_products p ON foi.product_id = p.product_id
    GROUP BY p.product_category_name),
ranked_categories AS 
    (SELECT product_category_name,revenue,
        SUM(revenue) OVER (ORDER BY revenue DESC) AS cumulative_revenue,
        SUM(revenue) OVER () AS total_revenue
     FROM category_revenue)
	 
SELECT product_category_name,revenue, 
       ROUND(cumulative_revenue*100.00 / total_revenue,2) AS cumulative_revenue_share_percentage
FROM ranked_categories
ORDER BY revenue DESC




















