-- Coffee Shop Sales Data Warehouse & Analysis
-- Author: Kartik Thakkar
-- Description: End-to-end SQL project including data cleaning, modeling and analytics

DROP DATABASE IF EXISTS coffee_shop_dw;
CREATE DATABASE coffee_shop_dw;
USE coffee_shop_dw;

SET FOREIGN_KEY_CHECKS = 0;

-- Drop tables if exist
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS staging_sales;

SET FOREIGN_KEY_CHECKS = 1;

-- 1. Staging Table (ALL VARCHAR to avoid load errors)
CREATE TABLE staging_sales (
    transaction_id VARCHAR(50),
    transaction_timestamp VARCHAR(50),
    store_id VARCHAR(50),
    city VARCHAR(50),
    country VARCHAR(50),
    store_type VARCHAR(50),
    product_category VARCHAR(50),
    product_name VARCHAR(100),
    unit_price VARCHAR(50),
    quantity VARCHAR(50),
    discount_applied VARCHAR(50),
    payment_method VARCHAR(50),
    customer_id VARCHAR(50),
    customer_age_group VARCHAR(50),
    customer_gender VARCHAR(50),
    loyalty_member VARCHAR(50),
    weather_condition VARCHAR(50),
    temperature_c VARCHAR(50)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/coffee_shop_sales.csv'
INTO TABLE staging_sales
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    @transaction_id,
    @transaction_timestamp,
    @store_id,
    @city,
    @country,
    @store_type,
    @product_category,
    @product_name,
    @unit_price,
    @quantity,
    @discount_applied,
    @payment_method,
    @customer_id,
    @customer_age_group,
    @customer_gender,
    @loyalty_member,
    @weather_condition,
    @temperature_c,
    @extra1, @extra2, @extra3, @extra4, @extra5
)
SET
    transaction_id = NULLIF(@transaction_id, ''),
    transaction_timestamp = NULLIF(@transaction_timestamp, ''),
    store_id = NULLIF(@store_id, ''),
    city = NULLIF(@city, ''),
    country = NULLIF(@country, ''),
    store_type = NULLIF(@store_type, ''),
    product_category = NULLIF(@product_category, ''),
    product_name = NULLIF(@product_name, ''),
    unit_price = NULLIF(@unit_price, ''),
    quantity = NULLIF(@quantity, ''),
    discount_applied = NULLIF(@discount_applied, ''),
    payment_method = NULLIF(@payment_method, ''),
    customer_id = NULLIF(@customer_id, ''),
    customer_age_group = NULLIF(@customer_age_group, ''),
    customer_gender = NULLIF(@customer_gender, ''),
    loyalty_member = NULLIF(@loyalty_member, ''),
    weather_condition = NULLIF(@weather_condition, ''),
    temperature_c = NULLIF(@temperature_c, '');

-- 3. Cleaned Dimensions

CREATE TABLE dim_store AS
SELECT DISTINCT
    CAST(store_id AS UNSIGNED) AS store_id,
    CONCAT(city, ' - ', store_type) AS store_location,
    city,
    country,
    store_type
FROM staging_sales
WHERE store_id REGEXP '^[0-9]+$';

ALTER TABLE dim_store ADD PRIMARY KEY (store_id);

CREATE TABLE dim_product (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2)
);

INSERT INTO dim_product (product_name, category, price)
SELECT DISTINCT
    product_name,
    product_category,
    CAST(unit_price AS DECIMAL(10,2))
FROM staging_sales
WHERE unit_price REGEXP '^[0-9.]+$';

CREATE TABLE dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    age_group VARCHAR(50),
    gender VARCHAR(50),
    loyalty_member VARCHAR(50)
);

INSERT INTO dim_customer (customer_id, age_group, gender, loyalty_member)
SELECT 
    customer_id,
    ANY_VALUE(customer_age_group),
    ANY_VALUE(customer_gender),
    ANY_VALUE(loyalty_member)
FROM staging_sales
GROUP BY customer_id;

CREATE TABLE dim_date AS
SELECT DISTINCT
    DATE(STR_TO_DATE(transaction_timestamp, '%Y-%m-%d %H:%i:%s')) AS date_id,
    DAY(STR_TO_DATE(transaction_timestamp, '%Y-%m-%d %H:%i:%s')) AS day,
    MONTH(STR_TO_DATE(transaction_timestamp, '%Y-%m-%d %H:%i:%s')) AS month,
    YEAR(STR_TO_DATE(transaction_timestamp, '%Y-%m-%d %H:%i:%s')) AS year,
    DAYNAME(STR_TO_DATE(transaction_timestamp, '%Y-%m-%d %H:%i:%s')) AS weekday,
    CASE 
        WHEN DAYOFWEEK(STR_TO_DATE(transaction_timestamp, '%Y-%m-%d %H:%i:%s')) IN (1,7)
        THEN TRUE ELSE FALSE
    END AS is_weekend
FROM staging_sales;

ALTER TABLE dim_date ADD PRIMARY KEY (date_id);

-- 4. Fact Table

CREATE TABLE fact_sales (
    transaction_id INT PRIMARY KEY,
    transaction_timestamp DATETIME,
    store_id INT,
    product_id INT,
    customer_id VARCHAR(50),
    quantity INT,
    total_amount DECIMAL(10,2)
);

INSERT INTO fact_sales (
    transaction_id,
    transaction_timestamp,
    store_id,
    product_id,
    customer_id,
    quantity,
    total_amount
)
SELECT 
    transaction_id,
    ANY_VALUE(transaction_timestamp),
    ANY_VALUE(store_id),
    ANY_VALUE(product_id),
    ANY_VALUE(customer_id),
    SUM(quantity),
    SUM(total_amount)
FROM (
    SELECT 
        CAST(s.transaction_id AS UNSIGNED) AS transaction_id,
        STR_TO_DATE(s.transaction_timestamp, '%Y-%m-%d %H:%i:%s') AS transaction_timestamp,
        CAST(s.store_id AS UNSIGNED) AS store_id,
        p.product_id,
        s.customer_id,
        CAST(s.quantity AS UNSIGNED) AS quantity,
        CAST(s.unit_price AS DECIMAL(10,2)) * CAST(s.quantity AS UNSIGNED) AS total_amount
    FROM staging_sales s
    JOIN dim_product p ON s.product_name = p.product_name
    WHERE 
        s.transaction_id REGEXP '^[0-9]+$'
        AND s.quantity REGEXP '^[0-9]+$'
        AND s.unit_price REGEXP '^[0-9.]+$'
) t
GROUP BY transaction_id;

-- 5. Indexing
CREATE INDEX idx_store ON fact_sales(store_id);
CREATE INDEX idx_product ON fact_sales(product_id);
CREATE INDEX idx_customer ON fact_sales(customer_id);

-- 6. Analytics

SELECT SUM(total_amount) AS total_revenue FROM fact_sales;

SELECT 
    p.product_name,
    SUM(f.total_amount) revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC;

SELECT 
    HOUR(transaction_timestamp) hour,
    SUM(total_amount) revenue
FROM fact_sales
GROUP BY hour
ORDER BY revenue DESC;

/*1. Total Revenue*/

SELECT SUM(total_amount) AS total_revenue FROM fact_sales;

/*2. Total Orders*/

SELECT COUNT(*) AS total_orders FROM fact_sales;

/*3. Average Order Value*/

SELECT AVG(total_amount) AS avg_order_value FROM fact_sales;

/*4. Top 5 Products*/

SELECT 
    p.product_name,
    SUM(f.total_amount) revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 5;

/*5.Monthly Revenue*/

SELECT 
    MONTH(transaction_timestamp) month,
    SUM(total_amount) revenue
FROM fact_sales
GROUP BY month;

/*6.Moving Average (7 days)*/

SELECT 
    DATE(transaction_timestamp) dt,
    SUM(total_amount) daily_sales,
    AVG(SUM(total_amount)) OVER 
    (
        ORDER BY DATE(transaction_timestamp)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) moving_avg
FROM fact_sales
GROUP BY dt;

/*7.Product Contribution %*/

SELECT 
    p.product_name,
    SUM(f.total_amount) revenue,
    ROUND(
        SUM(f.total_amount) * 100 / SUM(SUM(f.total_amount)) OVER (),
        2
    ) contribution_percent
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name;

/*8.Top Products with Ranking*/

SELECT 
    p.product_name,
    SUM(f.total_amount) revenue,
    RANK() OVER (ORDER BY SUM(f.total_amount) DESC) AS product_rank
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name;

/*9.Revenue Contribution %*/

SELECT 
    p.product_name,
    SUM(f.total_amount) revenue,
    ROUND(
        SUM(f.total_amount) * 100.0 /
        SUM(SUM(f.total_amount)) OVER (),
        2
    ) AS contribution_percent
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name;

/*10.Running Revenue Trend*/

SELECT 
    DATE(transaction_timestamp) dt,
    SUM(total_amount) daily_revenue,
    SUM(SUM(total_amount)) OVER (ORDER BY DATE(transaction_timestamp)) AS cumulative_revenue
FROM fact_sales
GROUP BY dt;

/*11.Customer Lifetime Value (CLV)*/

SELECT 
    customer_id,
    COUNT(*) total_orders,
    SUM(total_amount) lifetime_value,
    AVG(total_amount) avg_order_value
FROM fact_sales
GROUP BY customer_id
ORDER BY lifetime_value DESC;

/*12. Store Performance Ranking*/

SELECT 
    s.store_location,
    SUM(f.total_amount) revenue,
    RANK() OVER (ORDER BY SUM(f.total_amount) DESC) AS store_rank
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY s.store_location;

/*13.Peak vs Off-Peak Sales*/

SELECT 
    CASE 
        WHEN HOUR(transaction_timestamp) BETWEEN 8 AND 12 THEN 'Morning'
        WHEN HOUR(transaction_timestamp) BETWEEN 13 AND 17 THEN 'Afternoon'
        ELSE 'Evening'
    END AS time_slot,
    SUM(total_amount) revenue
FROM fact_sales
GROUP BY time_slot;

/*14. Repeat vs New Customers*/

SELECT 
    CASE 
        WHEN order_count = 1 THEN 'New'
        ELSE 'Repeat'
    END AS customer_type,
    COUNT(*) customers
FROM (
    SELECT customer_id, COUNT(*) order_count
    FROM fact_sales
    GROUP BY customer_id
) t
GROUP BY customer_type;

/*15. Revenue Growth % */

SELECT 
    dt,
    daily_revenue,
    LAG(daily_revenue) OVER (ORDER BY dt) prev_day,
    ROUND(
        (daily_revenue - LAG(daily_revenue) OVER (ORDER BY dt)) * 100 /
        LAG(daily_revenue) OVER (ORDER BY dt),
        2
    ) AS growth_percent
FROM (
    SELECT 
        DATE(transaction_timestamp) dt,
        SUM(total_amount) daily_revenue
    FROM fact_sales
    GROUP BY dt
) t;

/*16. Running Total Revenue*/

SELECT 
    DATE(transaction_timestamp) dt,
    SUM(total_amount) daily,
    SUM(SUM(total_amount)) OVER (ORDER BY DATE(transaction_timestamp)) running_total
FROM fact_sales
GROUP BY dt;

/*17.Product Category Revenue*/

SELECT 
    p.category,
    SUM(f.total_amount) revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category;

/*18.Customer Segmentation (Gender)*/

SELECT 
    c.gender,
    COUNT(*) orders,
    SUM(f.total_amount) revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.gender;

/*19.Daily Revenue*/

SELECT 
    DATE(transaction_timestamp) date,
    SUM(total_amount) revenue
FROM fact_sales
GROUP BY date;

/*20.Peak Sales Hour*/

SELECT 
    HOUR(transaction_timestamp) hour,
    SUM(total_amount) revenue
FROM fact_sales
GROUP BY hour
ORDER BY revenue DESC;