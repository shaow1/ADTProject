-- Create the database for your project
CREATE DATABASE ecommerce_recommender;

-- Select the database to use
USE ecommerce_recommender;

-- creating the main transaction table
CREATE TABLE transactions (
    transaction_id INT PRIMARY KEY AUTO_INCREMENT,
    invoice VARCHAR(20) NOT NULL,
    stock_code VARCHAR(50) NOT NULL,
    description VARCHAR(500),
    quantity INT NOT NULL,
    invoice_date DATETIME NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    customer_id INT,
    country VARCHAR(100),
    INDEX idx_customer (customer_id),
    INDEX idx_stock (stock_code),
    INDEX idx_invoice (invoice),
    INDEX idx_date (invoice_date)
);

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    country VARCHAR(100),
    first_purchase_date DATETIME,
    total_purchases INT DEFAULT 0,
    total_spent DECIMAL(12, 2) DEFAULT 0.00
);

CREATE TABLE products (
    stock_code VARCHAR(50) PRIMARY KEY,
    description VARCHAR(500),
    avg_price DECIMAL(10, 2),
    total_quantity_sold INT DEFAULT 0,
    category VARCHAR(100)
);



CREATE TABLE user_item_interactions (
    interaction_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    stock_code VARCHAR(50) NOT NULL,
    interaction_count INT DEFAULT 1,
    last_interaction_date DATETIME,
    total_spent DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (stock_code) REFERENCES products(stock_code),
    INDEX idx_customer_product (customer_id, stock_code)
);



SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/1000_data.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(invoice, stock_code, description, quantity, @invoice_date, price, @customer_id, country)
SET 
    invoice_date = STR_TO_DATE(@invoice_date, '%Y-%m-%d %H:%i:%s'),
    customer_id = NULLIF(@customer_id, '');






-- Count total records
SELECT COUNT(*) as total_transactions FROM transactions;

-- View sample data
SELECT * FROM transactions LIMIT 10;

-- Check for cancelled transactions
SELECT 
    COUNT(*) as total_transactions,
    SUM(CASE WHEN invoice LIKE 'C%' THEN 1 ELSE 0 END) as cancelled_transactions,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT stock_code) as unique_products
FROM transactions;










-- Step 3: Re-enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

-- Step 4: Create customers table
CREATE TABLE customers AS
SELECT 
    customer_id,
    MAX(country) as country,
    MIN(invoice_date) as first_purchase_date,
    MAX(invoice_date) as last_purchase_date,
    COUNT(DISTINCT invoice) as total_orders,
    SUM(CASE WHEN invoice NOT LIKE 'C%' THEN quantity * price ELSE 0 END) as total_spent,
    SUM(CASE WHEN invoice LIKE 'C%' THEN 1 ELSE 0 END) as total_returns
FROM transactions
WHERE customer_id IS NOT NULL
GROUP BY customer_id;

ALTER TABLE customers ADD PRIMARY KEY (customer_id);

-- Step 5: Create products table
CREATE TABLE products AS
SELECT 
    stock_code,
    MAX(description) as description,
    AVG(price) as avg_price,
    SUM(CASE WHEN invoice NOT LIKE 'C%' THEN quantity ELSE 0 END) as total_quantity_sold,
    COUNT(DISTINCT customer_id) as unique_customers
FROM transactions
WHERE invoice NOT LIKE 'C%'
GROUP BY stock_code;

ALTER TABLE products ADD PRIMARY KEY (stock_code);

-- Step 6: Create user-item interactions WITHOUT foreign keys
CREATE TABLE user_item_interactions AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY customer_id, stock_code) as interaction_id,
    customer_id,
    stock_code,
    COUNT(*) as interaction_count,
    MAX(invoice_date) as last_interaction_date,
    SUM(quantity * price) as total_spent
FROM transactions
WHERE customer_id IS NOT NULL 
    AND invoice NOT LIKE 'C%'
GROUP BY customer_id, stock_code;

ALTER TABLE user_item_interactions ADD PRIMARY KEY (interaction_id);
ALTER TABLE user_item_interactions ADD INDEX idx_customer (customer_id);
ALTER TABLE user_item_interactions ADD INDEX idx_stock (stock_code);
ALTER TABLE user_item_interactions ADD INDEX idx_customer_product (customer_id, stock_code);


-- Check all tables were created
SELECT COUNT(*) as total_customers FROM customers;
SELECT COUNT(*) as total_products FROM products;
SELECT COUNT(*) as total_interactions FROM user_item_interactions;

-- View sample data
SELECT * FROM customers LIMIT 3;
SELECT * FROM products LIMIT 3;
SELECT * FROM user_item_interactions LIMIT 3;


--

-- Use your database
USE ecommerce_recommender;

-- Create benchmark results table
CREATE TABLE benchmark_results (
    test_id INT AUTO_INCREMENT PRIMARY KEY,
    test_name VARCHAR(100) NOT NULL,
    query_type VARCHAR(50),
    execution_time_ms DECIMAL(10, 3),
    rows_examined INT,
    rows_returned INT,
    cpu_usage_percent DECIMAL(5, 2),
    memory_used_mb DECIMAL(10, 2),
    test_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

-- Create query log table
CREATE TABLE query_performance_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    query_name VARCHAR(150),
    query_text TEXT,
    execution_time_sec DECIMAL(10, 6),
    rows_sent INT,
    rows_examined INT,
    created_tmp_tables INT,
    test_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Verify tables created
SELECT 'Tables created successfully' as status;


-- Enable query profiling
SET profiling = 1;
SET profiling_history_size = 100;


-- Disable safe mode for this session only
SET SQL_SAFE_UPDATES = 0;

-- Now run your performance schema update
UPDATE performance_schema.setup_instruments 
SET ENABLED = 'YES', TIMED = 'YES' 
WHERE NAME LIKE '%statement%';

UPDATE performance_schema.setup_consumers 
SET ENABLED = 'YES' 
WHERE NAME LIKE '%events_statements%';

-- Re-enable safe mode (optional, good practice)
SET SQL_SAFE_UPDATES = 1;







-- Enable performance schema
UPDATE performance_schema.setup_instruments 
SET ENABLED = 'YES', TIMED = 'YES' 
WHERE NAME LIKE '%statement%';

UPDATE performance_schema.setup_consumers 
SET ENABLED = 'YES' 
WHERE NAME LIKE '%events_statements%';

-- Verify it's enabled
SHOW VARIABLES LIKE 'profiling';
SELECT * FROM performance_schema.setup_consumers WHERE NAME LIKE '%statements%';



-- 1
-- Clear profiling
SET profiling = 1;

-- Benchmark 1: Products Frequently Bought Together (OPTIMIZED)
-- Set start time
SET @start = NOW(6);

-- Main query with optimization
SELECT 
    t1.stock_code as product_1,
    MAX(t1.description) as product_1_desc,
    t2.stock_code as product_2,
    MAX(t2.description) as product_2_desc,
    COUNT(DISTINCT t1.invoice) as times_bought_together
FROM transactions t1
STRAIGHT_JOIN transactions t2 
    ON t1.invoice = t2.invoice 
    AND t1.stock_code < t2.stock_code
WHERE t1.invoice NOT LIKE 'C%'
    AND t2.invoice NOT LIKE 'C%'
    AND t1.invoice_date >= '2011-01-01'  -- Filter to reduce data
GROUP BY t1.stock_code, t2.stock_code
HAVING times_bought_together >= 15  -- Lower threshold
ORDER BY times_bought_together DESC
LIMIT 20;

-- Calculate and display execution time
SELECT TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000 as execution_time_ms;

-- Save to benchmark results
INSERT INTO benchmark_results (test_name, query_type, execution_time_ms, rows_returned, notes)
VALUES (
    'Benchmark 1: Products Frequently Bought Together',
    'Market Basket Analysis',
    TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000,
    20,
    'Optimized with date filter and STRAIGHT_JOIN'
);

-- 2

SET @start = NOW(6);

-- Use a temporary table or derived table approach
SELECT 
    p.stock_code,
    p.description,
    COUNT(DISTINCT t.customer_id) as recommended_by_customers,
    SUM(t.quantity) as total_purchased,
    ROUND(AVG(t.price), 2) as avg_price
FROM transactions t
JOIN products p ON t.stock_code = p.stock_code
JOIN (
    -- Find similar customers (removed LIMIT from subquery)
    SELECT DISTINCT t2.customer_id
    FROM transactions t1
    JOIN transactions t2 
        ON t1.stock_code = t2.stock_code 
        AND t1.customer_id != t2.customer_id
    WHERE t1.customer_id = 13085
        AND t1.invoice NOT LIKE 'C%'
        AND t2.invoice NOT LIKE 'C%'
) similar_customers ON t.customer_id = similar_customers.customer_id
WHERE t.stock_code NOT IN (
    SELECT stock_code FROM transactions WHERE customer_id = 13085
)
AND t.invoice NOT LIKE 'C%'
GROUP BY p.stock_code, p.description
ORDER BY recommended_by_customers DESC
LIMIT 10;

SELECT TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000 as execution_time_ms;

INSERT INTO benchmark_results (test_name, query_type, execution_time_ms, rows_returned, notes)
VALUES (
    'Benchmark 2: Top-N Product Recommendations',
    'Collaborative Filtering',
    TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000,
    10,
    'User-based collaborative filtering - fixed subquery issue'
);



-- 3
SET @start = NOW(6);

SELECT 
    c.customer_id,
    c.country,
    c.total_orders,
    ROUND(c.total_spent, 2) as total_spent,
    COUNT(DISTINCT t.invoice) as times_bought_this_product,
    SUM(t.quantity) as total_quantity,
    MAX(t.invoice_date) as last_purchase_date
FROM customers c
JOIN transactions t ON c.customer_id = t.customer_id
WHERE t.stock_code = '85123A'
    AND t.invoice NOT LIKE 'C%'
GROUP BY c.customer_id, c.country, c.total_orders, c.total_spent
ORDER BY times_bought_this_product DESC
LIMIT 50;

SELECT TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000 as execution_time_ms;

INSERT INTO benchmark_results (test_name, query_type, execution_time_ms, rows_returned, notes)
VALUES (
    'Benchmark 3: Customers Who Bought Product',
    'Product-to-Customer Lookup',
    TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000,
    50,
    'Find customers who purchased product 85123A'
);


-- 4
SET @start = NOW(6);

SELECT 
    country,
    stock_code,
    description,
    total_quantity,
    total_revenue,
    product_rank
FROM (
    SELECT 
        t.country,
        t.stock_code,
        MAX(t.description) as description,
        SUM(t.quantity) as total_quantity,
        ROUND(SUM(t.quantity * t.price), 2) as total_revenue,
        ROW_NUMBER() OVER (PARTITION BY t.country ORDER BY SUM(t.quantity) DESC) as product_rank
    FROM transactions t
    WHERE t.invoice NOT LIKE 'C%'
        AND t.country IS NOT NULL
    GROUP BY t.country, t.stock_code
) ranked
WHERE product_rank <= 5
ORDER BY country, product_rank;

SELECT TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000 as execution_time_ms;

INSERT INTO benchmark_results (test_name, query_type, execution_time_ms, rows_returned, notes)
VALUES (
    'Benchmark 4: Popular Products by Country',
    'Regional Analysis',
    TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000,
    (SELECT COUNT(DISTINCT country) * 5 FROM transactions WHERE country IS NOT NULL),
    'Top 5 products per country using window functions'
);


-- 5

SET @start = NOW(6);

-- Fix for Benchmark 5: Recent Purchases by Similar Customers
SELECT 
    t.stock_code,
    p.description,
    COUNT(DISTINCT t.customer_id) as similar_customers,
    SUM(t.quantity) as total_quantity,
    ROUND(AVG(t.price), 2) as avg_price,
    MAX(t.invoice_date) as most_recent_purchase
FROM transactions t
JOIN products p ON t.stock_code = p.stock_code
JOIN (
    SELECT DISTINCT t2.customer_id
    FROM transactions t1
    JOIN transactions t2 ON t1.stock_code = t2.stock_code
    WHERE t1.customer_id = 13085
        AND t2.customer_id != 13085
        AND t1.invoice NOT LIKE 'C%'
        AND t2.invoice NOT LIKE 'C%'
) similar_customers ON t.customer_id = similar_customers.customer_id
WHERE t.invoice_date >= DATE_SUB((SELECT MAX(invoice_date) FROM transactions), INTERVAL 90 DAY)
    AND t.invoice NOT LIKE 'C%'
GROUP BY t.stock_code, p.description
ORDER BY similar_customers DESC
LIMIT 15;

SELECT TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000 as execution_time_ms;

INSERT INTO benchmark_results (test_name, query_type, execution_time_ms, rows_returned, notes)
VALUES (
    'Benchmark 5: Recent Purchases by Similar Customers',
    'Time-based Collaborative Filtering',
    TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000,
    15,
    'Fixed - recent 90-day purchases from similar customers'
);

-- 6
SET @start = NOW(6);

SELECT 
    t1.customer_id as customer_1,
    t2.customer_id as customer_2,
    COUNT(DISTINCT t1.stock_code) as common_products,
    ROUND(SUM(t1.quantity * t1.price), 2) as total_spending
FROM transactions t1
JOIN transactions t2 
    ON t1.stock_code = t2.stock_code 
    AND t1.customer_id < t2.customer_id
WHERE t1.invoice NOT LIKE 'C%'
    AND t2.invoice NOT LIKE 'C%'
    AND t1.customer_id IS NOT NULL
    AND t2.customer_id IS NOT NULL
    AND t1.invoice_date >= '2011-01-01'
GROUP BY t1.customer_id, t2.customer_id
HAVING common_products >= 5
ORDER BY common_products DESC
LIMIT 25;

SELECT TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000 as execution_time_ms;

INSERT INTO benchmark_results (test_name, query_type, execution_time_ms, rows_returned, notes)
VALUES (
    'Benchmark 6: Co-purchase Network',
    'Customer Similarity Network',
    TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000,
    25,
    'Customer pairs with 5+ common products'
);

-- 7
SET @start = NOW(6);

SELECT 
    t2.stock_code as next_product,
    MAX(t2.description) as product_description,
    COUNT(DISTINCT t1.customer_id) as customers_who_bought_both,
    ROUND(AVG(DATEDIFF(t2.invoice_date, t1.invoice_date)), 1) as avg_days_between,
    SUM(t2.quantity) as total_quantity_next
FROM transactions t1
JOIN transactions t2 
    ON t1.customer_id = t2.customer_id 
    AND t1.invoice_date < t2.invoice_date
WHERE t1.stock_code = '85123A'
    AND t2.stock_code != '85123A'
    AND t1.invoice NOT LIKE 'C%'
    AND t2.invoice NOT LIKE 'C%'
    AND DATEDIFF(t2.invoice_date, t1.invoice_date) <= 180
GROUP BY t2.stock_code
HAVING customers_who_bought_both >= 3
ORDER BY customers_who_bought_both DESC
LIMIT 15;

SELECT TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000 as execution_time_ms;

INSERT INTO benchmark_results (test_name, query_type, execution_time_ms, rows_returned, notes)
VALUES (
    'Benchmark 7: Sequential Product Recommendations',
    'Temporal Purchase Patterns',
    TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000,
    15,
    'Products purchased within 180 days after product 85123A'
);


-- 8
SET @start = NOW(6);

SELECT 
    c.customer_id,
    c.country,
    c.total_orders,
    ROUND(c.total_spent, 2) as lifetime_value,
    ROUND(c.total_spent / c.total_orders, 2) as avg_order_value,
    COUNT(DISTINCT t.stock_code) as unique_products,
    SUM(t.quantity) as total_items_purchased,
    DATEDIFF(c.last_purchase_date, c.first_purchase_date) as customer_lifetime_days
FROM customers c
JOIN transactions t ON c.customer_id = t.customer_id
WHERE t.invoice NOT LIKE 'C%'
GROUP BY c.customer_id, c.country, c.total_orders, c.total_spent, 
         c.last_purchase_date, c.first_purchase_date
HAVING total_orders >= 10
ORDER BY lifetime_value DESC
LIMIT 50;

SELECT TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000 as execution_time_ms;

INSERT INTO benchmark_results (test_name, query_type, execution_time_ms, rows_returned, notes)
VALUES (
    'Benchmark 8: Most Active Customers',
    'Customer Analytics',
    TIMESTAMPDIFF(MICROSECOND, @start, NOW(6)) / 1000,
    50,
    'Top 50 customers by lifetime value and engagement'
);


-- View all 8 benchmark results
SELECT 
    test_id,
    test_name,
    query_type,
    ROUND(execution_time_ms, 2) as time_ms,
    ROUND(execution_time_ms / 1000, 4) as time_sec,
    rows_returned,
    test_timestamp
FROM benchmark_results
ORDER BY test_id DESC
LIMIT 8;
