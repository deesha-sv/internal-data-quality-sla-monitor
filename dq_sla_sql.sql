CREATE DATABASE dq_sandbox;
USE dq_sandbox;

-- Customers table
CREATE TABLE customers (
    customer_id      INT PRIMARY KEY AUTO_INCREMENT,
    customer_name    VARCHAR(100),
    email            VARCHAR(150),
    country          VARCHAR(50),
    segment          VARCHAR(50),
    customer_created_at DATETIME
);

-- Orders table
CREATE TABLE orders (
    order_id     INT PRIMARY KEY AUTO_INCREMENT,
    customer_id  INT,
    order_date   DATE,
    order_status VARCHAR(50),
    order_total  DECIMAL(10,2),
    created_at   DATETIME,
    updated_at   DATETIME,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Order items table
CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id      INT,
    product_id    INT,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Payments table
CREATE TABLE payments (
    payment_id     INT PRIMARY KEY AUTO_INCREMENT,
    order_id       INT,
    payment_amount DECIMAL(10,2),
    payment_status VARCHAR(50),
    payment_date   DATETIME,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- A few customers
INSERT INTO customers (customer_name, email, country, segment, customer_created_at)
VALUES
('Alice Smith', 'alice@example.com', 'Ireland', 'Retail',  NOW()),
('Bob Johnson',  'bob@example.com',   'Ireland', 'Enterprise', NOW()),
('Carol Lee',   'carol@example.com', 'UK',      'Retail', NOW());

-- A few orders (assume today and yesterday)
INSERT INTO orders (customer_id, order_date, order_status, order_total, created_at, updated_at)
VALUES
(1, CURDATE() - INTERVAL 1 DAY, 'completed', 120.50, NOW() - INTERVAL 1 DAY, NOW()),
(2, CURDATE(),                 'completed', 80.00,  NOW(),                 NOW()),
(3, CURDATE(),                 'pending',   45.00,  NOW(),                 NOW());

-- Order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES
(1, 101, 2, 30.00),
(1, 102, 1, 60.50),
(2, 103, 1, 80.00),
(3, 104, 3, 15.00);

-- Payments
INSERT INTO payments (order_id, payment_amount, payment_status, payment_date)
VALUES
(1, 120.50, 'paid',    NOW() - INTERVAL 1 DAY),
(2, 80.00,  'paid',    NOW()),
(3, 0.00,   'pending', NOW());

SELECT * FROM customers;
SELECT * FROM orders;
SELECT * FROM order_items;
SELECT * FROM payments;

-- Data quality check 
-- Row count check (orders)
SELECT COUNT(*) AS total_orders
FROM orders;

-- Null check on a key column
SELECT COUNT(*) AS null_customer_id
FROM orders
WHERE customer_id IS NULL;

-- Freshness check 
SELECT MAX(updated_at) AS latest_update
FROM orders;


CREATE TABLE dq_check_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    check_name VARCHAR(100),
    table_name VARCHAR(100),
    status VARCHAR(10),
    metric_value DOUBLE,
    expected_condition VARCHAR(255),
    run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT *
FROM dq_check_results
ORDER BY run_time DESC;

UPDATE orders
SET updated_at = NOW()
WHERE order_id > 0;

CREATE OR REPLACE VIEW dq_summary AS
SELECT
    table_name,
    check_name,
    status,
    run_time
FROM dq_check_results;


SELECT *
FROM dq_check_results
ORDER BY run_time DESC;

CREATE OR REPLACE VIEW dq_summary_table_run AS
SELECT
    table_name,
    run_time,
    SUM(status = 'PASS') AS passed_checks,
    SUM(status = 'FAIL') AS failed_checks
FROM dq_check_results
GROUP BY table_name, run_time
ORDER BY run_time DESC;

SELECT *
FROM dq_summary_table_run
ORDER BY run_time DESC;

SELECT *
FROM dq_summary_table_run;
