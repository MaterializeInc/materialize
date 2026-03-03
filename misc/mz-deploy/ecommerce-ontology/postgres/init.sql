-- Customers
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    email TEXT,
    name TEXT,
    signup_date DATE
);

INSERT INTO customers VALUES
(1, 'alice@example.com', 'alice smith', '2024-01-15'),
(2, 'bob@example.com', 'bob jones', '2024-02-20'),
(3, 'carol@example.com', 'carol white', '2024-03-10'),
(4, 'dave@example.com', 'dave brown', '2024-04-05'),
(5, 'eve@example.com', 'eve davis', '2024-05-12'),
(6, 'frank@example.com', 'frank miller', '2024-06-18'),
(7, 'grace@example.com', 'grace wilson', '2024-07-22'),
(8, 'hank@example.com', 'hank taylor', '2024-08-30'),
(9, NULL, 'ian anderson', '2024-09-14'),
(10, 'jane@example.com', 'jane thomas', '2024-10-01');

-- Products
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    name TEXT,
    category TEXT,
    price NUMERIC,
    cost NUMERIC,
    is_active BOOLEAN
);

INSERT INTO products VALUES
(1, 'Laptop Pro', 'Electronics', 1299.99, 800.00, true),
(2, 'Wireless Mouse', 'Electronics', 29.99, 12.00, true),
(3, 'Desk Chair', 'Furniture', 349.99, 180.00, true),
(4, 'USB-C Hub', 'Electronics', 59.99, 25.00, true),
(5, 'Standing Desk', 'Furniture', 599.99, 300.00, true),
(6, 'Webcam HD', 'Electronics', 79.99, 35.00, true),
(7, 'Keyboard Mech', 'Electronics', 149.99, 65.00, true),
(8, 'Monitor 27"', 'Electronics', 449.99, 220.00, false);

-- Orders
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    order_date TIMESTAMP,
    total_amount NUMERIC,
    status TEXT
);

INSERT INTO orders VALUES
(1,  1, 1, '2025-06-01 10:00:00', 1299.99, 'completed'),
(2,  1, 2, '2025-06-05 14:30:00', 29.99,   'completed'),
(3,  2, 3, '2025-07-10 09:15:00', 349.99,  'completed'),
(4,  2, 4, '2025-07-12 11:00:00', 59.99,   'completed'),
(5,  3, 1, '2025-08-01 16:45:00', 1299.99, 'completed'),
(6,  3, 5, '2025-08-03 08:20:00', 599.99,  'shipped'),
(7,  4, 6, '2025-09-15 13:00:00', 79.99,   'completed'),
(8,  4, 7, '2025-09-18 10:30:00', 149.99,  'completed'),
(9,  5, 2, '2025-10-01 15:00:00', 29.99,   'completed'),
(10, 5, 4, '2025-10-05 12:00:00', 59.99,   'shipped'),
(11, 6, 1, '2025-11-01 09:00:00', 1299.99, 'completed'),
(12, 6, 3, '2025-11-10 14:00:00', 349.99,  'completed'),
(13, 7, 5, '2025-12-01 11:30:00', 599.99,  'completed'),
(14, 7, 7, '2025-12-05 16:00:00', 149.99,  'shipped'),
(15, 8, 6, '2026-01-10 10:00:00', 79.99,   'completed'),
(16, 8, 2, '2026-01-15 13:45:00', 29.99,   'completed'),
(17, 1, 7, '2026-02-01 08:30:00', 149.99,  'completed'),
(18, 2, 5, '2026-02-10 15:15:00', 599.99,  'pending'),
(19, 3, 4, '2026-02-20 12:00:00', 59.99,   'completed'),
(20, 10, 1, '2026-03-01 09:00:00', 1299.99, 'pending');

-- Returns
CREATE TABLE returns (
    return_id INT PRIMARY KEY,
    order_id INT,
    refund_amount NUMERIC
);

INSERT INTO returns VALUES
(1, 2,  29.99),
(2, 7,  79.99),
(3, 9,  29.99),
(4, 12, 349.99),
(5, 16, 29.99);

-- Publication for Materialize CDC
CREATE PUBLICATION mz_source FOR ALL TABLES;
