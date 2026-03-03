CREATE VIEW customers_cleaned AS
SELECT
    c.customer_id,
    TRIM(c.email) AS email,
    INITCAP(c.name) AS name,
    c.signup_date
FROM raw.public.customers c
WHERE c.email IS NOT NULL
