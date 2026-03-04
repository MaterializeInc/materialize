CREATE VIEW customers_cleaned AS
SELECT
    c.customer_id,
    TRIM(c.email) AS email,
    INITCAP(c.name) AS name,
    c.signup_date
FROM raw.public.customers c
WHERE c.email IS NOT NULL;

CREATE INDEX customers_cleaned_by_id IN CLUSTER ontology ON customers_cleaned (customer_id);

EXECUTE UNIT TEST test_filters_null_emails
FOR ontology.internal.customers_cleaned
MOCK raw.public.customers(customer_id INTEGER, email TEXT, name TEXT, signup_date DATE) AS (
  SELECT * FROM VALUES
    (1, '  alice@ex.com  ', 'alice smith', '2024-01-15'::DATE),
    (2, NULL, 'no email', '2024-02-01'::DATE)
),
EXPECTED(customer_id INTEGER, email TEXT, name TEXT, signup_date DATE) AS (
  SELECT * FROM VALUES
    (1, 'alice@ex.com', 'Alice Smith', '2024-01-15'::DATE)
);

EXECUTE UNIT TEST test_trims_whitespace
FOR ontology.internal.customers_cleaned
MOCK raw.public.customers(customer_id INTEGER, email TEXT, name TEXT, signup_date DATE) AS (
  SELECT * FROM VALUES
    (1, '  bob@ex.com  ', 'bob jones', '2024-03-01'::DATE)
),
EXPECTED(customer_id INTEGER, email TEXT, name TEXT, signup_date DATE) AS (
  SELECT * FROM VALUES
    (1, 'bob@ex.com', 'Bob Jones', '2024-03-01'::DATE)
)
