---
title: "Single parsing"
description: "Parse a source once and save resources."
aliases:
  - /guides/single-parsing/
menu:
  main:
    parent: 'sql-patterns'
---

<!-- Use a single materialized view to parse a Kafka/Redpanda source **only once** and save resources. -->
Avoid parsing **more than once** a Kafka/Redpanda source. Using a single materialized view for parsing leads you to multiple benefits:
* Faster and reusable access to data
* Lesser resources processing data
* Cleaner code

## Details

Materialized views process data and store the results in durable storage. Re-doing the parsing step, as a subquery, on every view consuming from a source will multiply the processing effort.

#### Multi-parsing

The following scenario reflects how two different materialized views increase the processing **overhead** by doing the same parsing step in the subqueries:

```sql
CREATE MATERIALIZED VIEW paid_customers AS
SELECT ...
FROM (
    -- Parse
    SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data
    FROM kafka_source
)
WHERE type = 'paid';


CREATE MATERIALIZED VIEW free_customers AS
SELECT ...
FROM (
    -- Parse
    SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data
    FROM kafka_source
)
WHERE type = 'free';
```

#### Single-parsing

A single parsing materialized view **reduces the processing overhead** and **makes the results reusable**:

```sql
-- Parse once
CREATE MATERIALIZED VIEW customers AS
  SELECT
    ...
  FROM (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data FROM kafka_source);

-- Reuse
CREATE MATERIALIZED VIEW paid_customers AS
SELECT * FROM customers WHERE type ='paid';

CREATE MATERIALIZED VIEW free_customers AS
SELECT * FROM customers WHERE type = 'free';
```
