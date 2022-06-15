---
title: "Indexing"
description: "Find details about how to improve the performance using indexes"
aliases:
  - /optimizing-queries
menu:
  main:
    parent: ops
    weight: 50
---

Indexes are a way to optimize query performance. In the following sections, you will find different ways to implement them and get faster results.

<!-- This will be removed till the PR makes it into production and is available to everyone -->
### Checking if a query is using an index

The [EXPLAIN](https://materialize.com/docs/sql/explain/#conceptual-framework) command displays if the query plan is including an index to read from.

```sql
EXPLAIN SELECT * FROM contacts WHERE first_name = 'Jann';
```

```
                      Optimized Plan
------------------------------------------------------------------
 %0 =                                                            +
 | ReadExistingIndex materialize.public.contacts_first_name_idx  +
 | Get materialize.public.contacts (u23)                         +
 | Filter (#0 = "Jann")                                          +
```

`ReadExistingIndex` indicates that the query is using the `contacts_first_name_idx`. An absence of it would mean
that the query is scanning the whole table resulting in slower results.

## Optimizing WHERE filtering

Setting up an index over a column were filtering by literal values or expressions is common will improve the performance.

Consider the following example, a simple table with contacts data:

```sql
CREATE TABLE contacts (
    first_name TEXT,
    last_name TEXT,
    phone INT,
    prefix INT
);

-- 2) Random data
INSERT INTO contacts SELECT 'random_name', 'random_last_name', generate_series(0, 100000), 1;
```

### Literal Values

Typical usage is retrieving all the contacts with a particular first name (the literal value). Indexing the column will improve the performance of any query whose only filter is the one the index has.

```sql
CREATE INDEX contacts_first_name_idx ON contacts (first_name);

SELECT * FROM contacts WHERE first_name = 'Charlie';
```

### Expressions

Expressions can also be part of the index. Back to our example, since contact names are probably not always correctly written, using a function to upper case all the characters can be helpful. An index can do the job incrementally for us rather than calculating those upper values on the fly for every query.

```sql
CREATE INDEX contacts_upper_first_name_idx ON contacts (upper(first_name));

SELECT * FROM contacts WHERE upper(first_name) = 'CHARLIE';
```

An _expression_ goes beyond a function. It could be a mathematical expression that is present in the filtering

```sql
CREATE INDEX contacts_upper_first_name_idx ON contacts (prefix - 1 = 0);

SELECT * FROM contacts WHERE prefix - 1 = 0;
```


### Literal Values and Expressions

It's possible to also combine both worlds, literal values and expressions.

```sql
CREATE INDEX contacts_upper_first_name_phone_idx ON contacts (upper(first_name), phone);

SELECT * FROM contacts WHERE first_name = 'CHARLIE' AND phone = 873090;
```

## Optimizing JOIN

Optimize the performance of `JOIN` on two relations by ensuring their
join keys are the key columns in an index.

```sql
CREATE INDEX contacts_prefix_idx ON contacts (prefix);
CREATE INDEX geo_prefix_idx ON geo (prefix);

CREATE MATERIALIZED VIEW country_per_phone AS
    SELECT phone, prefix, country
    FROM geo
    JOIN contacts ON contacts.prefix = geo.prefix;
```

In the above example, the index `contacts_prefix_idx`...

-   Helps because it contains a key that the view `country_per_phone` can
    use to look up values for the join condition (`contacts.prefix`).

    Because this index is exactly what the query requires, the Materialize
    optimizer will choose to use `contacts_prefix_idx` rather than build
    and maintain a private copy of the index just for this query.

-   Obeys our restrictions by containing only a subset of columns in the result
    set.

## Optimizing Temporal Filters [Research pending]

<!-- The index pattern can be a good one to add into the SQL patterns -->

## The Index Pattern

Creating an index using expressions is an alternative pattern to avoid building downstream views that only apply a function like the one used in the last example: `upper(first_name)`. Take into account that aggregations like `count()` and other non-materializable functions are not possible to use as expressions.

<!-- Which query should run faster?

```sql
-- a)
SELECT * FROM phones WHERE first_name = 'Jann';

-- b)
SELECT * FROM phones WHERE first_name = 'Jann' and last_name = 'Johnson' and phone = 12233344;
```

At first *a)* sounds like the candidate since it's filtering by one field, but answer *b)* is the correct one! It's more than **ten times faster** than the alternative option (it can vary by setup). -->

<!-- Eanble `\timing` command and run both queries using `psql` to tell the difference in your setup. -->

<!-- ### What is happening?

The `phones_index` in the step *2)* is a multicolumn index and Materialize will take advantage of it only and only when all the columns are present in the filter clause. (Check fact)

Improving the performance of querying by the person first name, like in query *a)*, requires an index like the following:

```
CREATE INDEX first_name_index ON phones (first_name);
``` -->


<!-- ```sql
CREATE MATERIALIZED VIEW active_customers AS
    SELECT guid, geo_id, last_active_on
    FROM customer_source
    GROUP BY geo_id;

CREATE INDEX active_customers_idx ON active_customers (guid);

SELECT * FROM active_customers WHERE guid = 'd868a5bf-2430-461d-a665-40418b1125e7';

-- Using expressions
CREATE INDEX active_customers_exp_idx ON active_customers (upper(guid));

SELECT * FROM active_customers WHERE upper(guid) = 'D868A5BF-2430-461D-A665-40418B1125E7';

-- Filter using an expression in one field and a literal in another field
CREATE INDEX active_customers_exp_field_idx ON active_customers (upper(guid), geo_id);

SELECT * FROM active_customers WHERE upper(guid) = 'D868A5BF-2430-461D-A665-40418B1125E7' and geo_id = 'ID_8482';
``` -->