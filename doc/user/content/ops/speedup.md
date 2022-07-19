---
title: "Speedup"
description: "Find details about how to improve the performance using indexes"
aliases:
  - /speedup-queries
menu:
  main:
    parent: ops
    weight: 50
---

Indexes are a way to optimize query performance, from queries between views to queries from the client side. The following sections will display familiar scenarios that suit many common use cases.

Before continuing, consider the following example, a simple table with contacts data:

```sql
CREATE TABLE contacts (
    first_name TEXT,
    last_name TEXT,
    phone INT,
    prefix INT
);

INSERT INTO contacts SELECT 'random_name', 'random_last_name', generate_series(0, 100000), 1;
```

<!-- This will be removed till the PR makes it into production and is available to everyone -->
<!-- ### Checking if a query is using an index

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
that the query is scanning the whole table resulting in slower results. -->

## WHERE

The filtering clause is one of the most used operations in any database. Set up an index over the columns that a query filters by to increase the speed.

### Literal Values

Filtering by literal values is a typical case. An index over the column filtered will do the work.

Back to our example, to improve retrieving all the contacts by a particular first name (the literal value):

```sql
CREATE INDEX contacts_first_name_idx ON contacts (first_name);

SELECT * FROM contacts WHERE first_name = 'Charlie';
```

### Expressions

Expressions can also be part of the index. Materialize will calculate them faster using an index instead of calculating them on the fly for every query.

E.g., Since contact names are probably not always correctly written, using a function to upper case the name is helpful.

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

### Compound index

When using a compound index, consider how the optimizer works. It's rule-based, and if it detects an index it is helpful for a query, it will use it. Creating an index over all the columns doesn't mean that all
all the queries will be faster. Only the ones that use all the columns will receive an improvement.

E.g., There is indecision about what to index by, and a we decide to create a compound index.

```sql
CREATE INDEX contacts_all_index ON contacts(first_name, last_name, phone, prefix);

-- NO improvement over this query:
SELECT * FROM contacts WHERE first_name = 'CHARLIE' AND phone = 873090;

-- Improvement over this query:
SELECT * FROM contacts WHERE first_name = 'CHARLIE' AND last_name = 'EILR' AND phone = 873090 AND prefix = 1;
```

It is important to clarify that it is a good practice to create a compound index over all the columns of a view to distribute records between workers properly.

## JOIN

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



<!-- ## Temporal Filters [Research pending] -->

<!-- The index pattern can be a good one to add into the SQL patterns (Maybe in Manual materialization) -->

<!-- ## The Index Pattern

Creating an index using expressions is an alternative pattern to avoid building downstream views that only apply a function like the one used in the last example: `upper(first_name)`. Take into account that aggregations like `count()` and other non-materializable functions are not possible to use as expressions. -->
