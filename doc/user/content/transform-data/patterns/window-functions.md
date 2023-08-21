---
title: "Window functions (OVER clause)"
description: "How to efficiently handle queries that require window functions in Materialize."
aliases:
  - /sql/patterns/window-functions/
menu:
  main:
    parent: 'sql-patterns'
---

Some query patterns that are commonly used in batch are tricky to support in streaming due to the unbounded nature of the input data. Window functions, such as `LAG`, `LEAD`, `ROW_NUMBER`, `FIRST_VALUE`, are one of those patterns.
As is, Materialize doesn't handle window functions efficiently for large partition sizes: when an input record is added, removed or changed, the system recomputes the results for the entire partition that the changed record belongs to. For this reason, we recommend using the alternative approaches described in this page whenever possible, until window function support is refactored to provide better performance.

It's important to note that **temporal windows** are efficiently supported by using [temporal filters and time bucketing](/sql/patterns/temporal-filters). For example, if you want to window over the last hour of data, or group data based on which hour each record occurred in.

By "window functions", this page does _not_ refer to temporal windows, but instead refers to functions such as `LAG`, `LEAD`, `ROW_NUMBER`, `FIRST_VALUE`. You can access these functions with the `OVER` clause, in which you use the `PARTITION BY` clause to group input records into partitions by a key, and use the `ORDER BY` clause to order records inside each partition. Window functions compute results based on the position of records inside these partitions. For example, `x - LAG(x)` computes the differences of attribute `x` between each pair of _consecutive_ rows in each partition.

## Example data

Let's use the following sample data as input for examples:

```sql
CREATE TABLE cities (
    name text NOT NULL,
    state text NOT NULL,
    pop int NOT NULL
);

INSERT INTO cities VALUES
    ('Los_Angeles', 'CA', 3979576),
    ('Phoenix', 'AZ', 1680992),
    ('Houston', 'TX', 2320268),
    ('San_Diego', 'CA', 1423851),
    ('San_Francisco', 'CA', 881549),
    ('New_York', 'NY', 8336817),
    ('Dallas', 'TX', 1343573),
    ('San_Antonio', 'TX', 1547253),
    ('San_Jose', 'CA', 1021795),
    ('Chicago', 'IL', 2695598),
    ('Austin', 'TX', 978908);
```

## Top K using `ROW_NUMBER`

In other databases, a popular way of computing the top _K_ records per key is to use the `ROW_NUMBER` window function. For example, to get the 3 most populous city in each state:
```sql
SELECT state, name
FROM (
  SELECT state, name, ROW_NUMBER() OVER
    (PARTITION BY state ORDER BY pop DESC) as row_num
  FROM cities
)
WHERE row_num <= 3;
```

If there are states that have many cities, a more performant way to express this in Materialize is to use a lateral join (or `DISTINCT ON`, if _K_ = 1) instead of window functions:
```sql
SELECT state, name FROM
    (SELECT DISTINCT state FROM cities) grp,
    LATERAL (
        SELECT name FROM cities
        WHERE state = grp.state
        ORDER BY pop DESC LIMIT 3
    );
```
For more details, see [Top K by group](/sql/patterns/top-k).

## `FIRST_VALUE`/`LAST_VALUE` of an entire partition

Suppose that you want to compute the ratio of each city's population vs. the most populous city in the same state. You can do so using window functions as follows:
```sql
SELECT state, name,
       CAST(pop AS float) / FIRST_VALUE(pop)
         OVER (PARTITION BY state ORDER BY pop DESC)
FROM cities;
```

For better performance, you can rewrite this query to first compute the largest population of each state using an aggregation, and then join against that:

```sql
SELECT cities.state, name, CAST(pop as float) / max_pops.max_pop
FROM cities,
     (SELECT state, MAX(pop) as max_pop
      FROM cities
      GROUP BY state) max_pops
WHERE cities.state = max_pops.state;
```

If the `ROW_NUMBER` would be called with an expression that is different from the expression in the `ORDER BY` clause, then the inner query would have to be replaced by a [`DISTINCT ON` query](/sql/patterns/top-k).

## Aggregate window functions over an entire partition

Suppose that you want to compute the ratio of each city's population vs. the total population of the city's state. You can do so using an aggregate window function as follows:

```sql
SELECT state, name,
       CAST(pop AS float) / SUM(pop)
         OVER (PARTITION BY state)
FROM cities;
```

Aggregate window functions are not yet supported in Materialize, but you can rewrite this query to first compute the total population of each state using an aggregation, and then join against that:

```sql
SELECT cities.state, name, CAST(pop as float) / total_pops.total_pop
FROM cities,
     (SELECT state, SUM(pop) as total_pop
      FROM cities
      GROUP BY state) total_pops
WHERE cities.state = total_pops.state;
```

## `LAG`/`LEAD` for time series

If the input has a column that advances by regular amounts, then `LAG` and `LEAD` can be replaced by an equi-join. Suppose that you have the following data:

```sql
CREATE TABLE measurements(time timestamp, value float);
INSERT INTO measurements VALUES
    (TIMESTAMP '2007-02-01 15:04:01', 8),
    (TIMESTAMP '2007-02-01 15:05:01', 9),
    (TIMESTAMP '2007-02-01 15:06:01', 12);
```

You can compute the differences between consecutive measurements using `LAG()`:
```sql
SELECT time, value - LAG(value) OVER (ORDER BY time)
FROM measurements;
```

For better performance, you can rewrite this query using an equi-join:

```sql
SELECT m2.time, m2.value - m1.value
FROM measurements m1, measurements m2
WHERE m2.time = m1.time + INTERVAL '1' MINUTE;
```

Note that these queries differ in whether they include the first timestamp (with a `NULL` difference). Using a `LEFT JOIN` would make the outputs match exactly.
