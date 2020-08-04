---
title: "Idioms"
description: "Recommended SQL idioms."
weight: 12
menu:
  main:
    parent: sql
---

Some common transformations are surprisingly difficult to express in SQL. This
page documents our recommended idioms for several of these hard-to-express
transformations.

These idioms are known to the Materialize optimizer, so using these idioms can
result in much better performance than using idioms from other RDBMSes.

## Top-K by group

Suppose you want to group rows in a table by some key, then filter out all but
the first _k_ elements within each group according to some ordering. In other
databases you might use window functions. In Materialize, we recommend using a
[`LATERAL` subquery](/sql/join/#lateral-subqueries). The general form of the
query looks like this:

```sql
SELECT * FROM
    (SELECT DISTINCT key_col FROM tbl) grp,
    LATERAL (
        SELECT col1, col2... FROM tbl
        WHERE key_col = grp.key_col
        ORDER BY order_col LIMIT k
    )
```

For example, suppose you have a relation containing the population of various
U.S. cities.

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

To fetch the three most populous cities in each state:

```sql
SELECT state, name FROM
    (SELECT DISTINCT state FROM cities) grp,
    LATERAL (
        SELECT name FROM cities
        WHERE state = grp.state
        ORDER BY pop DESC LIMIT 3
    )
```
```nofmt
AZ  Phoenix
CA  Los_Angeles
CA  San_Diego
CA  San_Jose
IL  Chicago
NY  New_York
TX  Houston
TX  San_Antonio
TX  Dallas
```

Despite the verbosity of the above query, Materialize will produce a
straightforward plan:

```sql
EXPLAIN SELECT state, name FROM ...
```
```nofmt
%0 =
| Get materialize.public.cities (u1)
| TopK group=(#1) order=(#2 desc) limit=3 offset=0
| Project (#1, #0)
```
