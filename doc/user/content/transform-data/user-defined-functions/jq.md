---
title: "Write functions with jq"
description: "Sort, deduplicate, and reshape lists with the jq function."
menu:
  main:
    parent: user-defined-functions
    weight: 10
---

{{< warn-if-unreleased "v26.50" >}}

In this tutorial, you use the [`jq`](/sql/functions/jq/) function to clean up
lists stored in a `jsonb` column, and compare its plan with the equivalent SQL.

## Before you begin

Connect to Materialize with a SQL client, or use the SQL Shell in the
Materialize Console.

## Step 1. Create sample data

Create a table of orders. Each order has a list of tags and a list of line
items:

```mzsql
CREATE TABLE orders (order_id int, tags jsonb, items jsonb);

INSERT INTO orders VALUES
  (1, '["Gift", "rush", "gift", "Fragile"]',
      '[{"sku": "A1", "price": 30}, {"sku": "B2", "price": 120}, {"sku": "C3", "price": 75}]'),
  (2, '["bulk", "Bulk"]',
      '[{"sku": "D4", "price": 5}]');
```

## Step 2. Sort and deduplicate a list

Lowercase each tag, then deduplicate the list. `unique` also sorts it:

```mzsql
SELECT order_id, jq(tags, 'map(ascii_downcase) | unique') AS tags
FROM orders
ORDER BY order_id;
```

```nofmt
 order_id |           tags
----------+---------------------------
        1 | ["fragile","gift","rush"]
        2 | ["bulk"]
```

## Step 3. Pass in several values and return several values

A jq program takes one `jsonb` input and returns one `jsonb` output. To pass in
more than one value, build an object with `jsonb_build_object`. To return more
than one value, return an object and extract its fields with `->` and `->>`.

The following query passes in the line items and a limit `n`, and returns the
`n` most expensive SKUs and the order total:

```mzsql
SELECT
  order_id,
  summary -> 'top_skus' AS top_skus,
  (summary ->> 'total')::numeric AS total
FROM (
  SELECT
    order_id,
    jq(jsonb_build_object('items', items, 'n', 2), $$
      .n as $n
      | {
          top_skus: (.items | sort_by(-.price) | .[:$n] | map(.sku)),
          total: (.items | map(.price) | add)
        }
    $$) AS summary
  FROM orders
)
ORDER BY order_id;
```

```nofmt
 order_id |  top_skus   | total
----------+-------------+-------
        1 | ["B2","C3"] |   225
        2 | ["D4"]      |     5
```

## Step 4. Compare the plans

Create a materialized view that cleans up the tags with `jq`:

```mzsql
CREATE MATERIALIZED VIEW order_tags AS
SELECT order_id, jq(tags, 'map(ascii_downcase) | unique') AS tags
FROM orders;
```

Inspect its plan:

```mzsql
EXPLAIN MATERIALIZED VIEW order_tags;
```

```nofmt
                    Physical Plan
------------------------------------------------------
 materialize.public.order_tags:
   →Read materialize.public.orders

 Source materialize.public.orders
   project=(#0, #3)
   map=(jq["map(ascii_downcase) | unique"](#1{tags}))

 Target cluster: quickstart
```

The program runs as a `map` while Materialize reads `orders`. The plan has no
arrangements, so the view keeps no state beyond its output.

For comparison, create the same view in SQL:

```mzsql
CREATE MATERIALIZED VIEW order_tags_sql AS
SELECT
  o.order_id,
  (
    SELECT jsonb_agg(DISTINCT lower(t))
    FROM jsonb_array_elements_text(o.tags) AS t
  ) AS tags
FROM orders AS o;

EXPLAIN MATERIALIZED VIEW order_tags_sql;
```

```nofmt
                                                    Physical Plan
----------------------------------------------------------------------------------------------------------------------
 materialize.public.order_tags_sql:
   →With
     cte l0 =
       →Distinct GroupAggregate
         →Fused with Child Map/Filter/Project
           Project: #1
             →Read materialize.public.orders
     cte l1 =
       →Non-incremental GroupAggregate
         Aggregation: jsonb_agg[order_by=[]](distinct row(coalesce(jsonbable_to_jsonb(lower(#1{value})), json_null)))
         Key:
           Project: #0
         →Table Function jsonb_array_elements_text(#0{tags})
           →Arranged l0
     cte l2 =
       →Union
         →Unarranged Raw Stream
           →Arranged l1
         →Map/Filter/Project
           Project: #0, #1
           Map: null
             →Consolidating Union
               →Negate Diffs
                 →Fused with Child Map/Filter/Project
                   Project: #0
                     →Arranged l1
                       Key: (#0)
               →Unarranged Raw Stream
                 →Arranged l0
   →Return
     →Differential Join %1[#0] » %0:orders[#1]
       →Arrange (#1)
         →Read materialize.public.orders
       →Arrange (#0)
         →Union
           →Stream l2
           →Map/Filter/Project
             Project: #0, #1
             Map: null
               →Consolidating Union
                 →Negate Diffs
                   →Fused with Child Map/Filter/Project
                     Project: #0
                       →Read l2
                 →Unarranged Raw Stream
                   →Arranged l0

 Source materialize.public.orders
   project=(#0, #1)

 Target cluster: quickstart
```

The SQL plan is much longer. It unnests the list (`Table Function
jsonb_array_elements_text`), aggregates it back (`Distinct GroupAggregate` and
`Non-incremental GroupAggregate`), and joins the result to the original rows
(`Differential Join`). These operators maintain arrangements, which keep
copies of the data in memory.

## Step 5. Clean up

```mzsql
DROP MATERIALIZED VIEW order_tags_sql;
DROP MATERIALIZED VIEW order_tags;
DROP TABLE orders;
```

## Next steps

- See the [`jq` function reference](/sql/functions/jq/) for its behavior and
  limitations.
- See the [jq manual](https://jqlang.org/manual/) for the filters you can use
  in programs.
