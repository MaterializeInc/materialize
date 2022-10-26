---
title: "Optimization"
description: "Recommendations for query optimization in Materialize."
menu:
  main:
    parent: ops
    weight: 70
aliases:
  - /ops/speed-up/
---

Use indexes to speed up queries. Improvements can be significant, reducing some query times down to single-digit milliseconds. In particular, when the query filters only by the fields of an index, then Materialize can look up just the desired values from the index.

Building an efficient index for various **clauses** can be puzzling. To create the correct one, use the following sections, organized by clauses, as a guide:

* [`WHERE`](#where)
* [`JOIN`](#join)
* [`GROUP BY`](#group-by)

`ORDER BY` and `LIMIT` aren't clauses that benefit from an index.

### `WHERE`
Speed up a query involving a `WHERE` clause with equality comparisons to literals (e.g., `42`, or `'foo'`), using the following table as a guide. (`collection` can be a source, view, materialized
view, or table.)

| Clause                                            | Index                                    |
|---------------------------------------------------|------------------------------------------|
| `WHERE x = 42`                                    | `CREATE INDEX ON collection (x);`        |
| `WHERE x IN (1, 2, 3)`                            | `CREATE INDEX ON collection (x);`        |
| `WHERE (x, y) IN ((1, 'a'), (7, 'b'), (8, 'c'))`  | `CREATE INDEX ON collection (x, y);`     |
| `WHERE x = 1 AND y = 'abc'`                       | `CREATE INDEX ON collection (x, y);`     |
| `WHERE (x = 5 AND y = 'a') OR (x = 7 AND y = ''`) | `CREATE INDEX ON collection (x, y);`     |
| `WHERE 2 * x = 64`                                | `CREATE INDEX ON collection (2 * x);`    |
| `WHERE upper(y) = 'HELLO'`                        | `CREATE INDEX ON collection (upper(y));` |

You can verify that Materialize is accessing the input by an index lookup using `EXPLAIN`. Check for `lookup` after the index name to confirm that an index lookup is happening, i.e., that Materialize is only reading the matching elements of the index instead of scanning the entire index:
```
materialize=> EXPLAIN SELECT * FROM foo WHERE x = 42 AND y = 'hello';
                               Optimized Plan
-----------------------------------------------------------------------------
 Explained Query (fast path):                                               +
   Project (#0, #1)                                                         +
     ReadExistingIndex materialize.public.foo_x_y lookup value (42, "hello")+
                                                                            +
 Used Indexes:                                                              +
   - materialize.public.foo_x_y                                             +
```

#### Matching multi-column indexes to multi-column `WHERE` clauses

In general, your index key should exactly match the columns that are constrained in your `WHERE` clause. In more detail:
- **Attention:** If your `WHERE` clause constrains fewer fields than your index key includes, then **the index will not be used**. For example, an index on `(x, y)` cannot be used to speed up `WHERE x = 7`.
- If your `WHERE` clause constrains more fields than your index key includes, then the index might still provide some speedup, but it won't necessarily be optimal: In this case, the index lookup is performed using only those constraints that are included in the index key, and the rest of the constraints will be used to subsequently filter the result of the index lookup.
- If you have an `OR` not all of whose arguments constrain the same fields, then create an index for the intersection of the constrained fields. For example, if you have `WHERE (x = 51 AND y = 'bbb') OR (x = 76 AND z = 9)`, then create an index just on `x`.
- If you have an `OR` whose arguments constrain completely disjoint sets of fields (e.g., `WHERE x = 5 OR y = 'aaa'`), then try to rewrite your query using a `UNION` (or `UNION ALL`), where each argument of the `UNION` has one of the original `OR` arguments (with taking into account possible duplicate result rows from the intersection).

### `JOIN`
Speed up a `JOIN` query by indexing the join keys:

Clause                                      | Index                                                                       |
--------------------------------------------|-----------------------------------------------------------------------------|
`FROM view V JOIN table T ON (V.id = T.id)` | `CREATE INDEX ON view (id);` <br /> `CREATE INDEX ON table (id);`           |

For joins between more than two collections, you should strive for a *delta join* (a special join implementation of Materialize for making multi-way streaming joins memory-efficient), which is typically possible when you index all the join keys:
```
materialize=> EXPLAIN SELECT * FROM t1, t2, t3 WHERE t1.y = t2.x AND t2.y = t3.x;
                  Optimized Plan
--------------------------------------------------
 Explained Query:                                +
   Project (#0, #1, #1, #3, #3, #5)              +
     Filter (#1) IS NOT NULL AND (#3) IS NOT NULL+
       Join on=(#1 = #2 AND #3 = #4) type=delta  +  <--- "type" should show "delta"
         ArrangeBy keys=[[#1]]                   +
           Get materialize.public.t1             +
         ArrangeBy keys=[[#0], [#1]]             +
           Get materialize.public.t2             +
         ArrangeBy keys=[[#0]]                   +
           Get materialize.public.t3             +
                                                 +
 Used Indexes:                                   +
   - materialize.public.t1_y                     +
   - materialize.public.t3_x                     +
   - materialize.public.t2_x                     +
   - materialize.public.t2_y                     +
```
Delta joins have the advantage that they will use negligible additional memory outside the explicitly created indexes on the inputs. For more details, see [Maintaining Joins using Few Resources](https://materialize.com/blog/maintaining-joins-using-few-resources).

If your query filters one or more of the join inputs by a literal equality (e.g., `t1.y = 42`), then put one of those inputs first in the `FROM` clause. This can especially speed up one-off SELECT queries (i.e., queries that are not long-running dataflows, continuously updating their results), because this will avoid scanning any of the input collections: all of them will be accessed only by index lookups.

### `GROUP BY`
Speed up a query using a `GROUP BY` by indexing the aggregation keys:

Clause          | Index                               |
----------------|-------------------------------------|
`GROUP BY x,y`  | `CREATE INDEX ON collection (x,y);` |

### Default

Create the default index when there is no particular `WHERE`, `JOIN`, or `GROUP BY` clause to fulfill. This can still speed up your query by reading input from memory.

Clause                                               | Index                                |
-----------------------------------------------------|--------------------------------------|
`SELECT x, y FROM collection`                         | `CREATE DEFAULT INDEX ON collection;` |
