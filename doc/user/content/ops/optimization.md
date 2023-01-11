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

## Indexes

Like in any standard relational database, you can use [indexes](/overview/key-concepts/#indexes) to optimize query performance in Materialize. Improvements can be significant, reducing some query times down to single-digit milliseconds.

Building an efficient index depends on the clauses used in your queries, as well as your expected access patterns. Use the following as a guide:

* [WHERE](#where)
* [JOIN](#join)
* [DEFAULT](#default)

`GROUP BY`, `ORDER BY` and `LIMIT` clauses currently do not benefit from an index.


### `WHERE`
Speed up a query involving a `WHERE` clause with equality comparisons to literals (e.g., `42`, or `'foo'`):

| Clause                                            | Index                                    |
|---------------------------------------------------|------------------------------------------|
| `WHERE x = 42`                                    | `CREATE INDEX ON obj_name (x);`        |
| `WHERE x IN (1, 2, 3)`                            | `CREATE INDEX ON obj_name (x);`        |
| `WHERE (x, y) IN ((1, 'a'), (7, 'b'), (8, 'c'))`  | `CREATE INDEX ON obj_name (x, y);`     |
| `WHERE x = 1 AND y = 'abc'`                       | `CREATE INDEX ON obj_name (x, y);`     |
| `WHERE (x = 5 AND y = 'a') OR (x = 7 AND y = ''`) | `CREATE INDEX ON obj_name (x, y);`     |
| `WHERE 2 * x = 64`                                | `CREATE INDEX ON obj_name (2 * x);`    |
| `WHERE upper(y) = 'HELLO'`                        | `CREATE INDEX ON obj_name (upper(y));` |

You can verify that Materialize is accessing the input by an index lookup using `EXPLAIN`. Check for `lookup_value` after the index name to confirm that an index lookup is happening, i.e., that Materialize is only reading the matching elements of the index instead of scanning the entire index:
```
materialize=> EXPLAIN SELECT * FROM foo WHERE x = 42 AND y = 'hello';
                               Optimized Plan
-----------------------------------------------------------------------------
 Explained Query (fast path):                                               +
   Project (#0, #1)                                                         +
     ReadExistingIndex materialize.public.foo_x_y lookup_value=(42, "hello")+
                                                                            +
 Used Indexes:                                                              +
   - materialize.public.foo_x_y                                             +
```

#### Matching multi-column indexes to multi-column `WHERE` clauses

In general, your index key should exactly match the columns that are constrained in the `WHERE` clause. In more detail:
- If the `WHERE` clause constrains fewer fields than your index key includes, then **the index will not be used**. For example, an index on `(x, y)` cannot be used to speed up `WHERE x = 7`.
- If the `WHERE` clause constrains more fields than your index key includes, then the index might still provide some speedup, but it won't necessarily be optimal: In this case, the index lookup is performed using only those constraints that are included in the index key, and the rest of the constraints will be used to subsequently filter the result of the index lookup.
- If `OR` is used and not all arguments constrain the same fields, create an index for the intersection of the constrained fields. For example, if you have `WHERE (x = 51 AND y = 'bbb') OR (x = 76 AND z = 9)`, create an index just on `x`.
- If `OR` is used and its arguments constrain completely disjoint sets of fields (e.g. `WHERE x = 5 OR y = 'aaa'`), try to rewrite your query using a `UNION` (or `UNION ALL`), where each argument of the `UNION` has one of the original `OR` arguments.

### `JOIN`
Speed up a `JOIN` query by indexing the join keys:

Clause                                      | Index                                                                       |
--------------------------------------------|-----------------------------------------------------------------------------|
`FROM view V JOIN table T ON (V.id = T.id)` | `CREATE INDEX ON view (id);` <br /> `CREATE INDEX ON table (id);`           |

#### Optimize Multi-Way Joins with Delta Joins

For joins between more than two inputs, you should strive for a *delta join* (a special join implementation of Materialize for making multi-way streaming joins memory-efficient), which is typically possible when you index all the join keys:
```
materialize=> EXPLAIN SELECT * FROM t1, t2, t3 WHERE t1.y = t2.x AND t2.w = t3.z;
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
   - materialize.public.t2_x                     +
   - materialize.public.t2_w                     +
   - materialize.public.t3_z                     +
```
Delta joins have the advantage of using negligible additional memory outside the explicitly created indexes on the inputs. For more details, see [Delta Joins and Late Materialization](/overview/delta-joins).

If your query filters one or more of the join inputs by a literal equality (e.g., `t1.y = 42`), place one of those inputs first in the `FROM` clause. In particular, this can speed up [ad hoc `SELECT` queries](/sql/select/#ad-hoc-queries) by accessing inputs using index lookups, rather than full scans.

#### Further Optimize with Late Materialization

Materialize can further optimize memory usage - using a pattern known as late materialization - when joining relations containing primary and foreign key constraints.

1. Create indexes on the primary keys of your input collections. Example:
    ```sql
    CREATE INDEX pk_lineitem ON lineitem (l_orderkey, l_linenumber);
    CREATE INDEX pk_customer ON customer (c_custkey);
    CREATE INDEX pk_orders ON orders (o_orderkey);
    ```
2. For each foreign key in the join, create a "narrow" view with just two columns: foreign key and primary key. Then create two indexes: one for the foreign key and one for the primary key. Example from [Delta Joins and Late Materialization](/overview/delta-joins):
    ```sql
    -- Create a "narrow" view containing foreign key `l_orderkey` and `lineitem`'s composite primary key (l_orderkey, l_linenumber) and their respective indexes.
    CREATE VIEW lineitem_fk_orderkey AS SELECT l_orderkey, l_linenumber FROM lineitem;
    CREATE INDEX lineitem_fk_orderkey_0 ON lineitem_fk_orderkey (l_orderkey, l_linenumber);
    CREATE INDEX lineitem_fk_orderkey_1 ON lineitem_fk_orderkey (l_orderkey);
    -- Create a "narrow" view containing foreign key `o_custkey` and `orders`'s primary key `o_orderkey` and their respective indexes.
    CREATE VIEW orders_fk_custkey AS SELECT o_orderkey, o_custkey FROM orders;
    CREATE INDEX orders_fk_custkey_0 on orders_fk_custkey (o_orderkey);
    CREATE INDEX orders_fk_custkey_1 on orders_fk_custkey (o_custkey);
    ```
3. Update your query's `FROM` statement to include the narrow views. Example:
    
    Before:
    ```sql
    ...
    FROM
        customer,
        orders,
        lineitem
    ...
    ```
    After:
    ```sql
    ...
    FROM
        customer c,
        orders o,
        lineitem l,
        -- NEW: "narrow" collections containing just primary and foreign keys.
        lineitem_fk_orderkey l_ok,
        orders_fk_custkey o_ck
    ...
    ```
4. Replace each foreign key from "wide" collections with the foreign key from "narrow" collections. Example:
    
    Before:
    ```sql
    ...
    WHERE
    ...
        AND l_orderkey = o_orderkey
        AND o_custkey  = c_custkey
    ...
    ```
    After:
    ```sql
    ...
    WHERE
        -- core equijoin constraints using "narrow" collections.
        l_ok.l_orderkey = o.o_orderkey
        AND o_ck.o_custkey = c.c_custkey
    ...
    ```
5. Finally, add join conditions to equate each foreign key from "narrow" collections to a primary key from "wide" collections. Example:
    ```sql
    ...
    WHERE
    ...
        -- connect narrow and wide collections.
        AND o_ck.o_orderkey = o.o_orderkey
        AND l_ok.l_orderkey = l.l_orderkey
        AND l_ok.l_linenumber = l.l_linenumber
    ...
    ```

### Default

Create a default index when there is no particular `WHERE` or `JOIN` clause that would fit the above cases. This can still speed up your query by reading the input from memory.

Clause                                               | Index                               |
-----------------------------------------------------|-------------------------------------|
`SELECT x, y FROM obj_name`                          | `CREATE DEFAULT INDEX ON obj_name;` |
