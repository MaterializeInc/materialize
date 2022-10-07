---
title: "Speed up"
description: "Recommendations to speed up queries"
menu:
  main:
    parent: ops
    weight: 70
aliases:
  - /ops/speed-up/
---

Use indexes to speed up queries. Improvements can be significant, reducing some query times down to single-digit milliseconds. In particular, when the query filters only by the indexed fields.

Building an efficient index for distinct **clauses** and **operators** can be puzzling. To create the correct one, use the following sections, separated by clauses, as a guide:

* [`WHERE`](#where)
* [`JOIN`](#join)
* [`GROUP BY`](#group-by)

`ORDER BY` and `LIMIT` aren't clauses that benefit from an index.

### `WHERE`
Speed up a query involving a `WHERE` clause with equality comparisons, using the following table as a guide:

Clause                    | Index                                                                   |
--------------------------|-------------------------------------------------------------------------|
`WHERE x = $1`            | `CREATE INDEX ON view_name (x);`                                        |
`WHERE x IN ($1)`         | `CREATE INDEX ON view_name (x);`                                        |
`WHERE x * 2 = $1`        | `CREATE INDEX ON view_name (x * 2);`                                    |
`WHERE upper(x) = $1`     | `CREATE INDEX ON view_name (upper(x));`                                 |
`WHERE x = $1 AND y = $2` | `CREATE INDEX ON view_name (x, y);`                                     |
`WHERE x = $1 OR y = $2`  | `CREATE INDEX ON view_name (x);`<br /> `CREATE INDEX ON view_name (y);` |

**Note:** to speed up a query using a multi-column index, as in `WHERE x = $1 AND y = $2`, the query must use all the fields in the index chained together via the `AND` operator.

### `JOIN`
Speed up a query using a `JOIN` on two relations by indexing their join keys:

Clause                                      | Index                                                                       |
--------------------------------------------|-----------------------------------------------------------------------------|
`FROM view V JOIN table T ON (V.id = T.id)` | `CREATE INDEX ON view (id);` <br /> `CREATE INDEX ON table (id);`           |

### `GROUP BY`
Speed up a query using a `GROUP BY` by indexing the aggregation keys:

Clause          | Index                             |
----------------|-----------------------------------|
`GROUP BY x,y`  | `CREATE INDEX ON view_name (x,y);`|

### Default

Implement the default index when there is no particular `WHERE`, `JOIN`, or `GROUP BY` clause to fulfill. Or, as a shorthand for a multi-column index using all the available columns:

Clause                                               | Index                                |
-----------------------------------------------------|--------------------------------------|
`SELECT x, y FROM view_name`                         | `CREATE DEFAULT INDEX ON view_name;` |
`SELECT x, y FROM view_name WHERE x = $1 AND y = $2` | `CREATE DEFAULT INDEX ON view_name;` |
