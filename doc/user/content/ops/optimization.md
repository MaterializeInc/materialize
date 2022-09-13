---
title: "Optimization"
description: "Recommendations for memory settings"
menu:
  main:
    parent: ops
    weight: 70
aliases:
  - /ops/deployment/
  - /ops/memory/
  - /ops/speedup/
---

## Speedup

Use indexes to speedup queries. Improvements can be significant, reducing some query times down to single-digit milliseconds. In particular, when the query filters only by the indexed fields.

Building an efficient index for distinct **clauses** and **operators** can be puzzling. To create the correct one, use the following sections, separated by clauses, as a guide:

* [`WHERE`](#where)
* [`JOIN`](#join)
* [`GROUP BY`](#group-by)

`ORDER BY` and `LIMIT` aren't clauses that benefit from an index.

### `WHERE`
Speedup a query involving a `WHERE` clause with equality comparisons, using the following table as a guide:

Clause                    | Index                                                                   |
--------------------------|-------------------------------------------------------------------------|
`WHERE x = $1`            | `CREATE INDEX ON view_name (x);`                                        |
`WHERE x IN ($1)`         | `CREATE INDEX ON view_name (x);`                                        |
`WHERE x * 2 = $1`        | `CREATE INDEX ON view_name (x * 2);`                                    |
`WHERE upper(x) = $1`     | `CREATE INDEX ON view_name (upper(x));`                                 |
`WHERE x = $1 AND y = $2` | `CREATE INDEX ON view_name (x, y);`                                     |
`WHERE x = $1 OR y = $2`  | `CREATE INDEX ON view_name (x);`<br /> `CREATE INDEX ON view_name (y);` |

**Note:** to speedup a query using a multi-column index, as in `WHERE x = $1 AND y = $2;`, the query must use all the fields in the index chained together via the `AND` operator.

### `JOIN`
Speedup a query using a `JOIN` on two relations by indexing their join keys:

Clause                                      | Index                                                                       |
--------------------------------------------|-----------------------------------------------------------------------------|
`FROM view V JOIN table T ON (V.id = T.id)` | `CREATE INDEX ON view (id);` <br /> `CREATE INDEX ON table (id);`           |

### `GROUP BY`
Speedup a query using a `GROUP BY` by indexing the aggregation keys:

Clause          | Index                             |
----------------|-----------------------------------|
`GROUP BY x,y`  | `CREATE INDEX ON view_name (x,y);`|

### Default

Implement the default index when there is no particular `WHERE`, `JOIN`, or `GROUP BY` clause to fulfill. Or, as a shorthand for a multi-column index using all the available columns:

Clause                                               | Index                                |
-----------------------------------------------------|--------------------------------------|
`SELECT x, y FROM view_name`                         | `CREATE DEFAULT INDEX ON view_name;` |
`SELECT x, y FROM view_name WHERE x = $1 AND y = $2` | `CREATE DEFAULT INDEX ON view_name;` |


## Memory

Materialize stores the majority of its state in memory, and works best when the
streamed data can be reduced in some way. For example, if you know that only a
subset of your rows and columns are relevant for your queries, it helps to avoid
materializing sources or views until you've expressed this to the system.
Materialize can then avoid stashing the full set of rows and columns, which can
in some cases dramatically reduce Materialize's memory footprint.

## Compaction

To prevent memory from growing without bound, Materialize periodically
"compacts" data in [arrangements](/overview/arrangements). For
example, if you have a source that tracks product inventory, you might receive
periodic inventory updates throughout the day:

```
(T-shirts, 9:07am, +500)
(T-shirts, 11:32am, -1)
(T-shirts, 3:14pm, -2)
```

Logical compaction will fold historical updates that fall outside the compaction
window into the state at the start of the window.

```
(T-shirts, 3:14pm, +497)
```

Materialize will only perform this compaction on data that falls outside the
logical compaction window. The default compaction window is 1 millisecond behind
the current time, but the window can be adjusted via the
[`--logical-compaction-window`](/cli/#compaction-window) option.

Adjusting the compaction window involves making a tradeoff between historical
detail and resource usage. A larger compaction window retains more historical
detail, but requires more memory. A smaller compaction window uses less memory
but also retains less historical detail. Larger compaction windows also increase
CPU usage, as more detailed histories require more compute time to maintain.

Note that compaction is triggered in response to updates arriving. As a result,
if updates stop arriving for a source, Materialize may never compact the source
fully. This can result in higher-than-expected memory usage.

This phenomenon is particularly evident when ingesting a source with a large
amount of historical data (e.g, a several gigabyte Kafka topic that is no longer
changing). With a compaction window of 60 seconds, for example, it is likely that
the source will be fully ingested within the compaction window. By the time the
data is eligible for compaction, the source is fully ingested, no new updates
are arriving, and therefore no compaction is ever triggered.

If the increased memory usage is problematic, consider one of the following
solutions:

  * Decrease the logical compaction window so that compaction is triggered while
    the source is still ingesting.

  * Compact the source upstream of Materialize.

    If you are using the [upsert envelope with a Kafka
    source](/sql/create-source/kafka/#handling-upserts), consider
    setting compaction policy on the Kafka topic to have Kafka perform the
    compaction.

    If you are using a [file source](/sql/create-source/text-file), consider
    rewriting the file to remove irrelevant historical data.

  * Periodically send dummy updates to trigger compaction.

## Swap

To minimize the chances that Materialize runs out of memory in a production
environment, we recommend you make additional memory available to Materialize
via a SSD-backed swap file or swap partition.

This is particularly important in Linux and in Docker, where swap may not be
automatically set up for you.

### Docker

By default, a container has no [resource
constraints](https://docs.docker.com/config/containers/resource_constraints/)
and can use as much memory and swap as the host allows, unless you have
overridden this with the `--memory` or the `--memory-swap` flags.

### Linux

Most cloud Linux distributions do not enable swap by default. However, you can
enable it quite easily with the following steps.

1. Create a swapfile using the `fallocate` command.

   The general syntax is: `fallocate -l <swap size> filename`. For a 1GB swap
   file, for example:

    ```shell
    sudo fallocate -l 1G /swapfile
    ```

1. Make the swap file only accessible to `root`:

    ```shell
    chmod 600 /swapfile
    ```

1. Mark the file as swap space:

   ```shell
   mkswap /swapfile
   ```

1. Enable the swap file:

    ```shell
    swapon /swapfile
    ```

1. Verify the swap is available:

    ```shell
    swapon --show
    ```
    ```ignore
    NAME      TYPE SIZE  USED PRIO
    /swapfile file   1G  0M   -2
    ```

1. *Optional.* To make the swap file permanent, add an entry for it to
  `/etc/fstab`:

     ```shell
     cat '/swapfile none swap sw 0 0' >> /etc/fstab
     ```
