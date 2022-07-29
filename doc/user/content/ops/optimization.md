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

In this operational guide you will find ways to optimize Materialize for:

- Speedup queries
- Save memory
- Minimize crash chances

## Speedup

Indexes are one of the fastest ways to optimize Materialize query speed. The following sections will display different indexes for everyday use cases in familiar scenarios.

Before continuing, consider the following example, a simple table with contacts data:

```sql
CREATE TABLE contacts (
    first_name TEXT,
    last_name TEXT,
    phone INT,
    prefix INT
);

-- Data sample:
INSERT INTO contacts
SELECT 'Charlie' as first_name,
      'Jones' as last_name,
      generate_series(1, 1000000) as phone,
      1 as prefix;
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

### WHERE

The filtering clause is one of the most used. Setting up an index on the filtered columns will increases the query speed.

#### Literal Values

Filtering by literal values is a typical case. An index over the filtered column will do the work for any literal value.

Back to the example, creating an index over the `phone` column will improve the speed of retrieving the contacts with a particular phone number (the literal value, `234722` in the example):

```sql
CREATE INDEX contacts_phone_idx ON contacts (phone);

-- Query performance improvement like:
SELECT *
FROM contacts
WHERE phone = 234722;
```

#### Expressions

Expressions can also be part of the index. Rather than calculating an expression's value on the fly in every query request, Materialize can store the expression result in the index and look it up, improving the query's performance.

E.g., Formatting a text is a common practice to match a particular search, like using a function to upper case the names. An index over the column with the `upper()` expression will speed up the query.

```sql
CREATE INDEX contacts_upper_first_name_idx ON contacts (upper(first_name));

SELECT *
FROM contacts
WHERE upper(first_name) = 'CHARLIE';
```

An _expression_ goes beyond a function. It could be a mathematical expression that is present in the filtering clause:

```sql
CREATE INDEX contacts_multiples_idx ON contacts (phone % 10000);

SELECT *
FROM contacts
WHERE phone % 100000 = 0;

-- A more complex expressions (Concat first name and phone):
CREATE INDEX contacts_complex_idx ON contacts (upper(first_name) || '-' || phone);

SELECT *
FROM contacts
WHERE upper(first_name) || '-' || phone = 'CHARLIE-1';
```

[Unmaterialized functions](/sql/functions/#unmaterializable-functions), like `now()` or `current_user()`, are not possible to use in an index due to their external dependencies.


#### Literal Values and Expressions

Creating a multi-column index makes it possible to combine both worlds, literal values and expressions.

```sql
CREATE INDEX contacts_upper_first_name_phone_idx ON contacts (upper(first_name), phone);

SELECT *
FROM contacts
WHERE upper(first_name) = 'CHARLIE' AND phone = 873090;
```

#### Multi-column index

When using a multi-column index, consider that only the queries using all the columns from the index will receive most of the improvement.

E.g., There is indecision about what to index by, and someone decides to create an index over all the columns.

```sql
-- Shorthand for: CREATE INDEX contacts_all_index ON contacts(first_name, last_name, phone, prefix);
CREATE DEFAULT INDEX ON contacts;

-- Improves a query using all the fields.
SELECT *
FROM contacts
WHERE first_name = 'Charlie' AND last_name = 'Jones' AND phone = 87309 AND prefix = 1;
```

<!-- Info: -->
<!-- Slack thread: https://materializeinc.slack.com/archives/C01BE3RN82F/p1658422234896599 -->
<!-- Join physical optimization: https://github.com/MaterializeInc/materialize/blob/main/src/transform/src/join_implementation.rs -->
<!-- Filtering physical optimization: https://github.com/MaterializeInc/materialize/blob/e94f1a02faa83014a23fcc801da28c3cbf61ce3a/src/compute-client/src/plan/mod.rs#L59 -->

The field order doesn't need to match the index.

### JOIN

Optimize the performance of `JOIN` on two relations by ensuring their
join keys are the key columns in an index.

E.g., We want to know very fast from which country a prefix is:

```sql
CREATE TABLE geo (country TEXT, prefix INT);

INSERT INTO geo SELECT 'Country', generate_series(1, 1000000);

CREATE INDEX contacts_prefix_idx ON contacts (prefix);
CREATE INDEX geo_prefix_idx ON geo (prefix);

SELECT phone, country, G.prefix
FROM geo G
JOIN contacts C ON C.prefix = G.prefix
WHERE phone = 442233;
```

In the above example, the index `contacts_prefix_idx`...

-   Helps because it contains a key the the query can
    use to look up values for the join condition (`contacts.prefix`).

    Because this index is exactly what the query requires, the Materialize
    optimizer will choose to use `contacts_prefix_idx` rather than build
    and maintain a private copy of the index just for this query.

-   Obeys the [restrictions](/sql/create-index/#restrictions) by containing only a subset of columns in the result
    set.

In the above example, the index `contacts_prefix_idx`...

-   Helps because it contains a key the the query can
    use to look up values for the join condition (`contacts.prefix`).

    Because this index is exactly what the query requires, the Materialize
    optimizer will choose to use `contacts_prefix_idx` rather than build
    and maintain a private copy of the index just for this query. -->

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

E.g., setting 8GB of memory and 20GB of swap:

```shell
docker run -p 6875:6875 --memory='8g' --memory-swap '28g' materialize/materialized:{{< version >}} --workers 1
```

While Materialize starts up, it will show the available amount of memory and swap:
```shell
memory: 10434412KB total, 1067521KB used, 8388608KiB limit
swap: 2147479KB total, 204931KB used, 20971520KiB limit
```

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
