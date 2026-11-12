---
title: "Hot and cold data"
description: "Keep recent data in memory for fast, fresh queries, and keep older data in cheaper storage."
menu:
  main:
    parent: 'sql-patterns'
---

Many workloads need sub-second, up-to-date results on recent data (**hot
data**) but query older data (**cold data**) rarely, and can tolerate slower
queries on it. Indexing all of your data in Materialize keeps every row in
cluster memory, so cost grows with your total history rather than with the data
you actually serve.

The approaches on this page keep only hot data in an
[index](/fundamentals/concepts/indexes/), and keep cold data out of cluster memory.

| Approach | Cold data lives in | Cold queries run in | Extra moving parts |
|----------|--------------------|---------------------|--------------------|
| [Index only hot data](#index-only-hot-data) | Materialize storage | Materialize, computed on demand | None |
| [Freeze results for cold data](#freeze-results-for-cold-data) | A Materialize table | Materialize, precomputed | A scheduled job |
| [Archive cold data to Iceberg](#archive-cold-data-to-iceberg) | Apache Iceberg | Another query engine | Query routing |

If you don't need cold data at all, use a [temporal
filter](/transform-data/patterns/temporal-filters/) on its own.

The examples on this page use the following `events` table, which stands in
for a source or table that contains your full history:

```mzsql
CREATE TABLE events (
  event_id bigint,
  user_id bigint,
  amount numeric,
  event_ts timestamptz
);
```

## Index only hot data

Use a [temporal filter](/transform-data/patterns/temporal-filters/) to define
the hot window, and index only that view. Query cold data directly from the
underlying source or table, which Materialize keeps in object storage.

```mzsql
-- Hot: the last 30 days, kept in memory and updated as data arrives.
CREATE VIEW events_hot AS
SELECT *
FROM events
WHERE mz_now() <= event_ts + INTERVAL '30 days';

CREATE INDEX events_hot_user_id_idx ON events_hot (user_id);
```

Queries against `events_hot` are served from the index. Queries against
`events` that fall outside the hot window build a temporary dataflow, so they
are slower and use cluster resources while they run:

```mzsql
SELECT user_id, sum(amount) AS total
FROM events
WHERE event_ts >= now() - INTERVAL '90 days'
  AND event_ts < now() - INTERVAL '30 days'
GROUP BY user_id;
```

To keep cold queries fast and cheap:

- Filter on the time column so that [filter
  pushdown](/transform-data/patterns/partition-by/#filter-pushdown) can skip
  data outside the requested range.
- Run cold queries on a separate cluster, so that they don't compete for
  resources with the cluster that serves hot data.
- Don't create indexes or materialized views on the unfiltered `events`
  relation. They hold or recompute all of your history.

If your history isn't in Materialize, load it into a table with [`COPY
FROM`](/sql/copy-from/) and leave the table unindexed. If you want a single
relation for applications to query, combine the hot view and the cold table
with `UNION ALL` in a view, and don't index that view.

## Freeze results for cold data

Use this approach when your queries read results computed over the data, such
as daily aggregates, rather than the raw rows. Results for older time periods
stop changing, so you compute them once, store them in an unindexed table, and
incrementally maintain only the results for recent data.

A one-row table holds the cutoff between cold and hot results. Each view
filters on the cutoff, so a row is always in exactly one of the two sets.

```mzsql
CREATE TABLE cold_cutoff (cutoff timestamptz);
INSERT INTO cold_cutoff VALUES ('2026-11-01');

CREATE TABLE daily_revenue_cold (day timestamptz, revenue numeric);

-- Hot results, maintained incrementally and served from memory.
CREATE VIEW daily_revenue_hot AS
SELECT date_trunc('day', e.event_ts) AS day, sum(e.amount) AS revenue
FROM events AS e, cold_cutoff AS c
WHERE e.event_ts >= c.cutoff
  AND mz_now() <= e.event_ts + INTERVAL '30 days'
GROUP BY 1;

CREATE INDEX daily_revenue_hot_day_idx ON daily_revenue_hot (day);

-- All results. Don't index this view.
CREATE VIEW daily_revenue AS
SELECT day, revenue FROM daily_revenue_hot
UNION ALL
SELECT r.day, r.revenue
FROM daily_revenue_cold AS r, cold_cutoff AS c
WHERE r.day < c.cutoff;
```

The temporal filter bounds the memory that `daily_revenue_hot` uses. Keep the
cutoff inside the temporal filter's window, or results between the cutoff and
the start of the window are missing from `daily_revenue`. Load results from
before the initial cutoff with step 2 of [Advance the
cutoff](#advance-the-cutoff).

Frozen results don't change. If late-arriving data can land in a period you
have already frozen, freeze only periods older than your latest expected
arrival.

### Advance the cutoff

Run the following steps on a schedule, for example daily, with an external
scheduler. The example freezes results up to `'2026-11-08'`, which must align
with the `date_trunc` unit.

1. Remove any results left by a previous failed run, so that the job is safe
   to retry:

   ```mzsql
   DELETE FROM daily_revenue_cold
   WHERE day >= (SELECT cutoff FROM cold_cutoff);
   ```

1. Compute the results for the period to freeze and load them into the cold
   table. `INSERT ... SELECT` can only read from tables, so if `events` is a
   source, export the results with [`COPY TO`](/sql/copy-to/) and load them
   with [`COPY FROM`](/sql/copy-from/):

   ```mzsql
   COPY (
     SELECT date_trunc('day', event_ts) AS day, sum(amount) AS revenue
     FROM events
     WHERE event_ts >= '2026-11-01' -- The current cutoff.
       AND event_ts < '2026-11-08'  -- The new cutoff.
     GROUP BY 1
   ) TO 's3://my-bucket/daily_revenue/2026-11-08'
   WITH (AWS CONNECTION = aws_connection, FORMAT = 'parquet');

   COPY INTO daily_revenue_cold
   FROM 's3://my-bucket/daily_revenue/2026-11-08'
   (FORMAT PARQUET, AWS CONNECTION = aws_connection, PATTERN = '*.parquet');
   ```

   The loaded rows are not yet visible in `daily_revenue`, because they are
   later than the current cutoff.

1. Advance the cutoff. This single statement moves the period from the hot
   results to the cold results atomically:

   ```mzsql
   UPDATE cold_cutoff SET cutoff = '2026-11-08';
   ```

## Archive cold data to Iceberg

Use this approach when another engine, such as a data warehouse or lakehouse,
should serve historical queries. Send your data in two directions:

- **Hot:** index a view with a temporal filter, as in [Index only hot
  data](#index-only-hot-data).
- **Cold:** sink the full, unfiltered relation to an [Iceberg
  sink](/export-data/iceberg/) in append mode.

```mzsql
CREATE SINK events_archive
  FROM events
  INTO ICEBERG CATALOG CONNECTION iceberg_catalog_connection (
    NAMESPACE = 'analytics',
    TABLE = 'events'
  )
  MODE APPEND
  WITH (COMMIT INTERVAL = '5m');
```

Sink the input relation, not the temporal-filtered view. When a row leaves a
temporal filter's window, Materialize retracts it, and the sink writes that
retraction to Iceberg.

Applications send queries on recent data to Materialize and queries on older
data to the engine that reads the Iceberg table. To expose one endpoint,
put a service or query proxy in front of both systems that routes each query by
its time range.

## Related pages

- [Temporal filters](/transform-data/patterns/temporal-filters/)
- [Partitioning and filter pushdown](/transform-data/patterns/partition-by/)
- [`COPY FROM`](/sql/copy-from/)
- [`COPY TO`](/sql/copy-to/)
- [Iceberg sinks](/export-data/iceberg/)
