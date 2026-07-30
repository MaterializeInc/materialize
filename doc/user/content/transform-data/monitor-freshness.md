---
title: "Monitor freshness"
description: "How to monitor freshness for an object over time in Materialize."
menu:
  main:
    name: "Monitor freshness"
    identifier: monitor-freshness
    parent: transform-data
    weight: 84
---

[Freshness](/concepts/reaction-time/#freshness) measures the time from when a
change occurs in an upstream system to when it becomes visible in the results of
a query. This guide shows how to track freshness for an object over time and how
to summarize a whole window of freshness observations into a single curve.

If freshness is worse than expected, see [Freshness
troubleshooting](/transform-data/freshness-troubleshooting/) to diagnose and
resolve the cause.

## Track freshness over time

To track freshness for a specific object over time, query its wallclock lag
history. The following query returns the last 6 hours of wallclock lag for a
materialized view (replace `<your_mv_name>` with the name of your object):

```mzsql
SELECT wl.occurred_at, wl.lag
FROM mz_internal.mz_wallclock_global_lag_recent_history wl
JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
WHERE o.name = '<your_mv_name>'
  AND wl.occurred_at > now() - INTERVAL '6 hours'
ORDER BY wl.occurred_at DESC;
```

For example, for a materialized view named `freshness_demo`, the query returns
output like the following:

```none
      occurred_at       |   lag
------------------------+----------
 2026-07-30 19:38:00+00 | 00:00:02
 2026-07-30 19:37:00+00 | 00:00:02
 2026-07-30 19:36:00+00 | 00:00:02
 2026-07-30 19:35:00+00 | 00:00:02
 2026-07-30 19:34:00+00 | 00:00:02
(5 rows)
```

Each row is one minute-binned observation of the object's wallclock lag, most
recent first. Here lag holds steady at about two seconds, which is the expected,
healthy pattern for a lightly loaded object.

{{< note >}}

Materialize exposes wallclock lag history through two relations. Which one you
query has a large impact on performance.

- [`mz_internal.mz_wallclock_global_lag_recent_history`](/reference/system-catalog/mz_internal/#mz_wallclock_global_lag_recent_history)
  is indexed and holds only the past 24 hours of data. Querying it is fast, so
  it is the right choice for frequent or interactive monitoring and for
  dashboards. Use this relation by default, as in the query above.

- [`mz_internal.mz_wallclock_global_lag_history`](/reference/system-catalog/mz_internal/#mz_wallclock_global_lag_history)
  covers the full retention window (at least 30 days) but is unindexed, so it
  can be slow to query. A single query can occupy `mz_catalog_server` for
  several seconds. Reach for this
  relation only when you specifically need data older than 24 hours, and avoid
  querying it frequently.

{{< /note >}}

## Summarize freshness with a CCDF

A raw time series is hard to summarize. A **complementary cumulative
distribution function (CCDF)** compresses a whole window of freshness observations
into a single curve that answers one question: for a given threshold `X`,
what fraction of the time was the object's freshness at or above `X`?

This is the compact way to describe a freshness distribution. Instead of staring
at a time series, you can make statements like "the p99 freshness was 1s".

The following query builds a freshness CCDF across every object from the last 24
hours of history. It reads the fast, indexed
`mz_internal.mz_wallclock_global_lag_recent_history`, buckets each observation
into decade (power-of-ten) buckets, and then sums the tail of the histogram to
produce `(lag_threshold_seconds, fraction_of_time_at_or_above)` pairs:

```mzsql
WITH lags AS (
    -- Convert each lag to seconds, dropping unhydrated (NULL) observations and
    -- any non-positive lag, since the log scale is undefined at or below zero.
    SELECT extract(epoch FROM wl.lag) AS lag_seconds
    FROM mz_internal.mz_wallclock_global_lag_recent_history wl
    WHERE wl.lag IS NOT NULL
      AND wl.lag > INTERVAL '0'
),
histogram AS (
    -- Decade bucket: floor each lag to its power of ten, so lags land in
    -- 0.1, 1, 10, 100, ... second buckets ([1, 10) -> 1, [10, 100) -> 10).
    SELECT
        pow(10.0, floor(log10(lag_seconds))) AS lag_bucket,
        count(*) AS frequency
    FROM lags
    GROUP BY 1
)
SELECT
    h.lag_bucket AS lag_threshold_seconds,
    sum(g.frequency)::float8
        / (SELECT sum(frequency) FROM histogram) AS fraction_of_time_at_or_above
FROM histogram g, histogram h
WHERE g.lag_bucket >= h.lag_bucket   -- complement: sum the tail at or above the threshold
GROUP BY h.lag_bucket
ORDER BY h.lag_bucket;
```

The query returns output like the following:

```none
 lag_threshold_seconds | fraction_of_time_at_or_above
-----------------------+------------------------------
                     1 |                            1
(1 row)
```

Read this as: every observation of positive lag fell in the 1-second decade
bucket, so lag was at or above 1 second 100% of the time and never reached the
10-second bucket. The curve is a single step, the healthy shape for an instance
whose objects all sit at a low, near-constant lag.

![Freshness CCDF plotted from the sample output: the fraction of time at or above is 1.0 at the 1-second threshold and drops to 0 above 10 seconds, forming a single step](/images/freshness-ccdf-sample.png)

The chart above is generated from the sample output, captured on a lightly
loaded local instance where every observation landed in the 1-second bucket. A
production instance under real load shows a longer tail, with points extending
into the 10-second and 100-second thresholds.

By default this query aggregates across every object. To scope the CCDF to a
single object, add a join to `mz_catalog.mz_objects` and a name filter to the
`lags` CTE (replace `<your_mv_name>` with the name of your object):

```mzsql
    FROM mz_internal.mz_wallclock_global_lag_recent_history wl
    JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
    WHERE o.name = '<your_mv_name>'
      AND wl.lag IS NOT NULL
      AND wl.lag > INTERVAL '0'
```

Reading a freshness CCDF is straightforward once you know what the axes mean.
The **x-axis** is the lag threshold in seconds, on a log scale, and the
**y-axis** is the fraction of the window the object spent at or above that lag.

A **healthy** object produces a curve that drops off early and hugs low lag
values, so almost all of the time its lag is small. An **unhealthy** object
produces a long, flat tail that extends into minutes or hours, which means the
object is frequently far behind.

To read the curve against an SLO, pick your target lag (say 10 seconds) and read
the fraction of time at or above it. That fraction is how often the object was
violating the SLO over the window.

Expect a few artifacts in the data. NULL lag rows are unhydrated observations,
and the query above already filters them out. If a spurious shelf appears far
out at roughly `1.76e9` seconds (about 56 years), it comes from unhydrated
collections reported at the Unix epoch, so filter it out (for example with `AND
wl.lag < INTERVAL '1 year'`) so it does not distort the curve.
