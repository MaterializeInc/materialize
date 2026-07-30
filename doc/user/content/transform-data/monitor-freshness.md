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
  several seconds, during which the Console becomes unresponsive. Reach for this
  relation only when you specifically need data older than 24 hours, and avoid
  querying it frequently.

{{< /note >}}

## Summarize freshness with a CCDF

A raw lag time series is hard to summarize. A **complementary cumulative
distribution function (CCDF)** compresses a whole window of lag observations
into a single curve that answers one question: for a given lag threshold `X`,
what fraction of the time was the object's lag at or above `X`? It is the
complement of the ordinary cumulative distribution, so `CCDF(X) = 1 - CDF(X)`.

This is the compact way to describe a freshness distribution. Instead of staring
at a time series, you can make statements like "lag exceeded 10 seconds only 1%
of the time". Because latency spans many orders of magnitude, the threshold is
bucketed on a log scale so the tail (the rare, large lags you care about most)
stays visible.

The following query builds a freshness CCDF for a single object from the last 24
hours of history. It reads the fast, indexed
`mz_internal.mz_wallclock_global_lag_recent_history`, buckets each observation on
a log scale (100 buckets per doubling of lag), and then sums the tail of the
histogram to produce `(lag_threshold_seconds, fraction_of_time_at_or_above)`
pairs. Replace `<your_mv_name>` with the name of your object:

```mzsql
WITH lags AS (
    -- Convert each lag to seconds, dropping unhydrated (NULL) observations and
    -- any non-positive lag, since the log scale is undefined at or below zero.
    SELECT extract(epoch FROM wl.lag) AS lag_seconds
    FROM mz_internal.mz_wallclock_global_lag_recent_history wl
    JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
    WHERE o.name = '<your_mv_name>'
      AND wl.lag IS NOT NULL
      AND wl.lag > INTERVAL '0'
),
histogram AS (
    -- Log-scale bucket, labeled back in seconds: 100 buckets per doubling of lag.
    SELECT
        pow(2.0, round(100 * log(2, lag_seconds)) / 100.0) AS lag_bucket,
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

Run globally across every object (that is, with the `o.name` filter removed),
the query returns output like the following:

```none
          lag_threshold_seconds           | fraction_of_time_at_or_above
------------------------------------------+------------------------------
                                        1 |                            1
                                        2 |           0.6438906752411575
 2.98969849726987683297067456375109798188 |          0.05787781350482315
(3 rows)
```

Read this as: lag was at or above 1 second 100% of the time, at or above 2
seconds about 64% of the time, and at or above roughly 3 seconds only about 6%
of the time.

{{< note >}}
The output above comes from a lightly loaded instance whose objects sit at a
low, near-constant lag, so the curve is short and drops off within a few
seconds. A production instance under real load produces a longer tail with more
buckets extending into the higher lag thresholds.
{{< /note >}}

The log-scale bucketing is the same idea as the HDR histogram in the
[Percentile calculation](/transform-data/patterns/percentiles/) pattern, and the
final cross join is the same cumulative transform, flipped from `<=` to `>=` so
it sums the tail rather than the head. The cross join is quadratic in the number
of buckets, but the log bucketing keeps the bucket count small, so this stays
cheap. To get a single global curve or a per-cluster curve, drop the object
filter and add the grouping columns you want.

{{< note >}}

How to read and use a freshness CCDF:

- The **x-axis is the lag threshold** in seconds, on a log scale. The **y-axis
  is the fraction of the window** the object spent at or above that lag.

- A **healthy** object produces a curve that drops off early and hugs low lag
  values. Almost all of the time, lag is small. An **unhealthy** object produces
  a long, flat tail that extends into minutes or hours, meaning the object is
  frequently far behind.

- To use it against an SLO, pick your target lag (say 10 seconds) and read the
  fraction of time at or above it. That fraction is how often you were violating
  the SLO over the window.

- Exclude or expect a few artifacts. NULL lag rows are unhydrated observations
  and are already filtered out above. If a spurious shelf appears far out at
  roughly `1.76e9` seconds (about 56 years), it comes from unhydrated
  collections reported at the Unix epoch. Filter it out (for example with `AND
  wl.lag < INTERVAL '1 year'`) so it does not distort the curve.

{{< /note >}}
