---
title: "Percentile calculation"
description: "How to use histograms to efficiently calculate percentiles in Materialize."
aliases:
  - /sql/patterns/percentiles/
menu:
  main:
    parent: 'sql-patterns'
---

Percentiles are a useful statistic to understand and interpret data distribution. This pattern covers how to use histograms to efficiently calculate percentiles in Materialize.

A naive way to compute percentiles is to order all values and pick the value at the position of the corresponding percentile. This way of computing percentiles keeps all values around and therefore demands memory to grow linearly with the number of tracked values. Two better approaches to reduce the memory footprint are: histograms and High Dynamic Range (HDR) histograms.

Histograms have a lower memory footprint, linear to the number of _unique_ values, and computes precise percentiles. HDR histograms further reduce the memory footprint but at the expense of computing approximate percentiles. They are particularly interesting if there is a long tail of large values that you want to track, which is often the case for latency measurements.

## Using histograms to compute percentiles

Histograms reduce the memory footprint by tracking a count for each unique value, in a `bucket`, instead of tracking all values. Given an `input`, define the histogram for `values` as follows:

```sql
CREATE VIEW histogram AS
SELECT
  value AS bucket,
  count(*) AS frequency
FROM input
GROUP BY value;
```

To query percentiles from the view `histogram`, it's no longer possible to just order the `values` and pick a value from the right spot. Instead, the distribution of values needs to be reconstructed. It is done by determining the cumulative count (the sum of counts up through each bucket that came before) for each `bucket`. This is accomplished through a cross-join in the following view:

```sql
CREATE VIEW distribution AS
SELECT
  h.bucket,
  h.frequency,
  sum(g.frequency) AS cumulative_frequency,
  sum(g.frequency) / (SELECT sum(frequency) FROM histogram) AS cumulative_distribution
FROM histogram g, histogram h
WHERE g.bucket <= h.bucket
GROUP BY h.bucket, h.frequency
ORDER BY cumulative_distribution;
```

The cumulative count and the cumulative distribution can then be used to query for arbitrary percentiles. The following query returns the 90-th percentile.

```sql
SELECT bucket AS percentile
FROM distribution
WHERE cumulative_distribution >= 0.9
ORDER BY cumulative_distribution
LIMIT 1;
```

To increase query performance, it can make sense to keep the `distribution` always up to date by creating an index on the view:

```sql
CREATE INDEX distribution_idx ON distribution (cumulative_distribution);
```

Histograms work well for a domain with low cardinality. But note the cross join in the definition of `distribution`. As a consequence, the required memory grows quadratic in the number of unique values and may therefore grow large if the domain has many unique values.


## Using HDR histograms to compute approximate percentiles

[HDR histograms](https://github.com/HdrHistogram/HdrHistogram) can be used to approximate percentiles in a space efficient manner that scales well even for large domains with many distinct values. HDR histograms reduce the precision of values that are tracked and use buckets with variable width. Buckets that are closer to 0 are smaller whereas buckets far away from 0 are wider. This works particularly well for data that exhibits a long tail of large values, e.g., latency measurements.

HDR histograms are related to how [floating point numbers are represented](https://en.wikipedia.org/wiki/Double-precision_floating-point_format) as integers. The underlying assumption is that smaller numbers require a higher precision to be distinguishable (e.g. 5 ms and 6 ms are different and should be in different buckets) whereas larger numbers can be rounded more aggressively as their relative error becomes less relevant (e.g. 10000 ms and 10001 ms are almost the same and can reside in the same bucket).

The following snippets reduce the precision of numbers to limit the amount of required buckets. Numbers are decomposed into their floating point representation

	n = sign * mantissa * 2**exponent

and the precision of the mantissa is then reduced to compute the respective bucket.

The basic ideas of using histograms to compute percentiles remain the same; but determining the bucket becomes more involved because it’s now composed of the triple (sign, mantissa, exponent).

```sql
-- precision for the representation of the mantissa in bits
\set precision 4

CREATE VIEW hdr_histogram AS
SELECT
  CASE WHEN value<0 THEN -1 ELSE 1 END AS sign,
  trunc(log(2.0, abs(value)))::int AS exponent,
  trunc(pow(2.0, :precision) * (value / pow(2.0, trunc(log(2.0, abs(value)))::int) - 1.0))::int AS mantissa,
  count(*) AS frequency
FROM input
GROUP BY sign, exponent, mantissa;
```

The `hdr_distribution` view below reconstructs the `bucket` (with reduced precision), and determines the cumulative count and cumulative distribution.

```sql
CREATE VIEW hdr_distribution AS
SELECT
  h.sign*(1.0+h.mantissa/pow(2.0, :precision))*pow(2.0,h.exponent) AS bucket,
  h.frequency,
  sum(g.frequency) AS cumulative_frequency,
  sum(g.frequency) / (SELECT sum(frequency) FROM hdr_histogram) AS cumulative_distribution
FROM hdr_histogram g, hdr_histogram h
WHERE (g.sign,g.exponent,g.mantissa) <= (h.sign,h.exponent,h.mantissa)
GROUP BY h.sign, h.exponent, h.mantissa, h.frequency
ORDER BY cumulative_distribution;
```

This view can then be used to query _approximate_ percentiles. More precisely, the query returns the lower bound for the percentile (the next larger bucket represents the upper bound).

```sql
SELECT bucket AS approximate_percentile
FROM hdr_distribution
WHERE cumulative_distribution >= 0.9
ORDER BY cumulative_distribution
LIMIT 1;
```

As with histograms, increase query performance by creating an index on the `cumulative_distribution` column.

```sql
CREATE INDEX hdr_distribution_idx ON hdr_distribution (cumulative_distribution);
```


## Examples

```sql
CREATE TABLE input (value BIGINT);
```

Let's add the values 1 to 10 into the `input` table.

```sql
INSERT INTO input SELECT n FROM generate_series(1,10) AS n;
```

For small numbers, `distribution` and `hdr_distribution` are identical. Even in `hdr_distribution`, all numbers from 1 to 10 are stored in their own buckets.

```sql
SELECT * FROM hdr_distribution;

 bucket | frequency | cumulative_frequency | cumulative_distribution
--------+-----------+----------------------+-------------------------
      1 |         1 |                    1 |                     0.1
      2 |         1 |                    2 |                     0.2
      3 |         1 |                    3 |                     0.3
      4 |         1 |                    4 |                     0.4
      5 |         1 |                    5 |                     0.5
      6 |         1 |                    6 |                     0.6
      7 |         1 |                    7 |                     0.7
      8 |         1 |                    8 |                     0.8
      9 |         1 |                    9 |                     0.9
     10 |         1 |                   10 |                       1
(10 rows)
```

But if values grow larger, buckets can contain more than one value. Let's see what happens if more values are added to the `input` table.

```sql
INSERT INTO input SELECT n FROM generate_series(11,10001) AS n;
```

In the case of the `hdr_distribution`, a single bucket represents up to 512 distinct values, whereas each bucket of the `distribution` contains only a single value.

```sql
SELECT * FROM hdr_distribution ORDER BY cumulative_distribution;

 bucket | frequency | cumulative_frequency |          cumulative_distribution
--------+-----------+----------------------+-------------------------------------------
      1 |         1 |                    1 |     0.00000999990000099999000009999900001
      2 |         1 |                    2 |     0.00001999980000199998000019999800002
      3 |         1 |                    3 |     0.00002999970000299997000029999700003
      4 |         1 |                    4 |     0.00003999960000399996000039999600004
      5 |         1 |                    5 |     0.00004999950000499995000049999500005
...skipping...
   7424 |       256 |                 7679 | 0.767823217678232176782321767823217678232
   7680 |       256 |                 7935 | 0.793420657934206579342065793420657934207
   7936 |       256 |                 8191 | 0.819018098190180981901809819018098190181
   8192 |       512 |                 8703 |  0.87021297870212978702129787021297870213
   8704 |       512 |                 9215 | 0.921407859214078592140785921407859214079
   9216 |       512 |                 9727 | 0.972602739726027397260273972602739726027
   9728 |       274 |                10001 |                                         1
(163 rows)
```

Note that `hdr_distribution` only contains 163 rows as opposed to the 10001 rows of `distribution`, which is used in the histogram approach. However, when querying for the 90-th percentile, the query returns an approximate percentile of `8704` (or more precisely between `8704`and `9216`) whereas the precise percentile is `9001`.

```sql
SELECT bucket AS approximate_percentile
FROM hdr_distribution
WHERE cumulative_distribution >= 0.9
ORDER BY cumulative_distribution
LIMIT 1;

 approximate_percentile
------------------------
                   8704
(1 row)
```

The precision of the approximation can be adapted by changing the `precision` in the definition of `hdr_histogram`. The higher the `precision`, the fewer items are kept in the same bucket and therefore the more precise the approximate percentile becomes. The lower the `precision`, the more items are kept in the same bucket and therefore the less memory is required.
