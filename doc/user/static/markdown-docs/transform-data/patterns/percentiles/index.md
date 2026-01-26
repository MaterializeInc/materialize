# Percentile calculation

How to use histograms to efficiently calculate percentiles in Materialize.



Percentiles are a useful statistic to understand and interpret data distribution. This pattern covers how to use histograms to efficiently calculate percentiles in Materialize.

One way to compute percentiles is to order all values and pick the value at the
position of the corresponding percentile. However, this approach requires
storing all values, causing memory to grow linearly with the number of tracked
values.

Instead, more memory efficient alternatives are to use **histograms** and **High
Dynamic Range (HDR) histograms**:

- [Histograms](#using-histograms-to-compute-exact-percentiles) have a lower
  memory footprint that is linear to the number of _unique_ values and can
  compute precise percentiles. However, for domains with _high_ cardinality,
  calculating precise percentiles may be computationally expensive.

- [HDR histograms](#using-hdr-histograms-to-compute-approximate-percentiles)
  further reduce the memory footprint but computes approximate percentiles.
  Depending on the precision needed for the percentiles, HDR histograms may be
  preferred for domains with a high cardinality and dynamic range of values.

## Using histograms to compute exact percentiles

Histograms summarize data sets by grouping values into ranges and counting how
many elements fall into each range. From this summary, you can get the
percentile information by identifying the range where the cumulative count
crosses the desired percentile threshold. By grouping each distinct value into
its own range, you can get exact percentiles; however, this can be
computationally expensive if there are large number of distinct values.
Alternatively, you can get an approximate percentiles by using [HDR
histograms](#using-hdr-histograms-to-compute-approximate-percentiles).


To use histograms to compute exact percentiles:

- First, create a histogram view that groups each distinct value into its own
  bucket and counts the number of each distinct value.

- Then, using a cross join on the histogram view, create a distribution view
  that calculates the cumulative density for a bucket by dividing the cumulative
  counts (sum of the counts for all buckets up to and including that bucket) by
  the total count.

  > **Note:** The use of the cross join produces a number of outputs that is quadratic in
>   the input. And, while the results will only be linear in size, it may take a
>   disproportionate amount of time to produce and maintain.
>
>


### Example

1. Create a table `input`:

   ```mzsql
   CREATE TABLE input (value BIGINT);
   ```

2. Insert into the `input` table values `1` to `10`.

   ```mzsql
   INSERT INTO input
   SELECT n FROM generate_series(1,10) AS n;
   ```

1. Create a `histogram` view to track unique values from the
   `input` table and their count:

   ```mzsql
   CREATE VIEW histogram AS
   SELECT
     value AS bucket,
     count(*) AS count_of_bucket_values
   FROM input
   GROUP BY value;
   ```

1. Create a view `distribution` to calculate the cumulative count and the
   cumulative density for each bucket. The cumulative density is calculated by
   dividing the cumulative count for a bucket (i.e., count for all bucket values
   up to and including that bucket) by the total count.

   ```mzsql
   CREATE VIEW distribution AS
   SELECT
     h.bucket,
     h.count_of_bucket_values,
     sum(g.count_of_bucket_values) AS cumulative_count,
     sum(g.count_of_bucket_values) / (SELECT sum(count_of_bucket_values) FROM histogram) AS cumulative_density
   FROM histogram g, histogram h
   WHERE g.bucket <= h.bucket
   GROUP BY h.bucket, h.count_of_bucket_values
   ORDER BY cumulative_density;
   ```

   > **Note:** The use of the cross join produces a number of outputs that is quadratic in
>    the input. And, while the results will only be linear in size, it may take a
>    disproportionate amount of time to produce and maintain.
>
>


1. You can then query `distribution` by the `cumulative_density` field to
   return specific percentiles. For example, the following query returns the
   90-th percentile.

   ```mzsql
   SELECT bucket AS percentile90
   FROM distribution
   WHERE cumulative_density >= 0.9
   ORDER BY cumulative_density
   LIMIT 1;
   ```


## Using HDR histograms to compute approximate percentiles

[HDR histograms](https://github.com/HdrHistogram/HdrHistogram) can be used to approximate percentiles in a space efficient manner that scales well even for large domains with many distinct values. HDR histograms reduce the precision of values that are tracked and use buckets with variable width. Buckets that are closer to 0 are smaller whereas buckets far away from 0 are wider. This works particularly well for data that exhibits a long tail of large values, e.g., latency measurements.

HDR histograms are related to how [floating point numbers are
represented](https://en.wikipedia.org/wiki/Double-precision_floating-point_format)
as integers. The underlying assumption is that smaller numbers require a higher
precision to be distinguishable (e.g. 5 ms and 6 ms are different and should be
in different buckets) whereas larger numbers can be rounded more aggressively as
their relative error becomes less relevant (e.g. 10000 ms and 10001 ms are
almost the same and can reside in the same bucket).

In the example below, to reduce the number of buckets, the values are first
decomposed into `significand * 2^exponent`, and then with the precision of the
significand lowered, reconstructed for the respective bucket value.

- With higher precisions, fewer items are kept in the same bucket and thus, more
  memory is required, but the approximate percentile becomes more precise.

- With lower precisions, more items are kept in the same bucket, and thus, the
  less memory is required, but the approximate percentile becomes less precise.

Except for the bucket calculation, the basic ideas of [using histograms to
compute percentiles](#using-histograms-to-compute-exact-percentiles) remains the
same for HDR histograms.

### Example

> **Tip:** The following example assumes you have not previously created and populated the
> `input` table from the [Using histograms to compute exact percentiles
> example](#example). If you have created and populated the table, skip the
> corresponding steps.
>
>


1. Create a table `input`:

   ```mzsql
   CREATE TABLE input (value BIGINT);
   ```

2. Insert into the `input` table values `1` to `10`.

   ```mzsql
   INSERT INTO input
   SELECT n FROM generate_series(1,10) AS n;
   ```

1. Create a `hdr_histogram` view. To reduce the number of buckets, the values
   are rounded down to the nearest multiple of 1/16. Specifically, the values
   are first decomposed into `significand * 2^exponent`. Then by reducing the
   precision of the significand to 1/16 (4 bits), the value is reconstructed to
   an approximated value.



   **Materialize Console:**

```mzsql
CREATE VIEW hdr_histogram AS
WITH
  input_parts AS (
    SELECT
      CASE WHEN value = 0 THEN NULL
          ELSE trunc(log(2, abs(value)))::int
      END AS exponent,
      CASE WHEN value = 0 THEN NULL
          ELSE value / pow(2.0, trunc(log(2, abs(value)))::int)
      END AS significand
    FROM input
  ),
  buckets AS (
    -- reduce precision by 4 bits to round down the value to the nearest multiple of 1/16
    SELECT
      trunc(significand * pow(2.0, 4)) / pow(2.0, 4)
        * pow(2.0, exponent)
        AS bucket
    FROM input_parts
  )
SELECT
  COALESCE(bucket, 0) AS bucket,
  count(*) AS count_of_bucket_values
FROM buckets
GROUP BY bucket;
```



   **psql:**

```mzsql
-- precision for the representation of the significand in bits
\set precision 4

CREATE VIEW hdr_histogram AS
WITH
  input_parts AS (
    SELECT
      CASE WHEN value = 0 THEN NULL
          ELSE trunc(log(2, abs(value)))::int
      END AS exponent,
      CASE WHEN value = 0 THEN NULL
          ELSE value / pow(2.0, trunc(log(2, abs(value)))::int)
      END AS significand
    FROM input
  ),
  buckets AS (
    -- reduce precision by 4 bits to round down the value to the nearest multiple of 1/16
    SELECT
      trunc(significand * pow(2.0, :precision)) / pow(2.0, :precision)
        * pow(2.0, exponent)
        AS bucket
    FROM input_parts
  )
SELECT
  COALESCE(bucket, 0) AS bucket,
  count(*) AS count_of_bucket_values
FROM buckets
GROUP BY bucket;
```



1. Create a view `hdr_distribution` to calculate the cumulative count and the
   cumulative density for each bucket. The cumulative density is calculated by
   dividing the cumulative count for a bucket (i.e., count for all bucket values
   up to and including that bucket) by the total count.

   ```mzsql
   CREATE VIEW hdr_distribution AS
   SELECT
     h.bucket,
     h.count_of_bucket_values,
     sum(g.count_of_bucket_values) AS cumulative_count,
     sum(g.count_of_bucket_values) / (SELECT sum(count_of_bucket_values) FROM hdr_histogram) AS  cumulative_density
   FROM hdr_histogram g, hdr_histogram h
   WHERE g.bucket <= h.bucket
   GROUP BY h.bucket, h.count_of_bucket_values;
   ```

1. You can then query `hdr_distribution` by the `cumulative_density` field
   to return _approximate_ percentiles. More precisely, the query returns the
   lower bound for the percentile (the next larger bucket represents the upper
   bound).

   For example, the following query returns the lower bound for the 90-th
   percentile.

   ```mzsql
   SELECT bucket AS approximate_percentile
   FROM hdr_distribution
   WHERE cumulative_density >= 0.9
   ORDER BY cumulative_density
   LIMIT 1;
   ```

### HDR Histograms and approximate values

For small numbers, `distribution` and `hdr_distribution` are identical. Even in
`hdr_distribution`, all numbers from 1 to 10 are stored in their own buckets. To
verify, query `hdr_distribution`:

```mzsql
SELECT * FROM hdr_distribution;
```

The query returns the following:

```none
 bucket | frequency | cumulative_count     | cumulative_density
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

```mzsql
INSERT INTO input SELECT n FROM generate_series(11,10001) AS n;
```

Unlike the `distribution` view (used in the histogram approach) where each
bucket contains only a single value and has 10001 rows, a single bucket in
`hdr_distribution` can represent up to 512 distinct values and has 163 rows:

```mzsql
SELECT * FROM hdr_distribution ORDER BY cumulative_density;
```

The query returns the following:

```none
 bucket | frequency | cumulative_count     | cumulative_density
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

When querying `hdr_distribution`  for the 90-th percentile value:

```mzsql
SELECT bucket AS approximate_percentile
FROM hdr_distribution
WHERE cumulative_density >= 0.9
ORDER BY cumulative_density
LIMIT 1;
```

The query returns an approximate
percentile of `8704` (or more precisely between `8704`and `9216`) whereas the
precise percentile is `9001`.

```none
 approximate_percentile
------------------------
                   8704
(1 row)
```

The precision of the approximation can be adapted by changing the `precision` in the definition of `hdr_histogram`. The higher the `precision`, the fewer items are kept in the same bucket and therefore the more precise the approximate percentile becomes. The lower the `precision`, the more items are kept in the same bucket and therefore the less memory is required.
