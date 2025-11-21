<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Overview](/docs/transform-data/)
 /  [Patterns](/docs/transform-data/patterns/)

</div>

# Percentile calculation

Percentiles are a useful statistic to understand and interpret data
distribution. This pattern covers how to use histograms to efficiently
calculate percentiles in Materialize.

One way to compute percentiles is to order all values and pick the value
at the position of the corresponding percentile. However, this approach
requires storing all values, causing memory to grow linearly with the
number of tracked values.

Instead, more memory efficient alternatives are to use **histograms**
and **High Dynamic Range (HDR) histograms**:

- [Histograms](#using-histograms-to-compute-exact-percentiles) have a
  lower memory footprint that is linear to the number of *unique* values
  and can compute precise percentiles. However, for domains with *high*
  cardinality, calculating precise percentiles may be computationally
  expensive.

- [HDR
  histograms](#using-hdr-histograms-to-compute-approximate-percentiles)
  further reduce the memory footprint but computes approximate
  percentiles. Depending on the precision needed for the percentiles,
  HDR histograms may be preferred for domains with a high cardinality
  and dynamic range of values.

## Using histograms to compute exact percentiles

Histograms summarize data sets by grouping values into ranges and
counting how many elements fall into each range. From this summary, you
can get the percentile information by identifying the range where the
cumulative count crosses the desired percentile threshold. By grouping
each distinct value into its own range, you can get exact percentiles;
however, this can be computationally expensive if there are large number
of distinct values. Alternatively, you can get an approximate
percentiles by using [HDR
histograms](#using-hdr-histograms-to-compute-approximate-percentiles).

To use histograms to compute exact percentiles:

- First, create a histogram view that groups each distinct value into
  its own bucket and counts the number of each distinct value.

- Then, using a cross join on the histogram view, create a distribution
  view that calculates the cumulative density for a bucket by dividing
  the cumulative counts (sum of the counts for all buckets up to and
  including that bucket) by the total count.

  <div class="note">

  **NOTE:** The use of the cross join produces a number of outputs that
  is quadratic in the input. And, while the results will only be linear
  in size, it may take a disproportionate amount of time to produce and
  maintain.

  </div>

### Example

1.  Create a table `input`:

    <div class="highlight">

    ``` chroma
    CREATE TABLE input (value BIGINT);
    ```

    </div>

2.  Insert into the `input` table values `1` to `10`.

    <div class="highlight">

    ``` chroma
    INSERT INTO input
    SELECT n FROM generate_series(1,10) AS n;
    ```

    </div>

3.  Create a `histogram` view to track unique values from the `input`
    table and their count:

    <div class="highlight">

    ``` chroma
    CREATE VIEW histogram AS
    SELECT
      value AS bucket,
      count(*) AS count_of_bucket_values
    FROM input
    GROUP BY value;
    ```

    </div>

4.  Create a view `distribution` to calculate the cumulative count and
    the cumulative density for each bucket. The cumulative density is
    calculated by dividing the cumulative count for a bucket (i.e.,
    count for all bucket values up to and including that bucket) by the
    total count.

    <div class="highlight">

    ``` chroma
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

    </div>

    <div class="note">

    **NOTE:** The use of the cross join produces a number of outputs
    that is quadratic in the input. And, while the results will only be
    linear in size, it may take a disproportionate amount of time to
    produce and maintain.

    </div>

5.  You can then query `distribution` by the `cumulative_density` field
    to return specific percentiles. For example, the following query
    returns the 90-th percentile.

    <div class="highlight">

    ``` chroma
    SELECT bucket AS percentile90
    FROM distribution
    WHERE cumulative_density >= 0.9
    ORDER BY cumulative_density
    LIMIT 1;
    ```

    </div>

## Using HDR histograms to compute approximate percentiles

[HDR histograms](https://github.com/HdrHistogram/HdrHistogram) can be
used to approximate percentiles in a space efficient manner that scales
well even for large domains with many distinct values. HDR histograms
reduce the precision of values that are tracked and use buckets with
variable width. Buckets that are closer to 0 are smaller whereas buckets
far away from 0 are wider. This works particularly well for data that
exhibits a long tail of large values, e.g., latency measurements.

HDR histograms are related to how [floating point numbers are
represented](https://en.wikipedia.org/wiki/Double-precision_floating-point_format)
as integers. The underlying assumption is that smaller numbers require a
higher precision to be distinguishable (e.g. 5 ms and 6 ms are different
and should be in different buckets) whereas larger numbers can be
rounded more aggressively as their relative error becomes less relevant
(e.g. 10000 ms and 10001 ms are almost the same and can reside in the
same bucket).

In the example below, to reduce the number of buckets, the values are
first decomposed into `significand * 2^exponent`, and then with the
precision of the significand lowered, reconstructed for the respective
bucket value.

- With higher precisions, fewer items are kept in the same bucket and
  thus, more memory is required, but the approximate percentile becomes
  more precise.

- With lower precisions, more items are kept in the same bucket, and
  thus, the less memory is required, but the approximate percentile
  becomes less precise.

Except for the bucket calculation, the basic ideas of [using histograms
to compute percentiles](#using-histograms-to-compute-exact-percentiles)
remains the same for HDR histograms.

### Example

<div class="tip">

**💡 Tip:** The following example assumes you have not previously
created and populated the `input` table from the [Using histograms to
compute exact percentiles example](#example). If you have created and
populated the table, skip the corresponding steps.

</div>

1.  Create a table `input`:

    <div class="highlight">

    ``` chroma
    CREATE TABLE input (value BIGINT);
    ```

    </div>

2.  Insert into the `input` table values `1` to `10`.

    <div class="highlight">

    ``` chroma
    INSERT INTO input
    SELECT n FROM generate_series(1,10) AS n;
    ```

    </div>

3.  Create a `hdr_histogram` view. To reduce the number of buckets, the
    values are rounded down to the nearest multiple of 1/16.
    Specifically, the values are first decomposed into
    `significand * 2^exponent`. Then by reducing the precision of the
    significand to 1/16 (4 bits), the value is reconstructed to an
    approximated value.

    <div class="code-tabs">

    <div class="tab-content">

    <div id="tab-materialize-console" class="tab-pane"
    title="Materialize Console">

    <div class="highlight">

    ``` chroma
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

    </div>

    </div>

    <div id="tab-psql" class="tab-pane" title="psql">

    <div class="highlight">

    ``` chroma
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

    </div>

    </div>

    </div>

    </div>

4.  Create a view `hdr_distribution` to calculate the cumulative count
    and the cumulative density for each bucket. The cumulative density
    is calculated by dividing the cumulative count for a bucket (i.e.,
    count for all bucket values up to and including that bucket) by the
    total count.

    <div class="highlight">

    ``` chroma
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

    </div>

5.  You can then query `hdr_distribution` by the `cumulative_density`
    field to return *approximate* percentiles. More precisely, the query
    returns the lower bound for the percentile (the next larger bucket
    represents the upper bound).

    For example, the following query returns the lower bound for the
    90-th percentile.

    <div class="highlight">

    ``` chroma
    SELECT bucket AS approximate_percentile
    FROM hdr_distribution
    WHERE cumulative_density >= 0.9
    ORDER BY cumulative_density
    LIMIT 1;
    ```

    </div>

### HDR Histograms and approximate values

For small numbers, `distribution` and `hdr_distribution` are identical.
Even in `hdr_distribution`, all numbers from 1 to 10 are stored in their
own buckets. To verify, query `hdr_distribution`:

<div class="highlight">

``` chroma
SELECT * FROM hdr_distribution;
```

</div>

The query returns the following:

```
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

But if values grow larger, buckets can contain more than one value.
Let’s see what happens if more values are added to the `input` table.

<div class="highlight">

``` chroma
INSERT INTO input SELECT n FROM generate_series(11,10001) AS n;
```

</div>

Unlike the `distribution` view (used in the histogram approach) where
each bucket contains only a single value and has 10001 rows, a single
bucket in `hdr_distribution` can represent up to 512 distinct values and
has 163 rows:

<div class="highlight">

``` chroma
SELECT * FROM hdr_distribution ORDER BY cumulative_density;
```

</div>

The query returns the following:

```
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

When querying `hdr_distribution` for the 90-th percentile value:

<div class="highlight">

``` chroma
SELECT bucket AS approximate_percentile
FROM hdr_distribution
WHERE cumulative_density >= 0.9
ORDER BY cumulative_density
LIMIT 1;
```

</div>

The query returns an approximate percentile of `8704` (or more precisely
between `8704`and `9216`) whereas the precise percentile is `9001`.

```
 approximate_percentile
------------------------
                   8704
(1 row)
```

The precision of the approximation can be adapted by changing the
`precision` in the definition of `hdr_histogram`. The higher the
`precision`, the fewer items are kept in the same bucket and therefore
the more precise the approximate percentile becomes. The lower the
`precision`, the more items are kept in the same bucket and therefore
the less memory is required.

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/transform-data/patterns/percentiles.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
