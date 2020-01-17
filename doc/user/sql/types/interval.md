---
title: "interval Data Type"
description: "Expresses a duration of time"
menu:
    main:
        parent: "sql-types"
---

`interval` data expresses a duration of time.

| Detail           | Info                                                                                                 |
| ---------------- | ---------------------------------------------------------------------------------------------------- |
| **Quick Syntax** | `INTERVAL '1' MINUTE` <br/> `INTERVAL '1-2 3 4:5:6.7'` <br/>`INTERVAL '1 year 2.3 days 4.5 seconds'` |
| **Size**         | 32 bytes                                                                                             |
| **Min value**    | -9223372036854775807 months, -9223372036854775807 seconds, -999999999 nanoseconds                    |
| **Max value**    | 9223372036854775807 months, 9223372036854775807 seconds, 999999999 nanoseconds                       |

## Syntax

#### INTERVAL

{{< diagram "type-interval-val.html" >}}

#### `time_expr`

{{< diagram "type-interval-time-expr.html" >}}

#### `time_unit`

{{< diagram "time-units.html" >}}

| Field            | Definition                                                                                                                                                                                                                                                                |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| _head&lowbar;time&lowbar;unit_ | Return an interval without `time_unit`s larger than `head_time_unit`. Note that this differs from PostgreSQL's implementation, which ignores this clause.                                                                                                                 |
| _tail&lowbar;time&lowbar;unit_ | 1. Return an interval without `time_unit` smaller than `tail_time_unit`.<br/><br/>2. If the final `time_expr` is only a number, treat the `time_expr` as belonging to `tail_time_unit`. This is the case of the most common `interval` format like `INTERVAL '1' MINUTE`. |

## Details

### `time_expr` Syntax

Materialize strives for full PostgreSQL compatibility with `time_exprs`, which
offers support for two types of `time_expr` syntax:

-   SQL Standard, i.e. `'Y-M D H:M:S.NS'`
-   PostgreSQL, i.e. repeated `int.frac time_unit`, e.g.:
    -   `'1 year 2 months 3.4 days 5 hours 6 minutes 7.8 seconds'`
    -   `'1y 2mon 3.4d 5h 6m 7.8s'`

Like PostgreSQL, Materialize's implementation includes the following
stipulations:

-   You can freely mix SQL Standard- and PostgreSQL-style `time_expr`s.
-   You can write `time_expr`s in any order, e.g `'H:M:S.NS Y-M'`.
-   Each `time_unit` can only be written once.
-   SQL Standard `time_expr` uses the following groups of `time_unit`s:

    -   `Y-M`
    -   `D`
    -   `H:M:S.NS`

    Using a SQL Standard `time_expr` to write to any of these `time_units`
    writes to all other `time_units` in the same group, even if that `time_unit`
    is not explicitly referenced.

    For example, the `time_expr` `'1:2'` (1 hour, 2 minutes) also writes a value of
    0 seconds. You cannot then include another `time_expr` which writes to the
    seconds `time_unit`.

-   Only PostgreSQL `time_expr`s support non-second fractional `time_units`, e.g.
    `1.2 days`. Materialize only supports 9 places of decimal precision.

### Valid casts

`interval` does not support any casts.

## Examples

```sql
SELECT INTERVAL '1' MINUTE AS interval_m;
```

```nofmt
 interval_m
------------
 00:01:00
```

### SQL Standard syntax

```sql
SELECT INTERVAL '1-2 3 4:5:6.7' AS interval_p;
```

```nofmt
            interval_f
-----------------------------------
 1 year 2 months 3 days 04:05:06.7
```

### PostgreSQL syntax

```sql
SELECT INTERVAL '1 year 2.3 days 4.5 seconds' AS interval_p;
```

```nofmt
        interval_p
--------------------------
 1 year 2 days 07:12:04.5
```

### Negative intervals

`interval_n` demonstrates using negative and positive components in an interval.

```sql
SELECT INTERVAL '-1 day 2:3:4.5' AS interval_n;
```

```nofmt
 interval_n
-------------
 -21:56:55.5
```

### Truncating interval

`interval_r` demonstrates how `head_time_unit` and `tail_time_unit` truncate the
interval.

```sql
SELECT INTERVAL '1-2 3 4:5:6.7' DAY TO MINUTE AS interval_r;
```

```nofmt
   interval_r
-----------------
 3 days 04:05:00
```

### Complex example

`interval_w` demonstrates both mixing SQL Standard and PostgreSQL `time_expr`,
as well as using `tail_time_unit` to control the `time_unit` of the last value
of the `interval` string.

```sql
SELECT INTERVAL '1 day 2-3 4' MINUTE AS interval_w;
```

```nofmt
           interval_w
---------------------------------
 2 years 3 months 1 day 00:04:00
```

### Interaction with Timestamps

```sql
SELECT TIMESTAMP '2020-01-01 8:00:00' + INTERVAL '1' DAY AS ts_interaction;
```

```nofmt
        ts_interaction
-------------------------------
 2020-01-02 08:00:00.000000000
```
