<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)  /  [SQL data
types](/docs/self-managed/v25.2/sql/types/)

</div>

# numeric type

`numeric` data expresses an exact number with user-defined precision and
scale.

| Detail           | Info                 |
|------------------|----------------------|
| **Size**         | ~32 bytes            |
| **Aliases**      | `dec`, `decimal`     |
| **Catalog name** | `pg_catalog.numeric` |
| **OID**          | 1700                 |
| **Precision**    | 39                   |
| **Scale**        | \[0, 39\]            |

## Syntax

### Numeric values

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MjMiIGhlaWdodD0iMTEzIj4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iMyIgd2lkdGg9IjM2IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyOSIgeT0iMSIgd2lkdGg9IjM2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMzkiIHk9IjIxIj5pbnQ8L3RleHQ+CiAgIDxyZWN0IHg9IjEwNyIgeT0iMzUiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMDUiIHk9IjMzIiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjExNSIgeT0iNTMiPi48L3RleHQ+CiAgIDxyZWN0IHg9IjE1MSIgeT0iMzUiIHdpZHRoPSI0NiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMTQ5IiB5PSIzMyIgd2lkdGg9IjQ2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMTU5IiB5PSI1MyI+ZnJhYzwvdGV4dD4KICAgPHJlY3QgeD0iMjc3IiB5PSIzNSIgd2lkdGg9IjI4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI3NSIgeT0iMzMiIHdpZHRoPSIyOCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjg1IiB5PSI1MyI+ZTwvdGV4dD4KICAgPHJlY3QgeD0iMjc3IiB5PSI3OSIgd2lkdGg9IjI4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI3NSIgeT0iNzciIHdpZHRoPSIyOCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjg1IiB5PSI5NyI+RTwvdGV4dD4KICAgPHJlY3QgeD0iMzY1IiB5PSI2NyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjM2MyIgeT0iNjUiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzczIiB5PSI4NSI+LTwvdGV4dD4KICAgPHJlY3QgeD0iNDMxIiB5PSIzNSIgd2lkdGg9IjQ0IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI0MjkiIHk9IjMzIiB3aWR0aD0iNDQiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI0MzkiIHk9IjUzIj5leHA8L3RleHQ+CiAgIDxwYXRoIGNsYXNzPSJsaW5lIiBkPSJtMTcgMTcgaDIgbTAgMCBoMTAgbTM2IDAgaDEwIG0yMCAwIGgxMCBtMCAwIGgxMDAgbS0xMzAgMCBoMjAgbTExMCAwIGgyMCBtLTE1MCAwIHExMCAwIDEwIDEwIG0xMzAgMCBxMCAtMTAgMTAgLTEwIG0tMTQwIDEwIHYxMiBtMTMwIDAgdi0xMiBtLTEzMCAxMiBxMCAxMCAxMCAxMCBtMTEwIDAgcTEwIDAgMTAgLTEwIG0tMTIwIDEwIGgxMCBtMjQgMCBoMTAgbTAgMCBoMTAgbTQ2IDAgaDEwIG00MCAtMzIgaDEwIG0wIDAgaDIyOCBtLTI1OCAwIGgyMCBtMjM4IDAgaDIwIG0tMjc4IDAgcTEwIDAgMTAgMTAgbTI1OCAwIHEwIC0xMCAxMCAtMTAgbS0yNjggMTAgdjEyIG0yNTggMCB2LTEyIG0tMjU4IDEyIHEwIDEwIDEwIDEwIG0yMzggMCBxMTAgMCAxMCAtMTAgbS0yMjggMTAgaDEwIG0yOCAwIGgxMCBtLTY4IDAgaDIwIG00OCAwIGgyMCBtLTg4IDAgcTEwIDAgMTAgMTAgbTY4IDAgcTAgLTEwIDEwIC0xMCBtLTc4IDEwIHYyNCBtNjggMCB2LTI0IG0tNjggMjQgcTAgMTAgMTAgMTAgbTQ4IDAgcTEwIDAgMTAgLTEwIG0tNTggMTAgaDEwIG0yOCAwIGgxMCBtNDAgLTQ0IGgxMCBtMCAwIGgzNiBtLTY2IDAgaDIwIG00NiAwIGgyMCBtLTg2IDAgcTEwIDAgMTAgMTAgbTY2IDAgcTAgLTEwIDEwIC0xMCBtLTc2IDEwIHYxMiBtNjYgMCB2LTEyIG0tNjYgMTIgcTAgMTAgMTAgMTAgbTQ2IDAgcTEwIDAgMTAgLTEwIG0tNTYgMTAgaDEwIG0yNiAwIGgxMCBtMjAgLTMyIGgxMCBtNDQgMCBoMTAgbTIzIC0zMiBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTEzIDE3IDUyMSAxMyA1MjEgMjEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSI1MTMgMTcgNTA1IDEzIDUwNSAyMSI+PC9wb2x5Z29uPgo8L3N2Zz4=)

</div>

| Field      | Use                                                     |
|------------|---------------------------------------------------------|
| **E***exp* | Multiply the number preceding **E** by 10<sup>exp</sup> |

### Numeric definitions

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI0ODUiIGhlaWdodD0iNjkiPgogICA8cG9seWdvbiBwb2ludHM9IjkgMTcgMSAxMyAxIDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iMTcgMTcgOSAxMyA5IDIxIj48L3BvbHlnb24+CiAgIDxyZWN0IHg9IjMxIiB5PSIzIiB3aWR0aD0iNzYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjkiIHk9IjEiIHdpZHRoPSI3NiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzkiIHk9IjIxIj5udW1lcmljPC90ZXh0PgogICA8cmVjdCB4PSIxNDciIHk9IjM1IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTQ1IiB5PSIzMyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxNTUiIHk9IjUzIj4oPC90ZXh0PgogICA8cmVjdCB4PSIxOTMiIHk9IjM1IiB3aWR0aD0iODAiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjE5MSIgeT0iMzMiIHdpZHRoPSI4MCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjIwMSIgeT0iNTMiPnByZWNpc2lvbjwvdGV4dD4KICAgPHJlY3QgeD0iMjkzIiB5PSIzNSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI5MSIgeT0iMzMiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzAxIiB5PSI1MyI+LDwvdGV4dD4KICAgPHJlY3QgeD0iMzM3IiB5PSIzNSIgd2lkdGg9IjU0IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzMzUiIHk9IjMzIiB3aWR0aD0iNTQiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzNDUiIHk9IjUzIj5zY2FsZTwvdGV4dD4KICAgPHJlY3QgeD0iNDExIiB5PSIzNSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQwOSIgeT0iMzMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDE5IiB5PSI1MyI+KTwvdGV4dD4KICAgPHBhdGggY2xhc3M9ImxpbmUiIGQ9Im0xNyAxNyBoMiBtMCAwIGgxMCBtNzYgMCBoMTAgbTIwIDAgaDEwIG0wIDAgaDMwMCBtLTMzMCAwIGgyMCBtMzEwIDAgaDIwIG0tMzUwIDAgcTEwIDAgMTAgMTAgbTMzMCAwIHEwIC0xMCAxMCAtMTAgbS0zNDAgMTAgdjEyIG0zMzAgMCB2LTEyIG0tMzMwIDEyIHEwIDEwIDEwIDEwIG0zMTAgMCBxMTAgMCAxMCAtMTAgbS0zMjAgMTAgaDEwIG0yNiAwIGgxMCBtMCAwIGgxMCBtODAgMCBoMTAgbTAgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDEwIG01NCAwIGgxMCBtMCAwIGgxMCBtMjYgMCBoMTAgbTIzIC0zMiBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iNDc1IDE3IDQ4MyAxMyA0ODMgMjEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSI0NzUgMTcgNDY3IDEzIDQ2NyAyMSI+PC9wb2x5Z29uPgo8L3N2Zz4=)

</div>

| Field | Use |
|----|----|
| *precision* | **Ignored**: All `numeric` values in Materialize have a precision of 39. |
| *scale* | The total number of fractional decimal digits to track, e.g. `.321` has a scale of 3. *scale* cannot exceed the maximum precision. |

## Details

### Input

Materialize assumes untyped numeric literals are `numeric` if they:

- Contain decimal points or e-notation.
- Exceed `bigint`’s maximum or minimum values.

Materialize does not accept any numeric literals that exceed 39 digits
of precision.

### Output

Materialize trims all trailing zeroes off of `numeric` values,
irrespective of their specified scale. This behavior lets us perform
byte-level equality when comparing rows of data, where we would
otherwise need to decode rows’ columns' values for comparisons.

### Precision

All `numeric` values have a precision of 39, which cannot be changed.
For ease of use, Materialize accepts input of any value \<= 39, but
ignores the specified value.

Note that the leading zeroes in values between -1 and 1 (i.e. `(-1, 1)`)
are not counted digits of precision.

For details on exceeding the `numeric` type’s maximum precision, see
[Rounding](#rounding).

### Scale

By default, `numeric` values do not have a specified scale, so values
can have anywhere between 0 and 39 digits after the decimal point. For
example:

<div class="highlight">

``` chroma
CREATE TABLE unscaled (c NUMERIC);
INSERT INTO unscaled VALUES
  (987654321098765432109876543210987654321),
  (9876543210987654321.09876543210987654321),
  (.987654321098765432109876543210987654321);

SELECT c FROM unscaled;

                     c
-------------------------------------------
   987654321098765432109876543210987654321
 0.987654321098765432109876543210987654321
  9876543210987654321.09876543210987654321
```

</div>

However, if you specify a scale on a `numeric` value, values will be
rescaled appropriately. If the resulting value exceeds the maximum
precision for `numeric` types, you’ll receive an error.

<div class="highlight">

``` chroma
CREATE TABLE scaled (c NUMERIC(39, 20));

INSERT INTO scaled VALUES
  (987654321098765432109876543210987654321);
```

</div>

```
ERROR:  numeric field overflow
```

<div class="highlight">

``` chroma
INSERT INTO scaled VALUES
  (9876543210987654321.09876543210987654321),
  (.987654321098765432109876543210987654321);

SELECT c FROM scaled;

                    c
------------------------------------------
                   0.98765432109876543211
 9876543210987654321.09876543210987654321
```

</div>

### Rounding

`numeric` operations will always round off fractional values to limit
their values to 39 digits of precision.

<div class="highlight">

``` chroma
SELECT 2 * 9876543210987654321.09876543210987654321 AS rounded;

                 rounded
------------------------------------------
 19753086421975308642.1975308642197530864
```

</div>

However, if a value exceeds is \>= 1E39 or \<= -1E39, it generates an
overflow, i.e. it will not be rounded.

### Overflow

Operations generating values \>= 1E39 or \<= -1E39 are considered
overflown.

### Underflow

Operations generating values within the range `(-1E-40, 1E-40)` are
considered underflown.

### Aggregations (`sum`)

**tl;dr** If you use `sum` on `numeric` values, retrieving values of
`Infinity` or `-Infinity` from the operation signals you have overflown
the `numeric` type. At this point, you should start retracting values or
risk causing Materialize to panic.

Materialize’s dataflow engine (Differential) requires operations to
retain their commutativity and associativity––a simple way to think
about this invariant is that if you add a series of values, and then
subtract the same values in some random order, you must
deterministically reach zero.

While other types in Materialize achieve this by simply allowing
overflows (i.e. going from the maximum 64-bit signed integer to the
minimum 64-bit signed integer), the `numeric` type does not support an
equivalent operation.

To provide the invariants that Differential requires, `numeric` values
are instead aggregated into an even larger type behind the scenes––one
that supports twice as many digits of precision. This way, if your
aggregation exceeds the `numeric` type’s bounds, we can continue to
track the magnitude of excess.

To signal that the `numeric` value is in this state, the operation
begins returning `Infinity` or `-Infinity`. At this point, you must
either subtract or retract values to return representable `numeric`
values.

However, because the underlying aggregated type is not of infinite
precision, it too can overflow. In this case, Materialize will instead
panic and crash. To avoid crashing, always immediately retract or
subtract values when encountering infinite results from `sum`.

### Valid casts

#### From `numeric`

You can [cast](../../functions/cast) `numeric` to:

- [`int`/`bigint`](../int) (by assignment)
- [`real`/`double precision`](../float) (implicitly)
- [`text`](../text) (by assignment)

#### To `numeric`

You can [cast](../../functions/cast) from the following types to
`numeric`:

- [`int`/`bigint`](../int) (implicitly)
- [`real`/`double precision`](../float) (by assignment)
- [`text`](../text) (explicitly)

## Examples

<div class="highlight">

``` chroma
SELECT 1.23::numeric AS num_v;
```

</div>

```
 num_v
-------
  1.23
```

------------------------------------------------------------------------

<div class="highlight">

``` chroma
SELECT 1.23::numeric(38,3) AS num_38_3_v;
```

</div>

```
 num_38_3_v
------------
      1.230
```

------------------------------------------------------------------------

<div class="highlight">

``` chroma
SELECT 1.23e4 AS num_w_exp;
```

</div>

```
 num_w_exp
-----------
     12300
```

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/types/numeric.md"
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
