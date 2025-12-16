<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)  /  [SQL functions &
operators](/docs/self-managed/v25.2/sql/functions/)

</div>

# CAST function and operator

The `cast` function and operator return a value converted to the
specified [type](../../types/).

## Signatures

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIzOTkiIGhlaWdodD0iMzciPgogICA8cG9seWdvbiBwb2ludHM9IjkgMTcgMSAxMyAxIDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iMTcgMTcgOSAxMyA5IDIxIj48L3BvbHlnb24+CiAgIDxyZWN0IHg9IjMxIiB5PSIzIiB3aWR0aD0iNjAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjkiIHk9IjEiIHdpZHRoPSI2MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzkiIHk9IjIxIj5DQVNUPC90ZXh0PgogICA8cmVjdCB4PSIxMTEiIHk9IjMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMDkiIHk9IjEiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTE5IiB5PSIyMSI+KDwvdGV4dD4KICAgPHJlY3QgeD0iMTU3IiB5PSIzIiB3aWR0aD0iMzgiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjE1NSIgeT0iMSIgd2lkdGg9IjM4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMTY1IiB5PSIyMSI+dmFsPC90ZXh0PgogICA8cmVjdCB4PSIyMTUiIHk9IjMiIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyMTMiIHk9IjEiIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjIzIiB5PSIyMSI+QVM8L3RleHQ+CiAgIDxyZWN0IHg9IjI3NSIgeT0iMyIgd2lkdGg9IjUwIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyNzMiIHk9IjEiIHdpZHRoPSI1MCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI4MyIgeT0iMjEiPnR5cGU8L3RleHQ+CiAgIDxyZWN0IHg9IjM0NSIgeT0iMyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjM0MyIgeT0iMSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzNTMiIHk9IjIxIj4pPC90ZXh0PgogICA8cGF0aCBjbGFzcz0ibGluZSIgZD0ibTE3IDE3IGgyIG0wIDAgaDEwIG02MCAwIGgxMCBtMCAwIGgxMCBtMjYgMCBoMTAgbTAgMCBoMTAgbTM4IDAgaDEwIG0wIDAgaDEwIG00MCAwIGgxMCBtMCAwIGgxMCBtNTAgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0zIDAgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjM4OSAxNyAzOTcgMTMgMzk3IDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iMzg5IDE3IDM4MSAxMyAzODEgMjEiPjwvcG9seWdvbj4KPC9zdmc+)

</div>

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyMTUiIGhlaWdodD0iMzciPgogICA8cG9seWdvbiBwb2ludHM9IjkgMTcgMSAxMyAxIDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iMTcgMTcgOSAxMyA5IDIxIj48L3BvbHlnb24+CiAgIDxyZWN0IHg9IjMxIiB5PSIzIiB3aWR0aD0iMzgiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjI5IiB5PSIxIiB3aWR0aD0iMzgiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzOSIgeT0iMjEiPnZhbDwvdGV4dD4KICAgPHJlY3QgeD0iODkiIHk9IjMiIHdpZHRoPSIyOCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI4NyIgeT0iMSIgd2lkdGg9IjI4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI5NyIgeT0iMjEiPjo6PC90ZXh0PgogICA8cmVjdCB4PSIxMzciIHk9IjMiIHdpZHRoPSI1MCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMTM1IiB5PSIxIiB3aWR0aD0iNTAiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIxNDUiIHk9IjIxIj50eXBlPC90ZXh0PgogICA8cGF0aCBjbGFzcz0ibGluZSIgZD0ibTE3IDE3IGgyIG0wIDAgaDEwIG0zOCAwIGgxMCBtMCAwIGgxMCBtMjggMCBoMTAgbTAgMCBoMTAgbTUwIDAgaDEwIG0zIDAgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjIwNSAxNyAyMTMgMTMgMjEzIDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iMjA1IDE3IDE5NyAxMyAxOTcgMjEiPjwvcG9seWdvbj4KPC9zdmc+)

</div>

| Parameter | Type                    | Description                    |
|-----------|-------------------------|--------------------------------|
| *val*     | [Any](../../types)      | The value you want to convert. |
| *type*    | [Typename](../../types) | The return value’s type.       |

The following special syntax is permitted if *val* is a string literal:

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxNjciIGhlaWdodD0iMzciPgogICA8cG9seWdvbiBwb2ludHM9IjkgMTcgMSAxMyAxIDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iMTcgMTcgOSAxMyA5IDIxIj48L3BvbHlnb24+CiAgIDxyZWN0IHg9IjMxIiB5PSIzIiB3aWR0aD0iNTAiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjI5IiB5PSIxIiB3aWR0aD0iNTAiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzOSIgeT0iMjEiPnR5cGU8L3RleHQ+CiAgIDxyZWN0IHg9IjEwMSIgeT0iMyIgd2lkdGg9IjM4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI5OSIgeT0iMSIgd2lkdGg9IjM4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMTA5IiB5PSIyMSI+dmFsPC90ZXh0PgogICA8cGF0aCBjbGFzcz0ibGluZSIgZD0ibTE3IDE3IGgyIG0wIDAgaDEwIG01MCAwIGgxMCBtMCAwIGgxMCBtMzggMCBoMTAgbTMgMCBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iMTU3IDE3IDE2NSAxMyAxNjUgMjEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSIxNTcgMTcgMTQ5IDEzIDE0OSAyMSI+PC9wb2x5Z29uPgo8L3N2Zz4=)

</div>

### Return value

`cast` returns the value with the type specified by the *type*
parameter.

## Details

### Valid casts

Cast context defines when casts may occur.

| Cast context | Definition | Strictness |
|----|----|----|
| **Implicit** | Values are automatically converted. For example, when you add `int4` to `int8`, the `int4` value is automatically converted to `int8`. | Least |
| **Assignment** | Values of one type are converted automatically when inserted into a column of a different type. | Medium |
| **Explicit** | You must invoke `CAST` deliberately. | Most |

Casts allowed in less strict contexts are also allowed in stricter
contexts. That is, implicit casts also occur by assignment, and both
implicit casts and casts by assignment can be explicitly invoked.

| Source type | Return type | Cast context |
|----|----|----|
| [`array`](../../types/array/)<sup>1</sup> | [`text`](../../types/text/) | Assignment |
| [`bigint`](../../types/integer/) | [`bool`](../../types/boolean/) | Explicit |
| [`bigint`](../../types/integer/) | [`int`](../../types/integer/) | Assignment |
| [`bigint`](../../types/integer/) | [`float`](../../types/float/) | Implicit |
| [`bigint`](../../types/integer/) | [`numeric`](../../types/numeric/) | Implicit |
| [`bigint`](../../types/integer/) | [`real`](../../types/real/) | Implicit |
| [`bigint`](../../types/integer/) | [`text`](../../types/text/) | Assignment |
| [`bigint`](../../types/integer/) | [`uint2`](../../types/uint/) | Assignment |
| [`bigint`](../../types/integer/) | [`uint4`](../../types/uint/) | Assignment |
| [`bigint`](../../types/integer/) | [`uint8`](../../types/uint/) | Assignment |
| [`bool`](../../types/boolean/) | [`int`](../../types/integer/) | Explicit |
| [`bool`](../../types/boolean/) | [`text`](../../types/text/) | Assignment |
| [`bytea`](../../types/bytea/) | [`text`](../../types/text/) | Assignment |
| [`date`](../../types/date/) | [`text`](../../types/text/) | Assignment |
| [`date`](../../types/date/) | [`timestamp`](../../types/timestamp/) | Implicit |
| [`date`](../../types/date/) | [`timestamptz`](../../types/timestamp/) | Implicit |
| [`float`](../../types/float/) | [`bigint`](../../types/integer/) | Assignment |
| [`float`](../../types/float/) | [`int`](../../types/integer/) | Assignment |
| [`float`](../../types/float/) | [`numeric`](../../types/numeric/)<sup>2</sup> | Assignment |
| [`float`](../../types/float/) | [`real`](../../types/real/) | Assignment |
| [`float`](../../types/float/) | [`text`](../../types/text/) | Assignment |
| [`float`](../../types/float/) | [`uint2`](../../types/uint/) | Assignment |
| [`float`](../../types/float/) | [`uint4`](../../types/uint/) | Assignment |
| [`float`](../../types/float/) | [`uint8`](../../types/uint/) | Assignment |
| [`int`](../../types/integer/) | [`bigint`](../../types/integer/) | Implicit |
| [`int`](../../types/integer/) | [`bool`](../../types/boolean/) | Explicit |
| [`int`](../../types/integer/) | [`float`](../../types/float/) | Implicit |
| [`int`](../../types/integer/) | [`numeric`](../../types/numeric/) | Implicit |
| [`int`](../../types/integer/) | [`oid`](../../types/oid/) | Implicit |
| [`int`](../../types/integer/) | [`real`](../../types/real/) | Implicit |
| [`int`](../../types/integer/) | [`text`](../../types/text/) | Assignment |
| [`int`](../../types/integer/) | [`uint2`](../../types/uint/) | Assignment |
| [`int`](../../types/integer/) | [`uint4`](../../types/uint/) | Assignment |
| [`int`](../../types/integer/) | [`uint8`](../../types/uint/) | Assignment |
| [`interval`](../../types/interval/) | [`text`](../../types/text/) | Assignment |
| [`interval`](../../types/interval/) | [`time`](../../types/time/) | Assignment |
| [`jsonb`](../../types/jsonb/) | [`bigint`](../../types/integer/) | Explicit |
| [`jsonb`](../../types/jsonb/) | [`bool`](../../types/boolean/) | Explicit |
| [`jsonb`](../../types/jsonb/) | [`float`](../../types/float/) | Explicit |
| [`jsonb`](../../types/jsonb/) | [`int`](../../types/integer/) | Explicit |
| [`jsonb`](../../types/jsonb/) | [`real`](../../types/real/) | Explicit |
| [`jsonb`](../../types/jsonb/) | [`numeric`](../../types/numeric/) | Explicit |
| [`jsonb`](../../types/jsonb/) | [`text`](../../types/text/) | Assignment |
| [`list`](../../types/list/)<sup>1</sup> | [`list`](../../types/list/) | Implicit |
| [`list`](../../types/list/)<sup>1</sup> | [`text`](../../types/text/) | Assignment |
| [`map`](../../types/map/) | [`text`](../../types/text/) | Assignment |
| [`mz_aclitem`](../../types/mz_aclitem/) | [`text`](../../types/text/) | Explicit |
| [`numeric`](../../types/numeric/) | [`bigint`](../../types/integer/) | Assignment |
| [`numeric`](../../types/numeric/) | [`float`](../../types/float/) | Implicit |
| [`numeric`](../../types/numeric/) | [`int`](../../types/integer/) | Assignment |
| [`numeric`](../../types/numeric/) | [`real`](../../types/real/) | Implicit |
| [`numeric`](../../types/numeric/) | [`text`](../../types/text/) | Assignment |
| [`numeric`](../../types/numeric/) | [`uint2`](../../types/uint/) | Assignment |
| [`numeric`](../../types/numeric/) | [`uint4`](../../types/uint/) | Assignment |
| [`numeric`](../../types/numeric/) | [`uint8`](../../types/uint/) | Assignment |
| [`oid`](../../types/oid/) | [`int`](../../types/integer/) | Assignment |
| [`oid`](../../types/oid/) | [`text`](../../types/text/) | Explicit |
| [`real`](../../types/real/) | [`bigint`](../../types/integer/) | Assignment |
| [`real`](../../types/real/) | [`float`](../../types/float/) | Implicit |
| [`real`](../../types/real/) | [`int`](../../types/integer/) | Assignment |
| [`real`](../../types/real/) | [`numeric`](../../types/numeric/) | Assignment |
| [`real`](../../types/real/) | [`text`](../../types/text/) | Assignment |
| [`real`](../../types/real/) | [`uint2`](../../types/uint/) | Assignment |
| [`real`](../../types/real/) | [`uint4`](../../types/uint/) | Assignment |
| [`real`](../../types/real/) | [`uint8`](../../types/uint/) | Assignment |
| [`record`](../../types/record/) | [`text`](../../types/text/) | Assignment |
| [`smallint`](../../types/integer/) | [`bigint`](../../types/integer/) | Implicit |
| [`smallint`](../../types/integer/) | [`float`](../../types/float/) | Implicit |
| [`smallint`](../../types/integer/) | [`int`](../../types/integer/) | Implicit |
| [`smallint`](../../types/integer/) | [`numeric`](../../types/numeric/) | Implicit |
| [`smallint`](../../types/integer/) | [`oid`](../../types/oid/) | Implicit |
| [`smallint`](../../types/integer/) | [`real`](../../types/real/) | Implicit |
| [`smallint`](../../types/integer/) | [`text`](../../types/text/) | Assignment |
| [`smallint`](../../types/integer/) | [`uint2`](../../types/uint/) | Assignment |
| [`smallint`](../../types/integer/) | [`uint4`](../../types/uint/) | Assignment |
| [`smallint`](../../types/integer/) | [`uint8`](../../types/uint/) | Assignment |
| [`text`](../../types/text/) | [`bigint`](../../types/integer/) | Explicit |
| [`text`](../../types/text/) | [`bool`](../../types/boolean/) | Explicit |
| [`text`](../../types/text/) | [`bytea`](../../types/bytea/) | Explicit |
| [`text`](../../types/text/) | [`date`](../../types/date/) | Explicit |
| [`text`](../../types/text/) | [`float`](../../types/float/) | Explicit |
| [`text`](../../types/text/) | [`int`](../../types/integer/) | Explicit |
| [`text`](../../types/text/) | [`interval`](../../types/interval/) | Explicit |
| [`text`](../../types/text/) | [`jsonb`](../../types/jsonb/) | Explicit |
| [`text`](../../types/text/) | [`list`](../../types/list/) | Explicit |
| [`text`](../../types/text/) | [`map`](../../types/map/) | Explicit |
| [`text`](../../types/text/) | [`numeric`](../../types/numeric/) | Explicit |
| [`text`](../../types/text/) | [`oid`](../../types/oid/) | Explicit |
| [`text`](../../types/text/) | [`real`](../../types/real/) | Explicit |
| [`text`](../../types/text/) | [`time`](../../types/time/) | Explicit |
| [`text`](../../types/text/) | [`timestamp`](../../types/timestamp/) | Explicit |
| [`text`](../../types/text/) | [`timestamptz`](../../types/timestamp/) | Explicit |
| [`text`](../../types/text/) | [`uint2`](../../types/uint/) | Explicit |
| [`text`](../../types/text/) | [`uint4`](../../types/uint/) | Assignment |
| [`text`](../../types/text/) | [`uint8`](../../types/uint/) | Assignment |
| [`text`](../../types/text/) | [`uuid`](../../types/uuid/) | Explicit |
| [`time`](../../types/time/) | [`interval`](../../types/interval/) | Implicit |
| [`time`](../../types/time/) | [`text`](../../types/text/) | Assignment |
| [`timestamp`](../../types/timestamp/) | [`date`](../../types/date/) | Assignment |
| [`timestamp`](../../types/timestamp/) | [`text`](../../types/text/) | Assignment |
| [`timestamp`](../../types/timestamp/) | [`timestamptz`](../../types/timestamp/) | Implicit |
| [`timestamptz`](../../types/timestamp/) | [`date`](../../types/date/) | Assignment |
| [`timestamptz`](../../types/timestamp/) | [`text`](../../types/text/) | Assignment |
| [`timestamptz`](../../types/timestamp/) | [`timestamp`](../../types/timestamp/) | Assignment |
| [`uint2`](../../types/uint/) | [`bigint`](../../types/integer/) | Implicit |
| [`uint2`](../../types/uint/) | [`float`](../../types/float/) | Implicit |
| [`uint2`](../../types/uint/) | [`int`](../../types/integer/) | Implicit |
| [`uint2`](../../types/uint/) | [`numeric`](../../types/numeric/) | Implicit |
| [`uint2`](../../types/uint/) | [`real`](../../types/real/) | Implicit |
| [`uint2`](../../types/uint/) | [`text`](../../types/text/) | Assignment |
| [`uint2`](../../types/uint/) | [`uint4`](../../types/uint/) | Implicit |
| [`uint2`](../../types/uint/) | [`uint8`](../../types/uint/) | Implicit |
| [`uint4`](../../types/uint) | [`bigint`](../../types/integer/) | Implicit |
| [`uint4`](../../types/uint) | [`float`](../../types/float/) | Implicit |
| [`uint4`](../../types/uint) | [`int`](../../types/integer/) | Assignment |
| [`uint4`](../../types/uint) | [`numeric`](../../types/numeric/) | Implicit |
| [`uint4`](../../types/uint) | [`real`](../../types/real/) | Implicit |
| [`uint4`](../../types/uint) | [`text`](../../types/text/) | Assignment |
| [`uint4`](../../types/uint) | [`uint2`](../../types/uint/) | Assignment |
| [`uint4`](../../types/uint) | [`uint8`](../../types/uint/) | Implicit |
| [`uint8`](../../types/uint/) | [`bigint`](../../types/integer/) | Assignment |
| [`uint8`](../../types/uint/) | [`float`](../../types/float/) | Implicit |
| [`uint8`](../../types/uint/) | [`int`](../../types/integer/) | Assignment |
| [`uint8`](../../types/uint/) | [`real`](../../types/real/) | Implicit |
| [`uint8`](../../types/uint/) | [`uint2`](../../types/uint/) | Assignment |
| [`uint8`](../../types/uint/) | [`uint4`](../../types/uint/) | Assignment |
| [`uuid`](../../types/uuid/) | [`text`](../../types/text/) | Assignment |

<sup>1</sup> [`Arrays`](../../types/array/) and
[`lists`](../../types/list) are composite types subject to special
constraints. See their respective type documentation for details.

<sup>2</sup> Casting a [`float`](../../types/float/) to a
[`numeric`](../../types/numeric/) can yield an imprecise result due to
the floating point arithmetic involved in the conversion.

## Examples

<div class="highlight">

``` chroma
SELECT INT '4';
```

</div>

```
 ?column?
----------
         4
```

------------------------------------------------------------------------

<div class="highlight">

``` chroma
SELECT CAST (CAST (100.21 AS numeric(10, 2)) AS float) AS dec_to_float;
```

</div>

```
 dec_to_float
--------------
       100.21
```

------------------------------------------------------------------------

<div class="highlight">

``` chroma
SELECT 100.21::numeric(10, 2)::float AS dec_to_float;
```

</div>

```
 dec_to_float
--------------
       100.21
```

## Related topics

- [Data Types](../../types/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/functions/cast.md"
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
