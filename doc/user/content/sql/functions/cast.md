---
title: "CAST function and operator"
description: "Returns the value converted to the specified type"
menu:
  main:
    parent: 'sql-functions'
---

The `cast` function and operator return a value converted to the specified [type](../../types/).

## Signatures

{{< diagram "func-cast.svg" >}}

{{< diagram "op-cast.svg" >}}

Parameter | Type | Description
----------|------|------------
_val_ | [Any](../../types) | The value you want to convert.
_type_ | [Typename](../../types) | The return value's type.

The following special syntax is permitted if _val_ is a string literal:

{{< diagram "lit-cast.svg" >}}

### Return value

`cast` returns the value with the type specified by the _type_ parameter.

## Details

### Valid casts

Cast context defines when casts may occur.

Cast context | Definition | Strictness
--------|------------|-----------
**Implicit** | Values are automatically converted. For example, when you add `int4` to `int8`, the `int4` value is automatically converted to `int8`. | Least
**Assignment** | Values of one type are converted automatically when inserted into a column of a different type. | Medium
**Explicit** | You must invoke `CAST` deliberately. | Most

Casts allowed in less strict contexts are also allowed in stricter contexts. That is, implicit casts also occur by assignment, and both implicit casts and casts by assignment can be explicitly invoked.

Source type                                | Return type                                   | Cast context
-------------------------------------------|-----------------------------------------------|----------
[`array`](../../types/array/)<sup>1</sup>  | [`text`](../../types/text/)                   | Assignment
[`bigint`](../../types/integer/)           | [`bool`](../../types/boolean/)                | Explicit
[`bigint`](../../types/integer/)           | [`int`](../../types/integer/)                 | Assignment
[`bigint`](../../types/integer/)           | [`float`](../../types/float/)                 | Implicit
[`bigint`](../../types/integer/)           | [`numeric`](../../types/numeric/)             | Implicit
[`bigint`](../../types/integer/)           | [`real`](../../types/real/)                   | Implicit
[`bigint`](../../types/integer/)           | [`text`](../../types/text/)                   | Assignment
[`bigint`](../../types/integer/)           | [`uint2`](../../types/uint/)                  | Assignment
[`bigint`](../../types/integer/)           | [`uint4`](../../types/uint/)                  | Assignment
[`bigint`](../../types/integer/)           | [`uint8`](../../types/uint/)                  | Assignment
[`bool`](../../types/boolean/)             | [`int`](../../types/integer/)                 | Explicit
[`bool`](../../types/boolean/)             | [`text`](../../types/text/)                   | Assignment
[`bytea`](../../types/bytea/)              | [`text`](../../types/text/)                   | Assignment
[`date`](../../types/date/)                | [`text`](../../types/text/)                   | Assignment
[`date`](../../types/date/)                | [`timestamp`](../../types/timestamp/)         | Implicit
[`date`](../../types/date/)                | [`timestamptz`](../../types/timestamp/)       | Implicit
[`float`](../../types/float/)              | [`bigint`](../../types/integer/)              | Assignment
[`float`](../../types/float/)              | [`int`](../../types/integer/)                 | Assignment
[`float`](../../types/float/)              | [`numeric`](../../types/numeric/)<sup>2</sup> | Assignment
[`float`](../../types/float/)              | [`real`](../../types/real/)                   | Assignment
[`float`](../../types/float/)              | [`text`](../../types/text/)                   | Assignment
[`float`](../../types/float/)              | [`uint2`](../../types/uint/)                  | Assignment
[`float`](../../types/float/)              | [`uint4`](../../types/uint/)                  | Assignment
[`float`](../../types/float/)              | [`uint8`](../../types/uint/)                  | Assignment
[`int`](../../types/integer/)              | [`bigint`](../../types/integer/)              | Implicit
[`int`](../../types/integer/)              | [`bool`](../../types/boolean/)                | Explicit
[`int`](../../types/integer/)              | [`float`](../../types/float/)                 | Implicit
[`int`](../../types/integer/)              | [`numeric`](../../types/numeric/)             | Implicit
[`int`](../../types/integer/)              | [`oid`](../../types/oid/)                     | Implicit
[`int`](../../types/integer/)              | [`real`](../../types/real/)                   | Implicit
[`int`](../../types/integer/)              | [`text`](../../types/text/)                   | Assignment
[`int`](../../types/integer/)              | [`uint2`](../../types/uint/)                  | Assignment
[`int`](../../types/integer/)              | [`uint4`](../../types/uint/)                  | Assignment
[`int`](../../types/integer/)              | [`uint8`](../../types/uint/)                  | Assignment
[`interval`](../../types/interval/)        | [`text`](../../types/text/)                   | Assignment
[`interval`](../../types/interval/)        | [`time`](../../types/time/)                   | Assignment
[`jsonb`](../../types/jsonb/)              | [`bigint`](../../types/integer/)              | Explicit
[`jsonb`](../../types/jsonb/)              | [`bool`](../../types/boolean/)                | Explicit
[`jsonb`](../../types/jsonb/)              | [`float`](../../types/float/)                 | Explicit
[`jsonb`](../../types/jsonb/)              | [`int`](../../types/integer/)                 | Explicit
[`jsonb`](../../types/jsonb/)              | [`real`](../../types/real/)                   | Explicit
[`jsonb`](../../types/jsonb/)              | [`numeric`](../../types/numeric/)             | Explicit
[`jsonb`](../../types/jsonb/)              | [`text`](../../types/text/)                   | Assignment
[`list`](../../types/list/)<sup>1</sup>    | [`list`](../../types/list/)                   | Implicit
[`list`](../../types/list/)<sup>1</sup>    | [`text`](../../types/text/)                   | Assignment
[`map`](../../types/map/)                  | [`text`](../../types/text/)                   | Assignment
[`numeric`](../../types/numeric/)          | [`bigint`](../../types/integer/)              | Assignment
[`numeric`](../../types/numeric/)          | [`float`](../../types/float/)                 | Implicit
[`numeric`](../../types/numeric/)          | [`int`](../../types/integer/)                 | Assignment
[`numeric`](../../types/numeric/)          | [`real`](../../types/real/)                   | Implicit
[`numeric`](../../types/numeric/)          | [`text`](../../types/text/)                   | Assignment
[`numeric`](../../types/numeric/)          | [`uint2`](../../types/uint/)                  | Assignment
[`numeric`](../../types/numeric/)          | [`uint4`](../../types/uint/)                  | Assignment
[`numeric`](../../types/numeric/)          | [`uint8`](../../types/uint/)                  | Assignment
[`oid`](../../types/oid/)                  | [`int`](../../types/integer/)                 | Assignment
[`oid`](../../types/oid/)                  | [`text`](../../types/text/)                   | Explicit
[`real`](../../types/real/)                | [`bigint`](../../types/integer/)              | Assignment
[`real`](../../types/real/)                | [`float`](../../types/float/)                 | Implicit
[`real`](../../types/real/)                | [`int`](../../types/integer/)                 | Assignment
[`real`](../../types/real/)                | [`numeric`](../../types/numeric/)             | Assignment
[`real`](../../types/real/)                | [`text`](../../types/text/)                   | Assignment
[`real`](../../types/real/)                | [`uint2`](../../types/uint/)                  | Assignment
[`real`](../../types/real/)                | [`uint4`](../../types/uint/)                  | Assignment
[`real`](../../types/real/)                | [`uint8`](../../types/uint/)                  | Assignment
[`record`](../../types/record/)            | [`text`](../../types/text/)                   | Assignment
[`smallint`](../../types/integer/)         | [`bigint`](../../types/integer/)              | Implicit
[`smallint`](../../types/integer/)         | [`float`](../../types/float/)                 | Implicit
[`smallint`](../../types/integer/)         | [`int`](../../types/integer/)                 | Implicit
[`smallint`](../../types/integer/)         | [`numeric`](../../types/numeric/)             | Implicit
[`smallint`](../../types/integer/)         | [`oid`](../../types/oid/)                     | Implicit
[`smallint`](../../types/integer/)         | [`real`](../../types/real/)                   | Implicit
[`smallint`](../../types/integer/)         | [`text`](../../types/text/)                   | Assignment
[`smallint`](../../types/integer/)         | [`uint2`](../../types/uint/)                  | Assignment
[`smallint`](../../types/integer/)         | [`uint4`](../../types/uint/)                  | Assignment
[`smallint`](../../types/integer/)         | [`uint8`](../../types/uint/)                  | Assignment
[`text`](../../types/text/)                | [`bigint`](../../types/integer/)              | Explicit
[`text`](../../types/text/)                | [`bool`](../../types/boolean/)                | Explicit
[`text`](../../types/text/)                | [`bytea`](../../types/bytea/)                 | Explicit
[`text`](../../types/text/)                | [`date`](../../types/date/)                   | Explicit
[`text`](../../types/text/)                | [`float`](../../types/float/)                 | Explicit
[`text`](../../types/text/)                | [`int`](../../types/integer/)                 | Explicit
[`text`](../../types/text/)                | [`interval`](../../types/interval/)           | Explicit
[`text`](../../types/text/)                | [`jsonb`](../../types/jsonb/)                 | Explicit
[`text`](../../types/text/)                | [`list`](../../types/list/)                   | Explicit
[`text`](../../types/text/)                | [`map`](../../types/map/)                     | Explicit
[`text`](../../types/text/)                | [`numeric`](../../types/numeric/)             | Explicit
[`text`](../../types/text/)                | [`oid`](../../types/oid/)                     | Explicit
[`text`](../../types/text/)                | [`real`](../../types/real/)                   | Explicit
[`text`](../../types/text/)                | [`time`](../../types/time/)                   | Explicit
[`text`](../../types/text/)                | [`timestamp`](../../types/timestamp/)         | Explicit
[`text`](../../types/text/)                | [`timestamptz`](../../types/timestamp/)       | Explicit
[`text`](../../types/text/)                | [`uint2`](../../types/uint/)                  | Explicit
[`text`](../../types/text/)                | [`uint4`](../../types/uint/)                  | Assignment
[`text`](../../types/text/)                | [`uint8`](../../types/uint/)                  | Assignment
[`text`](../../types/text/)                | [`uuid`](../../types/uuid/)                   | Explicit
[`time`](../../types/time/)                | [`interval`](../../types/interval/)           | Implicit
[`time`](../../types/time/)                | [`text`](../../types/text/)                   | Assignment
[`timestamp`](../../types/timestamp/)      | [`date`](../../types/date/)                   | Assignment
[`timestamp`](../../types/timestamp/)      | [`text`](../../types/text/)                   | Assignment
[`timestamp`](../../types/timestamp/)      | [`timestamptz`](../../types/timestamp/)       | Implicit
[`timestamptz`](../../types/timestamp/)    | [`date`](../../types/date/)                   | Assignment
[`timestamptz`](../../types/timestamp/)    | [`text`](../../types/text/)                   | Assignment
[`timestamptz`](../../types/timestamp/)    | [`timestamp`](../../types/timestamp/)         | Assignment
[`uint2`](../../types/uint/)               | [`bigint`](../../types/integer/)              | Implicit
[`uint2`](../../types/uint/)               | [`float`](../../types/float/)                 | Implicit
[`uint2`](../../types/uint/)               | [`int`](../../types/integer/)                 | Implicit
[`uint2`](../../types/uint/)               | [`numeric`](../../types/numeric/)             | Implicit
[`uint2`](../../types/uint/)               | [`real`](../../types/real/)                   | Implicit
[`uint2`](../../types/uint/)               | [`text`](../../types/text/)                   | Assignment
[`uint2`](../../types/uint/)               | [`uint4`](../../types/uint/)                  | Implicit
[`uint2`](../../types/uint/)               | [`uint8`](../../types/uint/)                  | Implicit
[`uint4`](../../types/uint)                | [`bigint`](../../types/integer/)              | Implicit
[`uint4`](../../types/uint)                | [`float`](../../types/float/)                 | Implicit
[`uint4`](../../types/uint)                | [`int`](../../types/integer/)                 | Assignment
[`uint4`](../../types/uint)                | [`numeric`](../../types/numeric/)             | Implicit
[`uint4`](../../types/uint)                | [`real`](../../types/real/)                   | Implicit
[`uint4`](../../types/uint)                | [`text`](../../types/text/)                   | Assignment
[`uint4`](../../types/uint)                | [`uint2`](../../types/uint/)                  | Assignment
[`uint4`](../../types/uint)                | [`uint8`](../../types/uint/)                  | Implicit
[`uint8`](../../types/uint/)               | [`bigint`](../../types/integer/)              | Assignment
[`uint8`](../../types/uint/)               | [`float`](../../types/float/)                 | Implicit
[`uint8`](../../types/uint/)               | [`int`](../../types/integer/)                 | Assignment
[`uint8`](../../types/uint/)               | [`real`](../../types/real/)                   | Implicit
[`uint8`](../../types/uint/)               | [`uint2`](../../types/uint/)                  | Assignment
[`uint8`](../../types/uint/)               | [`uint4`](../../types/uint/)                  | Assignment
[`uuid`](../../types/uuid/)                | [`text`](../../types/text/)                   | Assignment

<sup>1</sup> [`Arrays`](../../types/array/) and [`lists`](../../types/list) are composite types subject to special constraints. See their respective type documentation for details.

<sup>2</sup> Casting a [`float`](../../types/float/) to a [`numeric`](../../types/numeric/) can yield an imprecise result due to the floating point arithmetic involved in the conversion.

## Examples

```sql
SELECT INT '4';
```
```nofmt
 ?column?
----------
         4
```

<hr>

```sql
SELECT CAST (CAST (100.21 AS numeric(10, 2)) AS float) AS dec_to_float;
```
```nofmt
 dec_to_float
--------------
       100.21
```

<hr/>

```sql
SELECT 100.21::numeric(10, 2)::float AS dec_to_float;
```
```nofmt
 dec_to_float
--------------
       100.21
```

## Related topics
* [Data Types](../../types/)
