# SQL data types

Learn more about the SQL data types supported in Materialize



Materialize's type system consists of two classes of types:

- [Built-in types](#built-in-types)
- [Custom types](#custom-types) created through [`CREATE TYPE`][create-type]

## Built-in types

Type | Aliases | Use | Size (bytes) | Catalog name | Syntax
-----|-------|-----|--------------|----------------|-----
[`bigint`](integer) | `int8` | Large signed integer | 8 | Named | `123`
[`boolean`](boolean) | `bool` | State of `TRUE` or `FALSE` | 1 | Named | `TRUE`, `FALSE`
[`bytea`](bytea) | `bytea` | Unicode string | Variable | Named | `'\xDEADBEEF'` or `'\\000'`
[`date`](date) | | Date without a specified time | 4 | Named | `DATE '2007-02-01'`
[`double precision`](float) | `float`, `float8`, `double` | Double precision floating-point number | 8 | Named | `1.23`
[`integer`](integer) | `int`, `int4` | Signed integer | 4 | Named | `123`
[`interval`](interval) | | Duration of time | 32 | Named | `INTERVAL '1-2 3 4:5:6.7'`
[`jsonb`](jsonb) | `json` | JSON | Variable | Named | `'{"1":2,"3":4}'::jsonb`
[`map`](map) | | Map with [`text`](text) keys and a uniform value type | Variable | Anonymous | `'{a => 1, b => 2}'::map[text=>int]`
[`list`](list) | | Multidimensional list | Variable | Anonymous | `LIST[[1,2],[3]]`
[`numeric`](numeric) | `decimal` | Signed exact number with user-defined precision and scale | 16 | Named | `1.23`
[`oid`](oid) | | PostgreSQL object identifier | 4 | Named | `123`
[`real`](float) | `float4` | Single precision floating-point number | 4 | Named | `1.23`
[`record`](record) | | Tuple with arbitrary contents | Variable | Unnameable | `ROW($expr, ...)`
[`smallint`](integer) | `int2` | Small signed integer | 2 | Named | `123`
[`text`](text) | `string` | Unicode string | Variable | Named | `'foo'`
[`time`](time) | | Time without date | 4 | Named | `TIME '01:23:45'`
[`uint2`](uint) | | Small unsigned integer | 2 | Named | `123`
[`uint4`](uint) | | Unsigned integer | 4 | Named | `123`
[`uint8`](uint) | | Large unsigned integer | 8 | Named | `123`
[`timestamp`](timestamp) | | Date and time | 8 | Named | `TIMESTAMP '2007-02-01 15:04:05'`
[`timestamp with time zone`](timestamp) | `timestamp with time zone` | Date and time with timezone | 8 | Named | `TIMESTAMPTZ '2007-02-01 15:04:05+06'`
[Arrays](array) (`[]`) | | Multidimensional array | Variable | Named | `ARRAY[...]`
[`uuid`](uuid) | | UUID | 16 | Named | `UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`

#### Catalog name

Value | Description
------|------------
**Named** | Named types can be referred to using a qualified object name, i.e. they are objects within the `pg_catalog` schema. Each named type a unique OID.
**Anonymous** | Anonymous types cannot be referred to using a qualified object name, i.e. they do not exist as objects anywhere.<br/><br/>Anonymous types do not have unique OIDs for all of their possible permutations, e.g. `int4 list`, `float8 list`, and `date list list` share the same OID.<br/><br/>You can create named versions of some anonymous types using [custom types](#custom-types).
**Unnameable** | Unnameable types are anonymous and do not yet support being custom types.

## Custom types

Custom types, in general, provide a mechanism to create names for specific
instances of anonymous types to suit users' needs.

However, types are considered custom if the type:
- Was created through [`CREATE TYPE`][create-type].
- Contains a reference to a custom type.

To create custom types, see [`CREATE TYPE`][create-type].

### Use

Currently, custom types only provides a shorthand for referring to
otherwise-annoying-to-type names.

### Casts

Structurally equivalent types can be cast to and from one another; the required
context depends on the types themselves, though.

From | To | Cast permitted
-----|----|-----------------
Custom type | Built-in type | Implicitly
Built-in type | Custom type | Implicitly
Custom type 1 | Custom type 2 | [For explicit casts](../functions/cast/)

### Equality

Values in custom types are _never_ considered equal to:

- Other custom types, irrespective of their structure or value.
- Built-in types, but built-in types can be coerced to and from structurally
  equivalent custom types.

### Polymorphism

When using custom types as values for [polymorphic
functions](list/#polymorphism), the following additional constraints apply:

- If any value passed to a polymorphic parameter is a custom type, the resultant
  type must use the custom type in the appropriate location.

  For example, if a custom type is used as:
  - `listany`, the resultant `list` must be of exactly the same type.
  - `listelementany`, the resultant `list`'s element must be of the custom type.
- If custom types and built-in types are both used, the resultant type is the
  "least custom type" that can be derived––i.e. the resultant type will have the
  fewest possible layers of custom types that still fulfill all constraints.
  Materialize will neither create nor discover a custom type that fills the
  constraints, nor will it coerce a custom type to a built-in type.

  For example, if appending a custom `list` to a built-in `list list`, the
  resultant type will be a `list` of custom `list`s.

#### Examples

This is a little easier to understand if we make it concrete, so we'll focus on
concatenating two lists and appending an element to list.

For these operations, Materialize uses the following polymorphic parameters:

- `listany`, which accepts any `list`, and constrains all lists to being of the
  same structurally equivalent type.
- `listelementany`, which accepts any type, but must be equal to the element
  type of the `list` type used with `listany`. For instance, if `listany` is
  constrained to being `int4 list`, `listelementany` must be `int4`.

When concatenating two lists, we'll use `list_cat` whose signature is
`list_cat(l: listany, r: listany)`.

If we concatenate a custom `list` (in this example, `custom_list`) and a
structurally equivalent built-in `list` (`int4 list`), the result is of the same
type as the custom `list` (`custom_list`).

```mzsql
CREATE TYPE custom_list AS LIST (ELEMENT TYPE int4);

SELECT pg_typeof(
  list_cat('{1}'::custom_list, '{2}'::int4 list)
) AS custom_list_built_in_list_cat;

```
```nofmt
 custom_list_built_in_list_cat
-------------------------------
 custom_list
```

When appending an element to a list, we'll use `list_append` whose signature is
`list_append(l: listany, e: listelementany)`.

If we append a structurally appropriate element (`int4`) to a custom `list`
(`custom_list`), the result is of the same type as the custom `list`
(`custom_list`).

```mzsql
SELECT pg_typeof(
  list_append('{1}'::custom_list, 2)
) AS custom_list_built_in_element_cat;

```
```nofmt
 custom_list_built_in_element_cat
----------------------------------
 custom_list
```

If we append a structurally appropriate custom element (`custom_list`) to a
built-in `list` (`int4 list list`), the result is a `list` of custom elements.

```mzsql
SELECT pg_typeof(
  list_append('{{1}}'::int4 list list, '{2}'::custom_list)
) AS built_in_list_custom_element_append;

```
```nofmt
 built_in_list_custom_element_append
-------------------------------------
 custom_list list
```

This is the "least custom type" we could support for these values––i.e.
Materialize will not create or discover a custom type whose elements are
`custom_list`, nor will it coerce `custom_list` into an anonymous built-in
list.

Note that `custom_list list` is considered a custom type because it contains a
reference to a custom type. Because it's a custom type, it enforces custom
types' polymorphic constraints.

For example, values of type `custom_list list` and `custom_nested_list` cannot
both be used as `listany` values for the same function:

```mzsql
CREATE TYPE custom_nested_list AS LIST (element_type=custom_list);

SELECT list_cat(
  -- result is "custom_list list"
  list_append('{{1}}'::int4 list list, '{2}'::custom_list),
  -- result is custom_nested_list
  '{{3}}'::custom_nested_list
);
```
```nofmt
ERROR: Cannot call function list_cat(custom_list list, custom_nested_list)...
```

As another example, when using `custom_list list` values for `listany`
parameters, you can only use `custom_list` or `int4 list` values for
`listelementany` parameters––using any other custom type will fail:

```mzsql
CREATE TYPE second_custom_list AS LIST (element_type=int4);

SELECT list_append(
  -- elements are custom_list
  '{{1}}'::custom_nested_list,
  -- second_custom_list is not interoperable with custom_list because both
  -- are custom
  '{2}'::second_custom_list
);
```
```nofmt
ERROR:  Cannot call function list_append(custom_nested_list, second_custom_list)...
```

To make custom types interoperable, you must cast them to the same type. For
example, casting `custom_nested_list` to `custom_list list` (or vice versa)
makes the values passed to `listany` parameters of the same custom type:

```mzsql
SELECT pg_typeof(
  list_cat(
    -- result is "custom_list list"
    list_append(
      '{{1}}'::int4 list list,
      '{2}'::custom_list
    ),
    -- result is "custom_list list"
    '{{3}}'::custom_nested_list::custom_list list
  )
) AS complex_list_cat;
```
```nofmt
 complex_list_cat
------------------
 custom_list list
```

[create-type]: ../create-type




---

## Array types


Arrays are a multidimensional sequence of any non-array type.

{{< warning >}}
We do not recommend using arrays, which exist in Materialize primarily to
facilitate compatibility with PostgreSQL. Specifically, many of the PostgreSQL
compatibility views in the [system catalog](/sql/system-catalog/) must expose
array types. Unfortunately, PostgreSQL arrays have odd semantics and do not
interoperate well with modern data formats like JSON and Avro.

Use the [`list` type](/sql/types/list) instead.
{{< /warning >}}

## Details

### Type name

The name of an array type is the name of an element type followed by square
brackets (`[]`) . For example, the type `int[]` specifies an integer array.

For compatibility with PostgreSQL, array types may optionally indicate
additional dimensions, as in `int[][][]`, or the sizes of dimensions, as in
`int[3][4]`. However, as in PostgreSQL, these additional annotations are
ignored. Arrays of the same element type are considered to be of the same type
regardless of their dimensions. For example, the type `int[3][4]` is exactly
equivalent to the type `int[]`.

To reduce confusion, we recommend that you use the simpler form of the type name
whenever possible.

### Construction

You can construct arrays using the special `ARRAY` expression:

```mzsql
SELECT ARRAY[1, 2, 3]
```
```nofmt
  array
---------
 {1,2,3}
```

You can nest `ARRAY` constructors to create multidimensional arrays:

```mzsql
SELECT ARRAY[ARRAY['a', 'b'], ARRAY['c', 'd']]
```
```nofmt
     array
---------------
 {{a,b},{c,d}}
```

Alternatively, you can construct an array from the results subquery.  These subqueries must return a single column. Note
that, in this form of the `ARRAY` expression, parentheses are used rather than square brackets.

```mzsql
SELECT ARRAY(SELECT x FROM test0 WHERE x > 0 ORDER BY x DESC LIMIT 3);
```
```nofmt
    x
---------
 {4,3,2}
```

Arrays cannot be "ragged." The length of each array expression must equal the
length of all other array constructors in the same dimension. For example, the
following ragged array is rejected:

```mzsql
SELECT ARRAY[ARRAY[1, 2], ARRAY[3]]
```
```nofmt
ERROR:  number of array elements (3) does not match declared cardinality (4)
```

### Textual format

The textual representation of an array consists of an opening curly brace (`{`),
followed by the textual representation of each element separated by commas
(`,`), followed by a closing curly brace (`}`). For multidimensional arrays,
this format is applied recursively to each array dimension. No additional
whitespace is added.

Null elements are rendered as the literal string `NULL`. Non-null elements are
rendered as if that element had been cast to `text`.

An element whose textual representation contains curly braces, commas,
whitespace, double quotes, backslashes, or is exactly the string `NULL` (in any
case) is wrapped in double quotes in order to distinguish the representation of
the element from the representation of the containing array. Within double
quotes, backslashes and double quotes are backslash-escaped.

The following example demonstrates the output format and includes many of the
aforementioned special cases.

```mzsql
SELECT ARRAY[ARRAY['a', 'white space'], ARRAY[NULL, ''], ARRAY['escape"m\e', 'nUlL']]
```
```nofmt
                         array
-------------------------------------------------------
 {{a,"white space"},{NULL,""},{"escape\"m\\e","nUlL"}}
```

### Catalog names

Builtin types (e.g. `integer`) have a builtin array type that can be referred to
by prefixing the type catalog name name with an underscore. For example,
`integer`'s catalog name is `pg_catalog.int4`, so its array type's catalog name
is `pg_catalog_int4`.

Array element | Catalog name | OID
--------------|--------------|-----
[`bigint`](../bigint) | `pg_catalog._int8` | 1016
[`boolean`](../boolean) | `pg_catalog._bool` | 1000
[`date`](../date) | `pg_catalog._date` | 1182
[`double precision`](../float) | `pg_catalog._float8` | 1022
[`integer`](../integer) | `pg_catalog._bool` | 1007
[`interval`](../interval) | `pg_catalog._interval` | 1187
[`jsonb`](../jsonb) | `pg_catalog.3807` | 1000
[`numeric`](../numeric) | `pg_catalog._numeric` | 1231
[`oid`](../oid) | `pg_catalog._oid` | 1028
[`real`](../float) | `pg_catalog._float4` | 1021
[`text`](../text) | `pg_catalog._bool` | 1009
[`time`](../time) | `pg_catalog._time` | 1183
[`timestamp`](../timestamp) | `pg_catalog._timestamp` | 1115
[`timestamp with time zone`](../timestamp) | `pg_catalog._timestamptz` | 1185
[`uuid`](../uuid) | `pg_catalog._uuid` | 2951

### Valid casts

You can [cast](/sql/functions/cast) all array types to:
- [`text`](../text) (by assignment)
- [`list`](../list) (explicit)

You can cast `text` to any array type. The input must conform to the [textual
format](#textual-format) described above, with the additional restriction that
you cannot yet use a cast to construct a multidimensional array.

### Array to `list` casts

You can cast any type of array to a list of the same element type, as long as
the array has only 0 or 1 dimensions, i.e. you can cast `integer[]` to `integer
list`, as long as the array is empty or does not contain any arrays itself.

```mzsql
SELECT pg_typeof('{1,2,3}`::integer[]::integer list);
```
```
integer list
```

## Examples

```mzsql
SELECT '{1,2,3}'::int[]
```
```nofmt
  int4
---------
 {1,2,3}
```

```mzsql
SELECT ARRAY[ARRAY[1, 2], ARRAY[NULL, 4]]::text
```
```nofmt
      array
------------------
 {{1,2},{NULL,4}}
```




---

## boolean type


`boolean` data expresses a binary value of either `TRUE` or `FALSE`.

Detail | Info
-------|------
**Quick Syntax** | `TRUE` or `FALSE`
**Size** | 1 byte
**Aliases** | `bool`
**Catalog name** | `pg_catalog.bool`
**OID** | 16

## Syntax

{{< diagram "type-bool.svg" >}}

## Details

### Valid casts

#### From `boolean`

You can [cast](../../functions/cast) from `boolean` to:

- [`int`](../int) (explicitly)
- [`text`](../text) (by assignment)

#### To `boolean`

You can [cast](../../functions/cast) the following types to `boolean`:

- [`int`](../int) (explicitly)
- [`jsonb`](../jsonb) (explicitly)
- [`text`](../text) (explicitly)

## Examples

```mzsql
SELECT TRUE AS t_val;
```
```nofmt
 t_val
-------
 t
```

```mzsql
SELECT FALSE AS f_val;
 f_val
-------
 f
```




---

## bytea type


The `bytea` data type allows the storage of [binary strings](https://www.postgresql.org/docs/9.0/datatype-binary.html) or what is typically thought of as "raw bytes". Materialize supports both the typical formats for input and output: the hex format and the historical PostgreSQL escape format. The hex format is preferred.

Hex format strings are preceded by `\x` and escape format strings are preceded by `\`.

For more information about `bytea`, see the [PostgreSQL binary data type documentation](https://www.postgresql.org/docs/13/datatype-binary.html#id-1.5.7.12.9).

Detail | Info
-------|------
**Quick Syntax** | `'\xDEADBEEF'` (hex),  `'\000'` (escape)
**Size** | 1 or 4 bytes plus the actual binary string
**Catalog name** | `pg_catalog.bytea`
**OID** | 17

## Syntax

### Hex format

{{< diagram "type-bytea-hex.svg" >}}

In some cases, the initial backslash may need to be escaped by doubling it (`\\`). For more information, see the PostgreSQL documentation on [string constants](https://www.postgresql.org/docs/13/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS).

### Escape format

{{< diagram "type-bytea-esc.svg" >}}

In the escape format, octet values can be escaped by converting them into their three-digit octal values and preceding them with backslashes; the backslash itself can be escaped as a double backslash. While any octet value *can* be escaped, the values in the table below *must* be escaped.

Decimal octet value | Description | Escaped input representation | Example | Hex representation
------------|--------|----|-----------|----
0  | zero octet | `'\000'` | `'\000'::bytea` | `\x00`
39  | single quote |`''''` or `'\047'` | `''''::bytea` | `\x27`
92  | backslash | `'\\' or '\134'` | `'\\'::bytea` | `\x5c`
0 to 31 and 127 to 255  | "non-printable" octets | `'\xxx'` (octal value) | `'\001'::bytea` | `\x01`

## Details

### Valid casts

#### From `bytea`

You can [cast](../../functions/cast) `bytea` to [`text`](../text) by assignment.

{{< warning >}}
Casting a `bytea` value to `text` unconditionally returns a
[hex-formatted](#hex-format) string, even if the byte array consists entirely of
printable characters. See [handling character data](#handling-character-data)
for alternatives.
{{< /warning >}}

#### To `bytea`

You can explicitly [cast](../../functions/cast) [`text`](../text) to `bytea`.

### Handling character data

Unless a `text` value is a [hex-formatted](#hex-format) string, casting to
`bytea` will encode characters using UTF-8:

```mzsql
SELECT 'hello 👋'::bytea;
```
```text
         bytea
------------------------
 \x68656c6c6f20f09f918b
```

The reverse, however, is not true. Casting a `bytea` value to `text` will not
decode UTF-8 bytes into characters. Instead, the cast unconditionally produces a
[hex-formatted](#hex-format) string:

```mzsql
SELECT '\x68656c6c6f20f09f918b'::bytea::text
```
```text
           text
----------------------------
 \x68656c6c6f2c20776f726c6
```

To decode UTF-8 bytes into characters, use the
[`convert_from`](../../functions#convert_from) function instead of casting:

```mzsql
SELECT convert_from('\x68656c6c6f20f09f918b', 'utf8') AS text;
```
```mzsql
  text
---------
 hello 👋
```

## Examples


```mzsql
SELECT '\xDEADBEEF'::bytea AS bytea_val;
```
```nofmt
 bytea_val
---------
 \xdeadbeef
```

<hr>

```mzsql
SELECT '\000'::bytea AS bytea_val;
```
```nofmt
   bytea_val
-----------------
 \x00
```




---

## date type


`date` data expresses a date without a specified time.

Detail | Info
-------|------
**Quick Syntax** | `DATE '2007-02-01'`
**Size** | 1 byte
**Catalog name** | `pg_catalog.date`
**OID** | 1082
**Min value** | 4714-11-24 BC
**Max value** | 262143-12-31 AD
**Resolution** | 1 day

## Syntax

{{< diagram "type-date.svg" >}}

Field | Use
------|----
_date&lowbar;str_ | A string representing a date in `Y-M-D`, `Y M-D`, `Y M D` or `YMD` format.
_time&lowbar;str_ | _(NOP)_ A string representing a time of day in `H:M:S.NS` format.
_tz&lowbar;offset_ | _(NOP)_ The timezone's distance, in hours, from UTC.

## Details

### Valid casts

#### From `date`

You can [cast](../../functions/cast) `date` to:

- [`text`](../text) (by assignment)
- [`timestamp`](../timestamp) (implicitly)
- [`timestamptz`](../timestamp) (implicitly)

#### To `date`

You can [cast](../../functions/cast) from the following types to `date`:

- [`text`](../text) (explicitly)
- [`timestamp`](../timestamp) (by assignment)
- [`timestamptz`](../timestamp) (by assignment)

### Valid operations

`time` data supports the following operations with other types.

Operation | Computes
----------|------------
[`date`](../date) `+` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `-` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `+` [`time`](../time) | [`timestamp`](../timestamp)
[`date`](../date) `-` [`date`](../date) | [`interval`](../interval)

## Examples

```mzsql
SELECT DATE '2007-02-01' AS date_v;
```
```nofmt
   date_v
------------
 2007-02-01
```




---

## Floating-point types


## `real` info

Detail | Info
-------|------
**Size** | 4 bytes
**Aliases** | `float4`
**Catalog name** | `pg_catalog.float4`
**OID** | 700
**Range** | Approx. 1E-37 to 1E+37 with 6 decimal digits of precision

## `double precision` info

Detail | Info
-------|------
**Size** | 8 bytes
**Aliases** | `float`,`float8`, `double`
**Catalog name** | `pg_catalog.float8`
**OID** | 701
**Range** | Approx. 1E-307 to 1E+307 with 15 decimal digits of precision

## Syntax

{{< diagram "type-float.svg" >}}

## Details

### Literals

Materialize assumes untyped numeric literals containing decimal points are
[`numeric`](../numeric); to use `float`, you must explicitly cast them as we've
done below.

### Special values

Floating-point numbers have three special values, as specified in IEEE 754:

Value       | Aliases                    | Represents
------------|----------------------------|-----------
`NaN`       |                            | Not a number
`Infinity`  | `Inf`, `+Infinity`, `+Inf` | Positive infinity
`-Infinity` | `-Inf`                     | Negative infinity

To input these special values, write them as a string and cast that string to
the desired floating-point type. For example:

```mzsql
SELECT 'NaN'::real AS nan
```
```nofmt
 nan
-----
 NaN
```

The strings are recognized case insensitively.

### Valid casts

In addition to the casts listed below, `real` and `double precision` values can be cast
to and from one another. The cast from `real` to `double precision` is implicit and the cast from `double precision` to `real` is by assignment.

#### From `real`

You can [cast](../../functions/cast) `real` or `double precision` to:

- [`int`](../int) (by assignment)
- [`numeric`](../numeric) (by assignment)
- [`text`](../text) (by assignment)

#### To `real`

You can [cast](../../functions/cast) to `real` or `double precision` from the following types:

- [`int`](../int) (implicitly)
- [`numeric`](../numeric) (implicitly)
- [`text`](../text) (explicitly)

## Examples

```mzsql
SELECT 1.23::real AS real_v;
```
```nofmt
 real_v
---------
    1.23
```




---

## Integer types


## `smallint` info

Detail | Info
-------|------
**Size** | 2 bytes
**Aliases** | `int2`
**Catalog name** | `pg_catalog.int2`
**OID** | 23
**Range** | [-32,768, 32,767]

## `integer` info

Detail | Info
-------|------
**Size** | 4 bytes
**Aliases** | `int`, `int4`
**Catalog name** | `pg_catalog.int4`
**OID** | 23
**Range** | [-2,147,483,648, 2,147,483,647]

## `bigint` info

Detail | Info
-------|------
**Size** | 8 bytes
**Aliases** | `int8`
**Catalog name** | `pg_catalog.int8`
**OID** | 20
**Range** | [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]

## Details

### Valid casts

For details about casting, including contexts, see [Functions:
Cast](../../functions/cast).

#### Between integer types

From | To | Required context
-----|----|--------
`smallint` | `integer` | Implicit
`smallint` | `bigint` | Implicit
`integer` | `smallint` | Assignment
`integer` | `bigint` | Implicit
`bigint` | `smallint` | Assignment
`bigint` | `integer` | Assignment

#### From integer types

You can cast integer types to:

To | Required context
---|--------
[`boolean`](../boolean) (`integer` only) | Explicit
[`numeric`](../numeric) | Implicit
[`oid`](../oid) | Implicit
[`real`/`double precision`](../float) | Implicit
[`text`](../text) | Assignment
[`uint2`/`uint4`/`uint8`](../uint) | Depends on specific cast

#### To `integer` or `bigint`

You can cast the following types to integer types:

From | Required context
---|--------
[`boolean`](../boolean) (`integer` only) | Explicit
[`jsonb`](../jsonb) | Explicit
[`oid`](../oid) (`integer` and `bigint` only) | Assignment
[`numeric`](../numeric) | Assignment
[`real`/`double precision`](../float) | Assignment
[`text`](../text) | Explicit
[`uint2`/`uint4`/`uint8`](../uint) | Depends on specific cast

## Examples

```mzsql
SELECT 123::integer AS int_v;
```
```nofmt
 int_v
-------
   123
```

<hr/>

```mzsql
SELECT 1.23::integer AS int_v;
```
```nofmt
 int_v
-------
     1
```




---

## interval type


`interval` data expresses a duration of time.

`interval` data keeps months, days, and microseconds completely separate and will not try to convert between any of
those fields when comparing `interval`s. This may lead to some unexpected behavior. For example `1 month` is considered
greater than `100 days`. See ['justify_days'](../../functions/justify-days), ['justify_hours'](../../functions/justify-hours), and
['justify_interval'](../../functions/justify-interval) to explicitly convert between these fields.

Detail | Info
-------|-----
**Quick Syntax** | `INTERVAL '1' MINUTE` <br/> `INTERVAL '1-2 3 4:5:6.7'` <br/>`INTERVAL '1 year 2.3 days 4.5 seconds'`
**Size** | 20 bytes
**Catalog name** | `pg_catalog.interval`
**OID** | 1186
**Min value** | -178956970 years -8 months -2147483648 days -2562047788:00:54.775808
**Max value** | 178956970 years 7 months 2147483647 days 2562047788:00:54.775807

## Syntax

#### INTERVAL

{{< diagram "type-interval-val.svg" >}}

#### `time_expr`

{{< diagram "type-interval-time-expr.svg" >}}

#### `time_unit`

{{< diagram "time-unit.svg" >}}

Field | Use
------|----
_ym&lowbar;str_ | A string representing years and months in `Y-M D` format.
_time&lowbar;str_ | A string representing hours, minutes, seconds, and nanoseconds in `H:M:S.NS` format.
_head&lowbar;time&lowbar;unit_ | Return an interval without `time_unit`s larger than `head_time_unit`. Note that this differs from PostgreSQL's implementation, which ignores this clause.
_tail&lowbar;time&lowbar;unit_ | 1. Return an interval without `time_unit` smaller than `tail_time_unit`.<br/><br/>2. If the final `time_expr` is only a number, treat the `time_expr` as belonging to `tail_time_unit`. This is the case of the most common `interval` format like `INTERVAL '1' MINUTE`.

## Details

### `time_expr` Syntax

Materialize strives for full PostgreSQL compatibility with `time_exprs`, which
offers support for two types of `time_expr` syntax:

- SQL Standard, i.e. `'Y-M D H:M:S.NS'`
- PostgreSQL, i.e. repeated `int.frac time_unit`, e.g.:
    - `'1 year 2 months 3.4 days 5 hours 6 minutes 7 seconds 8 milliseconds'`
    - `'1y 2mon 3.4d 5h 6m 7s 8ms'`

Like PostgreSQL, Materialize's implementation includes the following
stipulations:

- You can freely mix SQL Standard- and PostgreSQL-style `time_expr`s.
- You can write `time_expr`s in any order, e.g `'H:M:S.NS Y-M'`.
- Each `time_unit` can only be written once.
- SQL Standard `time_expr` uses the following groups of `time_unit`s:

    - `Y-M`
    - `D`
    - `H:M:S.NS`

    Using a SQL Standard `time_expr` to write to any of these `time_units`
    writes to all other `time_units` in the same group, even if that `time_unit`
    is not explicitly referenced.

    For example, the `time_expr` `'1:2'` (1 hour, 2 minutes) also writes a value
    of 0 seconds. You cannot then include another `time_expr` which writes to
    the seconds `time_unit`.
- A two-field `time_expr` like `'1:2'` is by default interpreted as (hour, minute)
  while `1:2 MINUTE TO SECOND` interprets as (minute, seconds).
- Only PostgreSQL `time_expr`s support non-second fractional `time_units`, e.g.
    `1.2 days`. Materialize only supports 9 places of decimal precision.

### Valid casts

#### From `interval`

You can [cast](../../functions/cast) `interval` to:

- [`text`](../text) (by assignment)
- [`time`](../time)  (by assignment)

#### To `interval`

You can [cast](../../functions/cast) from the following types to `interval`:

- [`text`](../text) (explicitly)
- [`time`](../time)  (explicitly)

### Valid operations

`interval` data supports the following operations with other types.

Operation | Computes | Notes
----------|----------|-------
[`date`](../date) `+` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `-` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `-` [`date`](../date) | [`interval`](../interval)
[`timestamp`](../timestamp) `+` [`interval`](../interval) | [`timestamp`](../timestamp)
[`timestamp`](../timestamp) `-` [`interval`](../interval) | [`timestamp`](../timestamp)
[`timestamp`](../timestamp) `-` [`timestamp`](../timestamp) | [`interval`](../interval)
[`time`](../time) `+` [`interval`](../interval) | `time`
[`time`](../time) `-` [`interval`](../interval) | `time`
[`time`](../time) `-` [`time`](../time) | [`interval`](../interval)
[`interval`](../interval) `*` [`double precision`](../float) | [`interval`](../interval) |
[`interval`](../interval) `/` [`double precision`](../float) | [`interval`](../interval) |

## Examples

```mzsql
SELECT INTERVAL '1' MINUTE AS interval_m;
```

```nofmt
 interval_m
------------
 00:01:00
```

### SQL Standard syntax

```mzsql
SELECT INTERVAL '1-2 3 4:5:6.7' AS interval_p;
```

```nofmt
            interval_f
-----------------------------------
 1 year 2 months 3 days 04:05:06.7
```

### PostgreSQL syntax

```mzsql
SELECT INTERVAL '1 year 2.3 days 4.5 seconds' AS interval_p;
```

```nofmt
        interval_p
--------------------------
 1 year 2 days 07:12:04.5
```

### Negative intervals

`interval_n` demonstrates using negative and positive components in an interval.

```mzsql
SELECT INTERVAL '-1 day 2:3:4.5' AS interval_n;
```

```nofmt
 interval_n
-------------
 -1 days +02:03:04.5
```

### Truncating interval

`interval_r` demonstrates how `head_time_unit` and `tail_time_unit` truncate the
interval.

```mzsql
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

```mzsql
SELECT INTERVAL '1 day 2-3 4' MINUTE AS interval_w;
```

```nofmt
           interval_w
---------------------------------
 2 years 3 months 1 day 00:04:00
```

### Interaction with timestamps

```mzsql
SELECT TIMESTAMP '2020-01-01 8:00:00' + INTERVAL '1' DAY AS ts_interaction;
```

```nofmt
   ts_interaction
---------------------
 2020-01-02 08:00:00
```




---

## jsonb type


`jsonb` data expresses a JSON object similar to
[PostgreSQL's implementation](https://www.postgresql.org/docs/current/datatype-json.html).

Detail | Info
-------|------
**Quick Syntax** | `'{"1":2,"3":4}'::jsonb`
**Size** | Variable
**Catalog name** | `pg_catalog.jsonb`
**OID** | 3802

Materialize does not yet support a type more similar to PostgreSQL's
implementation of `json`.

## Syntax

{{< diagram "type-jsonb.svg" >}}

Field | Use
------|-----
_json&lowbar;string_ | A well-formed [JSON object](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON).

## `jsonb` functions + operators

Materialize supports the following operators and functions.

### Operators

{{% json-operators %}}

### Functions

{{< fnlist "JSON" >}}

#### Detail

Functions that return `Col`s are considered table functions and can only be used
as tables, i.e. you cannot use them as scalar values. For example, you can only
use `jsonb_object_keys` in the following way:

```mzsql
SELECT * FROM jsonb_object_keys('{"1":2,"3":4}'::jsonb);
```

## Details

- `jsonb` elements can be of the following types:
  - Objects
  - Arrays
  - String
  - Number
  - Boolean
  - Null
- Numbers in `jsonb` elements are all equivalent to
  [`numeric`](/sql/types/numeric) in SQL.

### Valid casts

#### From `jsonb`

You can [cast](../../functions/cast) `jsonb` to:

- [`boolean`](../boolean) (explicitly)
- [`numeric`](../numeric) (explicitly)
- [`int`](../integer) (explicitly)
- [`real`/`double precision`](../float) (explicitly)
- [`text`](../text) (by assignment) (stringifies `jsonb`)

#### To `jsonb`

You can explicitly [cast](../../functions/cast) from [`text`](../text) to `jsonb`.

#### Notes about converting `jsonb` to `text`

`jsonb` can have some odd-feeling corner cases when converting to or from
[`text`](/sql/types/text).

- `jsonb::text` always produces the printed version of the JSON.

    ```mzsql
    SELECT ('"a"'::jsonb)::text AS jsonb_elem;
    ```
    ```nofmt
     jsonb_elem
    ------------
     "a"
    ```

- `->>` and the `_text` functions produce the printed version of the inner
  element, unless the output is a single JSON string in which case they print it
  without quotes, i.e. as a SQL `text` value.

    ```mzsql
    SELECT ('"a"'::jsonb)->>0 AS string_elem;
    ```
    ```nofmt
     jsonb_elem
    ------------
     a
    ```

- `text` values passed to `to_jsonb` with quotes (`"`) produced `jsonb` strings
  with the quotes escaped.

    ```mzsql
    SELECT to_jsonb('"foo"') AS escaped_quotes;
    ```
    ```nofmt
     escaped_quotes
    ----------------
     "\"foo\""
    ```

### Subscripting

You can use subscript notation (`[]`) to extract an element from a `jsonb` array
or object.

The returned value is always of type `jsonb`. If the requested array element or
object key does not exist, or if either the input value or subscript value is
`NULL`, the subscript operation returns `NULL`.

#### Arrays

To extract an element from an array, supply the 0-indexed position as the
subscript:

```mzsql
SELECT ('[1, 2, 3]'::jsonb)[1]
```
```nofmt
 jsonb
-------
 2
```

Negative indexes count backwards from the end of the array. [Slice syntax] is
not supported. Note also that 0-indexed positions are at variance with [`list`]
and [`array`] types, whose subscripting operation uses 1-indexed positions.

#### Objects

To extract a value from an object, supply the key as the subscript:

```mzsql
SELECT ('{"a": 1, "b": 2, "c": 3}'::jsonb)['b'];
```
```nofmt
 jsonb
-------
 2
```

You can chain subscript operations to retrieve deeply nested elements:

```mzsql
SELECT ('{"1": 2, "a": ["b", "c"]}'::jsonb)['a'][1];
```
```nofmt
 jsonb
-------
 "c"
```

#### Remarks

Because the output type of the subscript operation is always `jsonb`, when
comparing the output of a subscript to a string, you must supply a JSON string
to compare against:

```mzsql
SELECT ('["a", "b"]::jsonb)[1] = '"b"'
```

Note the extra double quotes on the right-hand side of the comparison.

### Parsing

{{< json-parser >}}

## Examples

### Operators

#### Field access as `jsonb` (`->`)

The type of JSON element you're accessing dictates the RHS's type.

- Use a `string` to return the value for a specific key:

  ```mzsql
  SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'1' AS field_jsonb;
  ```
  ```nofmt
   field_jsonb
  -------------
   2
  ```

- Use an `int` to return the value in an array at a specific index:

  ```mzsql
  SELECT '["1", "a", 2]'::jsonb->1 AS field_jsonb;
  ```
  ```nofmt
   field_jsonb
  -------------
   "a"
  ```
Field accessors can also be chained together.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'a'->1 AS field_jsonb;
```
```nofmt
 field_jsonb
-------------
 "c"
```

Note that all returned values are `jsonb`.

<hr/>

#### Field access as `text` (`->>`)

The type of JSON element you're accessing dictates the RHS's type.

- Use a `string` to return the value for a specific key:

  ```mzsql
  SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->>'1' AS field_text;
  ```
  ```nofmt
   field_text
  -------------
   2
  ```

- Use an `int` to return the value in an array at a specific index:

  ```mzsql
  SELECT '["1", "a", 2]'::jsonb->>1 AS field_text;
  ```
  ```nofmt
   field_text
  -------------
   a
  ```

Field accessors can also be chained together, as long as the LHS remains
`jsonb`.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'a'->>1 AS field_text;
```
```nofmt
 field_text
-------------
 c
```

Note that all returned values are `string`.

#### Path access as `jsonb` (`#>`)

You can access specific elements in a `jsonb` value using a "path", which is a
[text array](/sql/types/array) where each element is either a field key or an
array element:

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb #> '{a,1}' AS field_jsonb;
```
```nofmt
 field_jsonb
-------------
 "c"
```

The operator returns a value of type `jsonb`. If the path is invalid, it returns
`NULL`.

#### Path access as `text` (`#>>`)

The `#>>` operator is equivalent to the [`#>`](#path-access-as-jsonb-) operator,
except that the operator returns a value of type `text`.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb #>> '{a,1}' AS field_text;
```
```nofmt
 field_text
-------------
 c
```

<hr/>

#### `jsonb` concat (`||`)

```mzsql
SELECT '{"1": 2}'::jsonb ||
       '{"a": ["b", "c"]}'::jsonb AS concat;
```
```nofmt
             concat
---------------------------------
 {"1":2,"a":["b","c"]}
```

<hr/>

#### Remove key (`-`)

```mzsql
 SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb - 'a' AS rm_key;
```
```nofmt
  rm_key
-----------
 {"1":2}
```

<hr/>

#### LHS contains RHS (`@>`)

Here, the left hand side does contain the right hand side, so the result is `t` for true.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb @>
       '{"1": 2}'::jsonb AS lhs_contains_rhs;
```
```nofmt
 lhs_contains_rhs
------------------
 t
```

<hr/>

#### RHS contains LHS (`<@`)

Here, the right hand side does contain the left hand side, so the result is `t` for true.

```mzsql
SELECT '{"1": 2}'::jsonb <@
       '{"1": 2, "a": ["b", "c"]}'::jsonb AS lhs_contains_rhs;
```
```nofmt
 rhs_contains_lhs
------------------
 t
```

<hr/>

#### Search top-level keys (`?`)

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb ? 'a' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 t
```

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb ? 'b' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 f
```

### Functions

#### `jsonb_array_elements`

##### Expanding a JSON array

```mzsql
SELECT * FROM jsonb_array_elements('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
   value
-----------
 true
 1.0
 "a"
 {"b":2.0}
 null
```

##### Flattening a JSON array

```mzsql
SELECT t.id,
       obj->>'a' AS a,
       obj->>'b' AS b
FROM (
  VALUES
    (1, '[{"a":1,"b":2},{"a":3,"b":4}]'::jsonb),
    (2, '[{"a":5,"b":6},{"a":7,"b":8}]'::jsonb)
) AS t(id, json_col)
CROSS JOIN jsonb_array_elements(t.json_col) AS obj;
```

```nofmt
 id | a | b
----+---+---
  1 | 1 | 2
  1 | 3 | 4
  2 | 5 | 6
  2 | 7 | 8
```

<hr/>


#### `jsonb_array_elements_text`

```mzsql
SELECT * FROM jsonb_array_elements_text('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
   value
-----------
 true
 1.0
 "a"
 {"b":2.0}
 null
```

<hr/>

#### `jsonb_array_length`

```mzsql
SELECT jsonb_array_length('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
 jsonb_array_length
--------------------
                  5
```

<hr/>

#### `jsonb_build_array`

```mzsql
SELECT jsonb_build_array('a', 1::float, 2.0::float, true);
```
```nofmt
 jsonb_build_array
--------------------
 ["a",1.0,2.0,true]
```

<hr/>

#### `jsonb_build_object`

```mzsql
SELECT jsonb_build_object(2.0::float, 'b', 'a', 1.1::float);
```
```nofmt
 jsonb_build_object
--------------------
 {"2":"b","a":1.1}
```

<hr/>

#### `jsonb_each`

```mzsql
SELECT * FROM jsonb_each('{"1": 2.1, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 key |   value
-----+-----------
 1   | 2.1
 a   | ["b","c"]
```

Note that the `value` column is `jsonb`.

<hr/>

#### `jsonb_each_text`

```mzsql
SELECT * FROM jsonb_each_text('{"1": 2.1, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 key |   value
-----+-----------
 1   | 2.1
 a   | ["b","c"]
```

Note that the `value` column is `string`.

<hr/>

#### `jsonb_object_keys`

```mzsql
SELECT * FROM jsonb_object_keys('{"1": 2, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 jsonb_object_keys
-------------------
 1
 a
```

<hr/>

#### `jsonb_pretty`

```mzsql
SELECT jsonb_pretty('{"1": 2, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 jsonb_pretty
--------------
 {           +
   "1": 2,   +
   "a": [    +
     "b",    +
     "c"     +
   ]         +
 }
```

<hr/>

#### `jsonb_typeof`

```mzsql
SELECT jsonb_typeof('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
 jsonb_typeof
--------------
 array
```

```mzsql
SELECT * FROM jsonb_typeof('{"1": 2, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 jsonb_typeof
--------------
 object
```

<hr/>

#### `jsonb_strip_nulls`

```mzsql
SELECT jsonb_strip_nulls('[{"1":"a","2":null},"b",null,"c"]'::jsonb);
```
```nofmt
    jsonb_strip_nulls
--------------------------
 [{"1":"a"},"b",null,"c"]
```

<hr/>

#### `to_jsonb`

```mzsql
SELECT to_jsonb(t) AS jsonified_row
FROM (
  VALUES
  (1, 'hey'),
  (2, NULL),
  (3, 'hi'),
  (4, 'salutations')
  ) AS t(id, content)
WHERE t.content LIKE 'h%';
```
```nofmt
      jsonified_row
--------------------------
 {"content":"hi","id":3}
 {"content":"hey","id":1}
```

Note that the output is `jsonb`.

[Slice syntax]: /sql/types/list#slicing-ranges
[`list`]: /sql/types/list
[`array`]: /sql/types/array




---

## List types


Lists are ordered sequences of homogenously typed elements. Lists' elements can
be other lists, known as "layered lists."

| Detail           | Info                                         |
| ---------------- | -------------------------------------------- |
| **Quick Syntax** | `LIST[[1,2],[3]]`                            |
| **Size**         | Variable                                     |
| **Catalog name** | Anonymous, but [nameable](../../create-type) |

## Syntax

{{< diagram "type-list.svg" >}}

| Field     | Use                                                                                                       |
| --------- | --------------------------------------------------------------------------------------------------------- |
| _element_ | An element of any [data type](../) to place in the list. Note that all elements must be of the same type. |

## List functions + operators

#### Polymorphism

<!--
  If any type other than list supports fully polymorphic functions, this
  should be moved to doc/user/content/sql/types/_index.md
-->

List functions and operators are polymorphic, which applies the following
constraints to their arguments:

- All instances of `listany` must be lists of the same type.
- All instances of `listelementany` must be of the same type.
- If a function uses both `listany` and `listelementany` parameters, all
  instances of `listany` must be a list of the type used in `listelementany`.
- If any value passed to a polymorphic parameter is a [custom type](/sql/types/#custom-types), additional constraints apply.

### Operators

{{% list-operators %}}

### Functions

{{< fnlist "List" >}}

## Details

### Type name

The name of a list type is the name of its element type followed by `list`, e.g.
`int list`. This rule can be applied recursively, e.g. `int list list` for a
`list` of `int list`s which is a two-layer list.

### Construction

You can construct lists using the `LIST` expression:

```mzsql
SELECT LIST[1, 2, 3];
```
```nofmt
  list
---------
 {1,2,3}
```

You can nest `LIST` constructors to create layered lists:

```mzsql
SELECT LIST[LIST['a', 'b'], LIST['c']];
```
```nofmt
    list
-------------
 {{a,b},{c}}
```

You can also elide the `LIST` keyword from the interior list expressions:

```mzsql
SELECT LIST[['a', 'b'], ['c']];
```
```nofmt
    list
-------------
 {{a,b},{c}}
```

Alternatively, you can construct a list from the results of a subquery. The
subquery must return a single column. Note that, in this form of the `LIST`
expression, parentheses are used rather than square brackets.

```mzsql
SELECT LIST(SELECT x FROM test0 WHERE x > 0 ORDER BY x DESC LIMIT 3);
```
```nofmt
    x
---------
 {4,3,2}
```

Layered lists can be “ragged”, i.e. the length of lists in each layer can differ
from one another. This differs from `array`, which requires that each dimension
of a multidimensional array only contain arrays of the same length.

Note that you can also construct lists using the available [`text`
cast](#text-to-list-casts).

### Accessing lists

You can access elements of lists through:

  - [Indexing](#indexing-elements) for individual elements
  - [Slicing](#slicing-ranges) for ranges of elements

#### Indexing elements

To access an individual element of list, you can “index” into it using brackets
(`[]`) and 1-index element positions:

```mzsql
SELECT LIST[['a', 'b'], ['c']][1];
```
```nofmt
 ?column?
----------
 {a,b}
```

Indexing operations can be chained together to descend the list’s layers:

```mzsql
SELECT LIST[['a', 'b'], ['c']][1][2];
```
```nofmt
 ?column?
----------
 b
```

If the index is invalid (either less than 1 or greater than the maximum index),
lists return _NULL_.

```mzsql
SELECT LIST[['a', 'b'], ['c']][1][5] AS exceed_index;
```
```nofmt
 exceed_index
--------------

```

Lists have types based on their layers (unlike arrays' dimension), and error if
you attempt to index a non-list element (i.e. indexing past the list’s last
layer):

```mzsql
SELECT LIST[['a', 'b'], ['c']][1][2][3];
```
```nofmt
ERROR:  cannot subscript type string
```

#### Slicing ranges

To access contiguous ranges of a list, you can slice it using `[first index :
last index]`, using 1-indexed positions:

```mzsql
SELECT LIST[1,2,3,4,5][2:4] AS two_to_four;
```
```nofmt
  slice
---------
 {2,3,4}
```

You can omit the first index to use the first value in the list, and omit the
last index to use all elements remaining in the list.

```mzsql
SELECT LIST[1,2,3,4,5][:3] AS one_to_three;
```
```nofmt
 one_to_three
--------------
 {1,2,3}
```

```mzsql
SELECT LIST[1,2,3,4,5][3:] AS three_to_five;
```
```nofmt
 three_to_five
---------------
 {3,4,5}
```

If the first index exceeds the list's maximum index, the operation returns an
empty list:

```mzsql
SELECT LIST[1,2,3,4,5][10:] AS exceed_index;
```
```nofmt
 exceed_index
--------------
 {}
```

If the last index exceeds the list’s maximum index, the operation returns all
remaining elements up to its final element.

```mzsql
SELECT LIST[1,2,3,4,5][2:10] AS two_to_end;
```
```nofmt
 two_to_end
------------
 {2,3,4,5}
```

Performing successive slices behaves more like a traditional programming
language taking slices of an array, rather than PostgreSQL's slicing, which
descends into each layer.

```mzsql
SELECT LIST[1,2,3,4,5][2:][2:3] AS successive;
```
```nofmt
 successive
------------
 {3,4}
```

### Output format

We represent lists textually using an opening curly brace (`{`), followed by the
textual representation of each element separated by commas (`,`), terminated by
a closing curly brace (`}`). For layered lists, this format is applied
recursively to each list layer. No additional whitespace is added.

We render _NULL_ elements as the literal string `NULL`. Non-null elements are
rendered as if that element had been cast to `text`.

Elements whose textual representations contain curly braces, commas, whitespace,
double quotes, backslashes, or which are exactly the string `NULL` (in any case)
get wrapped in double quotes in order to distinguish the representation of the
element from the representation of the containing list. Within double quotes,
backslashes and double quotes are backslash-escaped.

The following example demonstrates the output format and includes many of the
aforementioned special cases.

```mzsql
SELECT LIST[['a', 'white space'], [NULL, ''], ['escape"m\e', 'nUlL']];
```
```nofmt
                        list
-----------------------------------------------------
 {{a,"white space"},{NULL,""},{"escape\"m\\e",nUlL}}
```

### `text` to `list` casts

To cast `text` to a `list`, you must format the text similar to list's [output
format](#output-format).

The text you cast must:

- Begin with an opening curly brace (`{`) and end with a closing curly brace
  (`}`)
- Separate each element with a comma (`,`)
- Use a representation for elements that can be cast from text to the list's
  element type.

    For example, to cast `text` to a `date list`, you use `date`'s `text`
    representation:

    ```mzsql
    SELECT '{2001-02-03, 2004-05-06}'::date list as date_list;
    ```

    ```nofmt
            date_list
    -------------------------
     {2001-02-03,2004-05-06}
    ```

    You cannot include the `DATE` keyword.
- Escape any special representations (`{`, `}`, `"`, `\`, whitespace, or the
  literal string `NULL`) you want parsed as text using...
    - Double quotes (`"`) to escape an entire contiguous string
    - Backslashes (`\`) to escape an individual character in any context, e.g.
      `\"` to escape double quotes within an escaped string.

      Note that escaping any character in the string "null" (case-insensitive)
      generates a `text` value equal to "NULL" and not a _NULL_ value.

    For example:

    ```mzsql
    SELECT '{
        "{brackets}",
        "\"quotes\"",
        \\slashes\\,
        \ leading space,
        trailing space\ ,
        \NULL
    }'::text list as escape_examples;
    ```

    ```nofmt
                                       escape_examples
    -------------------------------------------------------------------------------------
     {"{brackets}","\"quotes\"","\\slashes\\"," leading space","trailing space ","NULL"}
    ```

    Note that all unescaped whitespace is trimmed.

### List vs. array

`list` is a Materialize-specific type and is designed to provide:

- Similar semantics to Avro and JSON arrays
- A more ergonomic experience than PostgreSQL-style arrays

This section focuses on the distinctions between Materialize’s `list` and
`array` types, but with some knowledge of the PostgreSQL array type, you should
also be able to infer how list differs from it, as well.

#### Terminology

| Feature                         | Array term             | List term            |
| ------------------------------- | ---------------------- | -------------------- |
| **Nested structure**            | Multidimensional array | Layered list         |
| **Accessing single element**    | Subscripting           | Indexing<sup>1</sup> |
| **Accessing range of elements** | Subscripting           | Slicing<sup>1</sup>  |

<sup>1</sup>In some places, such as error messages, Materialize refers to
both list indexing and list slicing as subscripting.

#### Type definitions

**Lists** require explicitly declared layers, and each possible layer is treated
as a distinct type. For example, a list of `int`s with two layers is `int list
list` and one with three is `int list list list`. Because their number of layers
differ, they cannot be used interchangeably.

**Arrays** only have one type for each non-array type, and all arrays share that
type irrespective of their dimensions. This means that arrays of the same
element type can be used interchangeably in most situations, without regard to
their dimension. For example, arrays of `text` are all of type `text[]` and 1D,
2D, and 3D `text[]` can all be used in the same columns.

#### Nested structures

**Lists** allow each element of a layer to be of a different length. For
example, in a two-layer list, each of the first layer’s lists can be of a
different length:

```mzsql
SELECT LIST[[1,2], [3]] AS ragged_list;
```
```
ragged_list
-------------
{{1,2},{3}}
```

This is known as a "ragged list."

**Arrays** require each element of a dimension to have the same length. For
example, if the first element in a 2D list has a length of 2, all subsequent
members must also have a length of 2.

```mzsql
SELECT ARRAY[[1,2], [3]] AS ragged_array;
```
```
ERROR:  number of array elements (3) does not match declared cardinality (4)
```
#### Accessing single elements

**Lists** support accessing single elements via [indexing](#indexing-elements).
When indexed, lists return a value with one less layer than the indexed list.
For example, indexing a two-layer list returns a one-layer list.

```mzsql
SELECT LIST[['foo'],['bar']][1] AS indexing;
```
```
 indexing
--------------
 {foo}
```

Attempting to index twice into a `text list` (i.e. a one-layer list), fails
because you cannot index `text`.

```mzsql
SELECT LIST['foo'][1][2];
```
```
ERROR:  cannot subscript type text
```

##### Accessing ranges of elements

**Lists** support accessing ranges of elements via [slicing](#slicing-ranges).
However, lists do not currently support PostgreSQL-style slicing, which
descends into layers in each slice.

**Arrays** require each element of a dimension to have the same length. For
example, if the first element in a 2D list has a length of 2, all subsequent
members must also have a length of 2.

### Custom types

You can create [custom `list` types](/sql/types/#custom-types), which lets you
create a named entry in the catalog for a specific type of list.

Currently, custom types only provides a shorthand for referring to
otherwise-annoying-to-type names.

Note that custom `list` types have special rules regarding [polymorphism](/sql/types/#polymorphism).

### Valid casts

#### Between `list`s

You can cast one list type to another if the type of the source list’s elements
can be cast to the target list’s elements’ type. For example, `float list` can
be cast to `int list`, but `float list` cannot be cast to `timestamp list`.

Note that this rule also applies to casting between custom list types.

#### From `list`

You can [cast](../../functions/cast) `list` to:

- [`text`](../text) (implicitly)
- Other `lists` as noted above.

#### To `list`

You can [cast](../../functions/cast) the following types to `list`:

- [arrays](../array) (explicitly). See [details](../array#array-to-list-casts).
- [`text`](../text) (explicitly). See [details](#text-to-list-casts).
- Other `lists` as noted above.

## Examples

### Literals

```mzsql
SELECT LIST[[1.5, NULL],[2.25]];
```
```nofmt
         list
----------------------
 {{1.50,NULL},{2.25}}
```

### Casting between lists

```mzsql
SELECT LIST[[1.5, NULL],[2.25]]::int list list;
```
```nofmt
      list
----------------
 {{2,NULL},{2}}
```

### Casting to text

```mzsql
SELECT LIST[[1.5, NULL],[2.25]]::text;
```
```nofmt
      list
------------------
 {{1,NULL},{2}}
```

Despite the fact that the output looks the same as the above examples, it is, in
fact, `text`.

```mzsql
SELECT length(LIST[[1.5, NULL],[2.25]]::text);
```
```nofmt
 length
--------
     20
```

### Casting from text

```mzsql
SELECT '{{1.5,NULL},{2.25}}'::numeric(38,2) list list AS text_to_list;
```
```nofmt
     text_to_list
----------------------
 {{1.50,NULL},{2.25}}
```

### List containment

{{< note >}}
Like [array containment operators in PostgreSQL](https://www.postgresql.org/docs/current/functions-array.html#FUNCTIONS-ARRAY),
list containment operators in Materialize **do not** account for duplicates.
{{< /note >}}

{{< warn-if-unreleased "v0.107" >}}

```mzsql
SELECT LIST[1,4,3] @> LIST[3,1] AS contains;
```
```nofmt
 contains
----------
 t
```

```mzsql
SELECT LIST[2,7] <@ LIST[1,7,4,2,6] AS is_contained_by;
```
```nofmt
 is_contained_by
-----------------
 t
```


```mzsql
SELECT LIST[7,3,1] @> LIST[1,3,3,3,3,7] AS contains;
```
```nofmt
 contains
----------
 t
```

```mzsql
SELECT LIST[1,3,7,NULL] @> LIST[1,3,7,NULL] AS contains;
```
```nofmt
 contains
----------
 f
```




---

## map type


`map` data expresses an unordered map with [`text`](../text) keys and an
arbitrary uniform value type.

Detail | Info
-------|------
**Quick Syntax** | `'{a=>123.4, b=>111.1}'::map[text=>double]'`
**Size** | Variable
**Catalog name** | Anonymous, but [nameable](../../create-type)

## Syntax

{{< diagram "type-map.svg" >}}

Field | Use
------|-----
_map&lowbar;string_ | A well-formed map object.
_value&lowbar;type_ | The [type](../../types) of the map's values.

## Map functions + operators

### Operators

{{% map-operators %}}

### Functions

{{< fnlist "Map" >}}

## Details

### Construction

You can construct maps using the `MAP` expression:

```mzsql
SELECT MAP['a' => 1, 'b' => 2];
```
```nofmt
     map
-------------
 {a=>1,b=>2}
```

You can nest `MAP` constructors:

```mzsql
SELECT MAP['a' => MAP['b' => 'c']];
```
```nofmt
     map
-------------
 {a=>{b=>c}}
```

You can also elide the `MAP` keyword from the interior map expressions:

```mzsql
SELECT MAP['a' => ['b' => 'c']];
```
```nofmt
     map
-------------
 {a=>{b=>c}}
```

`MAP` expressions evalute expressions for both keys and values:

```mzsql
SELECT MAP['a' || 'b' => 1 + 2];
```
```nofmt
     map
-------------
 {ab=>3}
```

Alternatively, you can construct a map from the results of a subquery. The
subquery must return two columns: a key column of type `text` and a value column
of any type, in that order. Note that, in this form of the `MAP` expression,
parentheses are used rather than square brackets.

```mzsql
SELECT MAP(SELECT key, value FROM test0 ORDER BY x DESC LIMIT 3);
```
```nofmt
       map
------------------
 {a=>1,b=>2,c=>3}
```

With all constructors, if the same key appears multiple times, the last value
for the key wins.

Note that you can also construct maps using the available [`text`
cast](#text-to-map-casts).

### Constraints

- Keys must be of type [`text`](../text).
- Values can be of any [type](../../types) as long as the type is uniform.
- Keys must be unique. If duplicate keys are present in a map, only one of the
  (`key`, `value`) pairs will be retained. There is no guarantee which will be
  retained.

### Custom types

You can create [custom `map` types](/sql/types/#custom-types), which lets you
create a named entry in the catalog for a specific type of `map`.

Currently, custom types only provides a shorthand for referring to
otherwise-annoying-to-type names.

### `text` to `map` casts

The textual format for a `map` is a sequence of `key => value` mappings
separated by commas and surrounded by curly braces (`{}`). For example:

```mzsql
SELECT '{a=>123.4, b=>111.1}'::map[text=>double] as m;
```
```nofmt
  m
------------------
 {a=>123.4,b=>111.1}
```

You can create nested maps the same way:
```mzsql
SELECT '{a=>{b=>{c=>d}}}'::map[text=>map[text=>map[text=>text]]] as nested_map;
```
```nofmt
  nested_map
------------------
 {a=>{b=>{c=>d}}}
```

### Valid casts

#### Between `map`s

Two `map` types can only be cast to and from one another if they are
structurally equivalent, e.g. one is a [custom map
type](/sql/types#custom-types) and the other is a [built-in
map](/sql/types#built-in-types) and their key-value types are structurally
equivalent.

#### From `map`

You can [cast](../../functions/cast) `map` to and from the following types:

- [`text`](../text) (by assignment)
- Other `map`s as noted above.

#### To `map`

- [`text`](../text) (explicitly)
- Other `map`s as noted above.

## Examples

### Operators

#### Retrieve value with key (`->`)

Retrieves and returns the target value or `NULL`.

```mzsql
SELECT MAP['a' => 1, 'b' => 2] -> 'a' as field_map;
```
```nofmt
 field_map
-----------
 1
```

```mzsql
SELECT MAP['a' => 1, 'b' => 2] -> 'c' as field_map;
```
```nofmt
 field_map
----------
 NULL
```

Field accessors can also be chained together.

```mzsql
SELECT MAP['a' => ['b' => 1], 'c' => ['d' => 2]] -> 'a' -> 'b' as field_map;
```
```nofmt
 field_map
-------------
 1
```

Note that all returned values are of the map's value type.

<hr/>

#### LHS contains RHS (`@>`)

```mzsql
SELECT MAP['a' => 1, 'b' => 2] @> MAP['a' => 1] AS lhs_contains_rhs;
```
```nofmt
 lhs_contains_rhs
------------------
 t
```

<hr/>

#### RHS contains LHS (`<@`)

```mzsql
SELECT MAP['a' => 1, 'b' => 2] <@ MAP['a' => 1] as rhs_contains_lhs;
```
```nofmt
 rhs_contains_lhs
------------------
 f
```

<hr/>

#### Search top-level keys (`?`)

```mzsql
SELECT MAP['a' => 1.9, 'b' => 2.0] ? 'a' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 t
```

```mzsql
SELECT MAP['a' => ['aa' => 1.9], 'b' => ['bb' => 2.0]] ? 'aa' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 f
```

#### Search for all top-level keys (`?&`)

Returns `true` if all keys provided on the RHS are present in the top-level of
the map, `false` otherwise.

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?& ARRAY['b', 'a'] as search_for_all_keys;
```
```nofmt
 search_for_all_keys
---------------------
 t
```

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?& ARRAY['c', 'b'] as search_for_all_keys;
```
```nofmt
 search_for_all_keys
---------------------
 f
```

#### Search for any top-level keys (`?|`)

Returns `true` if any keys provided on the RHS are present in the top-level of
the map, `false` otherwise.

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?| ARRAY['c', 'b'] as search_for_any_keys;
```
```nofmt
 search_for_any_keys
---------------------
 t
```

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?| ARRAY['c', 'd', '1'] as search_for_any_keys;
```
```nofmt
 search_for_any_keys
---------------------
 f
```

#### Count entries in map (`map_length`)

Returns the number of entries in the map.

```mzsql
SELECT map_length(MAP['a' => 1, 'b' => 2]);
```
```nofmt
 map_length
------------
 2
```




---

## mz_aclitem type


`mz_aclitem` data expresses a granted privilege on some object.

## `mz_aclitem` info

Detail | Info
-------|------
**Size** | 26 bytes
**Catalog name** | `mz_catalog.mz_aclitem`
**OID** | 16566

## Details

`mz_aclitem` represents a privilege granted to some user on some object. The format of `mz_aclitem`
is `<grantee>=<privileges>/<grantor>`.
- `<grantee>` is the role ID of the role that has some privilege.
- `<privileges>` is the abbreviation of the privileges that `grantee` has concatenated together.
- `<grantor>` is the role ID of the role that granted the privileges.

A list of all privileges and their abbreviations are below:

| Privilege             | Description                                                                                    | Abbreviation        | Applicable Object Types                       |
|-----------------------|------------------------------------------------------------------------------------------------|---------------------|-----------------------------------------------|
| `SELECT`              | Allows reading rows from an object.                                                            | r(”read”)           | Table, View, Materialized View, Source        |
| `INSERT`              | Allows inserting into an object.                                                               | a(”append”)         | Table                                         |
| `UPDATE`              | Allows updating an object (requires SELECT if a read is necessary).                            | w(”write”)          | Table                                         |
| `DELETE`              | Allows deleting from an object (requires SELECT if a read is necessary).                       | d                   | Table                                         |
| `CREATE`              | Allows creating a new object within another object.                                            | C                   | Database, Schema, Cluster                     |
| `USAGE`               | Allows using an object or looking up members of an object.                                     | U                   | Database, Schema, Connection, Secret, Cluster |
| `CREATEROLE`          | Allows creating, altering, deleting roles and the ability to grant and revoke role membership. | R("Role")           | System                                        |
| `CREATEDB`            | Allows creating databases.                                                                     | B("dataBase")       | System                                        |
| `CREATECLUSTER`       | Allows creating clusters.                                                                      | N("compute Node")   | System                                        |
| `CREATENETWORKPOLICY` | Allows creating network policies.                                                              | P("network Policy") | System                                        |

The `CREATEROLE` privilege is very powerful. It allows roles to grant and revoke membership in
other roles, even if it doesn't have explicit membership in those roles. As a consequence, any role
with this privilege can obtain the privileges of any other role in the system.

If a `mz_aclitem` is casted to `text`, the role IDs are automatically converted to role names.

### Valid casts

For details about casting, including contexts, see [Functions:
Cast](../../functions/cast).

From | To | Required context
-----|----|--------
`mz_aclitem` | `text` | Explicit

### Valid operations

There are no supported operations or functions on `mz_aclitem` types.




---

## mz_timestamp type


`mz_timestamp` data expresses an internal timestamp.

## `mz_timestamp` info

Detail | Info
-------|------
**Size** | 8 bytes
**Catalog name** | `mz_catalog.mz_timestamp`
**OID** | 16552
**Min value** | 0
**Max value** | 18446744073709551615

## Details

- This type is produced by `mz_now()`.
- In general this is an opaque type, designed to ease the use of `mz_now()` by making various timestamp types castable to it.

### Valid casts

For details about casting, including contexts, see [Functions:
Cast](../../functions/cast).

Integer and numeric casts must be in the form of milliseconds since the Unix epoch. Casting from `text` can be either also in the form of milliseconds since the Unix epoch or in a human-readable form that is the same as for the [`timestamptz`](../timestamptz) type.

From | To | Required context
-----|----|--------
`mz_timestamp` | `text` | Assignment
`mz_timestamp` | `timestamp` | Assignment
`mz_timestamp` | `timestamptz` | Assignment
`text` | `mz_timestamp` | Assignment
`uint4` | `mz_timestamp` | Implicit
`uint8` | `mz_timestamp` | Implicit
`int4` | `mz_timestamp` | Implicit
`int8` | `mz_timestamp` | Implicit
`numeric` | `mz_timestamp` | Implicit
`timestamp` | `mz_timestamp` | Implicit
`timestamptz` | `mz_timestamp` | Implicit
`date` | `mz_timestamp` | Implicit

### Valid operations

There are no supported operations or functions on `mz_timestamp` types.




---

## numeric type


`numeric` data expresses an exact number with user-defined precision and scale.

Detail | Info
-------|------
**Size** | ~32 bytes
**Aliases** | `dec`, `decimal`
**Catalog name** | `pg_catalog.numeric`
**OID** | 1700
**Precision** | 39
**Scale** | [0, 39]

## Syntax

### Numeric values

{{< diagram "type-numeric-val.svg" >}}

Field | Use
------|-----------
**E**_exp_ | Multiply the number preceding **E** by 10<sup>exp</sup>

### Numeric definitions

{{< diagram "type-numeric-dec.svg" >}}

Field | Use
------|-----------
_precision_ | **Ignored**: All `numeric` values in Materialize have a precision of 39.
_scale_ | The total number of fractional decimal digits to track, e.g. `.321` has a scale of 3. _scale_ cannot exceed the maximum precision.

## Details

### Input

Materialize assumes untyped numeric literals are `numeric` if they:
- Contain decimal points or e-notation.
- Exceed `bigint`'s maximum or minimum values.

Materialize does not accept any numeric literals that exceed 39 digits of precision.

### Output

Materialize trims all trailing zeroes off of `numeric` values, irrespective of
their specified scale. This behavior lets us perform byte-level equality when
comparing rows of data, where we would otherwise need to decode rows' columns'
values for comparisons.

### Precision

All `numeric` values have a precision of 39, which cannot be changed. For ease
of use, Materialize accepts input of any value <= 39, but ignores the specified
value.

Note that the leading zeroes in values between -1 and 1 (i.e. `(-1, 1)`) are not
counted digits of precision.

For details on exceeding the `numeric` type's maximum precision, see
[Rounding](#rounding).

### Scale

By default, `numeric` values do not have a specified scale, so values can have
anywhere between 0 and 39 digits after the decimal point. For example:

```mzsql
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

However, if you specify a scale on a `numeric` value, values will be rescaled
appropriately. If the resulting value exceeds the maximum precision for
`numeric` types, you'll receive an error.

```mzsql
CREATE TABLE scaled (c NUMERIC(39, 20));

INSERT INTO scaled VALUES
  (987654321098765432109876543210987654321);
```
```
ERROR:  numeric field overflow
```
```mzsql
INSERT INTO scaled VALUES
  (9876543210987654321.09876543210987654321),
  (.987654321098765432109876543210987654321);

SELECT c FROM scaled;

                    c
------------------------------------------
                   0.98765432109876543211
 9876543210987654321.09876543210987654321
```

### Rounding

`numeric` operations will always round off fractional values to limit their
values to 39 digits of precision.

```mzsql
SELECT 2 * 9876543210987654321.09876543210987654321 AS rounded;

                 rounded
------------------------------------------
 19753086421975308642.1975308642197530864
```

However, if a value exceeds is >= 1E39 or <= -1E39, it generates an
overflow, i.e. it will not be rounded.

### Overflow

Operations generating values >= 1E39 or <= -1E39 are considered overflown.

### Underflow

Operations generating values within the range `(-1E-40, 1E-40)` are considered
underflown.

### Aggregations (`sum`)

**tl;dr** If you use `sum` on `numeric` values, retrieving values of `Infinity`
or `-Infinity` from the operation signals you have overflown the `numeric` type.
At this point, you should start retracting values or risk causing Materialize to
panic.

Materialize's dataflow engine (Differential) requires operations to retain their
commutativity and associativity––a simple way to think about this invariant is
that if you add a series of values, and then subtract the same values in some
random order, you must deterministically reach zero.

While other types in Materialize achieve this by simply allowing overflows (i.e.
going from the maximum 64-bit signed integer to the minimum 64-bit signed
integer), the `numeric` type does not support an equivalent operation.

To provide the invariants that Differential requires, `numeric` values are
instead aggregated into an even larger type behind the scenes––one that supports
twice as many digits of precision. This way, if your aggregation exceeds the
`numeric` type's bounds, we can continue to track the magnitude of excess.

To signal that the `numeric` value is in this state, the operation begins
returning `Infinity` or `-Infinity`. At this point, you must either subtract or
retract values to return representable `numeric` values.

However, because the underlying aggregated type is not of infinite precision, it
too can overflow. In this case, Materialize will instead panic and crash. To
avoid crashing, always immediately retract or subtract values when encountering
infinite results from `sum`.

### Valid casts

#### From `numeric`

You can [cast](../../functions/cast) `numeric` to:

- [`int`/`bigint`](../int) (by assignment)
- [`real`/`double precision`](../float) (implicitly)
- [`text`](../text) (by assignment)

#### To `numeric`

You can [cast](../../functions/cast) from the following types to `numeric`:

- [`int`/`bigint`](../int) (implicitly)
- [`real`/`double precision`](../float) (by assignment)
- [`text`](../text) (explicitly)

## Examples

```mzsql
SELECT 1.23::numeric AS num_v;
```
```nofmt
 num_v
-------
  1.23
```
<hr/>

```mzsql
SELECT 1.23::numeric(38,3) AS num_38_3_v;
```
```nofmt
 num_38_3_v
------------
      1.230
```

<hr/>

```mzsql
SELECT 1.23e4 AS num_w_exp;
```
```nofmt
 num_w_exp
-----------
     12300
```




---

## oid type


`oid` expresses a PostgreSQL-compatible object identifier.

Detail | Info
-------|------
**Size** | 4 bytes
**Catalog name** | `pg_catalog.oid`
**OID** | 26

## Details

`oid` types in Materialize are provided for compatibility with PostgreSQL. You
typically will not interact with the `oid` type unless you are working with a
tool that was developed for PostgreSQL.

See the [Object Identifier Types][pg-oid] section of the PostgreSQL
documentation for more details.

### Valid casts

#### From `oid`

You can [cast](../../functions/cast) `oid` to:

- [`integer`](../integer) (by assignment)
- [`bigint`](../integer) (by assignment)
- [`text`](../text) (explicitly)

#### To `oid`

You can [cast](../../functions/cast) from the following types to `oid`:

- [`integer`](../integer) (implicitly)
- [`bigint`](../integer) (implicitly)
- [`text`](../text) (explicitly)

[pg-oid]: https://www.postgresql.org/docs/current/datatype-oid.html




---

## record type


A `record` is a tuple that can contain an arbitrary number of elements of any
type.

Detail | Info
-------|------
**Quick Syntax** | `ROW($expr, ...)`
**Size** | Variable
**Catalog name** | Unnameable

## Syntax

{{< diagram "type-record.svg" >}}

## Details

Record types can be used to represent nested data.

The fields of a record are named `f1`, `f2`, and so on. To access a
field of a record, use the `.` operator. Note that you need to parenthesize the
record expression to ensure that the `.` is interpreted as the field selection
operator, rather than part of a database- or schema-qualified table name.

### Catalog name

`record` is a named type in PostgreSQL (`pg_catalog.record`), but is
currently an [unnameable](../#catalog-name) type in Materialize.

### Valid casts

You can [cast](../../functions/cast) `record` to [`text`](../../types/text/) by assignment.

You cannot cast from any other types to `record`.

## Examples

```mzsql
SELECT ROW(1, 2) AS record;
```
```nofmt
 record
--------
 (1,2)
```

<hr>

```mzsql
SELECT record, (record).f2 FROM (SELECT ROW(1, 2) AS record);
```
```nofmt
record | f2
--------+----
 (1,2)  |  2
```

<hr>

Forgetting to parenthesize the record expression in a field selection operation
will result in errors like the following

```mzsql
SELECT record.f2 FROM (SELECT ROW(1, 2) AS record);
```
```nofmt
ERROR:  column "record.f2" does not exist
```

as the expression `record.f2` specifies a column named `f2` from a table named
`record`, rather than the field `f2` from the record-typed column named
`record`.




---

## text type


`text` data expresses a Unicode string. This is equivalent to `string` or
`varchar` in other RDBMSes.

Detail | Info
-------|------
**Quick Syntax** | `'foo'`
**Aliases** | `string`
**Size** | Variable
**Catalog name** | `pg_catalog.text`
**OID** | 25

## Syntax

### Standard

{{< diagram "type-text.svg" >}}

To escape a single quote character (`'`) in a standard string literal, write two
adjacent single quotes:

```mzsql
SELECT 'single''quote' AS output
```
```nofmt
   output
------------
single'quote
```

All other characters are taken literally.

### Escape

A string literal that is preceded by an `e` or `E` is an "escape" string
literal:

{{< diagram "type-escape-text.svg" >}}

Escape string literals follow the same rules as standard string literals, except
that backslash character (`\`) starts an escape sequence. The following escape
sequences are recognized:

Escape sequence | Meaning
----------------|--------
`\b`  | Backspace
`\f`  | Form feed
`\n`  | Newline
`\r`  | Carriage return
`\t`  | Tab
`\uXXXX`, `\UXXXXXXXX`  | Unicode codepoint, where `X` is a hexadecimal digit

Any other character following a backslash is taken literally, so `\\` specifies
a literal backslash, and `\'` is an alternate means of escaping the single quote
character.

Unlike in PostgreSQL, there are no escapes that produce arbitrary byte values,
in order to ensure that escape string literals are always valid UTF-8.

## Details

### Valid casts

#### From `text`

You can [cast](../../functions/cast) `text` to [all types](../) except [`record`](../../types/record/). All casts are explicit. Casts from text
will error if the string is not valid input for the destination type.

#### To `text`

You can [cast](../../functions/cast) [all types](../) to `text`. All casts are by assignment.

## Examples

```mzsql
SELECT 'hello' AS text_val;
```
```nofmt
 text_val
---------
 hello
```

<hr>

```mzsql
SELECT E'behold\nescape strings\U0001F632' AS escape_val;
```
```nofmt
   escape_val
-----------------
 behold         +
 escape strings😲
```




---

## time type


`time` data expresses a time without a specific date.

Detail | Info
-------|------
**Quick Syntax** | `TIME '01:23:45'`
**Size** | 4 bytes
**Catalog name** | `pg_catalog.time`
**OID** | 1083
**Min value** | `TIME '00:00:00'`
**Max value** | `TIME '23:59:59.999999'`

## Syntax

{{< diagram "type-time.svg" >}}

Field | Use
------|------------
_time&lowbar;str_ | A string representing a time of day in `H:M:S.NS` format.

## Details

### Valid casts

#### From `time`

You can [cast](../../functions/cast) `time` to:

- [`interval`](../interval) (implicitly)
- [`text`](../text) (by assignment)

#### To `time`

You can [cast](../../functions/cast) from the following types to `time`:

- [`interval`](../interval) (by assignment)
- [`text`](../text) (explicitly)
- [`timestamp`](../timestamp) (by assignment)
- [`timestamptz`](../timestamp) (by assignment)

### Valid operations

`time` data supports the following operations with other types.

Operation | Computes
----------|------------
[`date`](../date) `+` [`time`](../time) | [`timestamp`](../timestamp)
[`time`](../time) `+` [`interval`](../interval) | `time`
[`time`](../time) `-` [`interval`](../interval) | `time`
[`time`](../time) `-` [`time`](../time) | [`interval`](../interval)

## Examples

```mzsql
SELECT TIME '01:23:45' AS t_v;
```
```nofmt
   t_v
----------
 01:23:45
```

<hr/>

```mzsql
SELECT DATE '2001-02-03' + TIME '12:34:56' AS d_t;
```
```nofmt
         d_t
---------------------
 2001-02-03 12:34:56
```




---

## Timestamp types


`timestamp` and `timestamp with time zone` data expresses a date and time in
UTC.

## `timestamp` info

Detail | Info
-------|------
**Quick Syntax** | `TIMESTAMP WITH TIME ZONE '2007-02-01 15:04:05+06'`
**Size** | 8 bytes
**Catalog name** | `pg_catalog.timestamp`
**OID** | 1083
**Min value** | 4713 BC
**Max value** | 294276 AD
**Max resolution** | 1 microsecond

## `timestamp with time zone` info

Detail | Info
-------|------
**Quick Syntax** | `TIMESTAMPTZ '2007-02-01 15:04:05+06'`
**Aliases** | `timestamp with time zone`
**Size** | 8 bytes
**Catalog name** | `pg_catalog.timestamptz`
**OID** | 1184
**Min value** | 4713 BC
**Max value** | 294276 AD
**Max resolution** | 1 microsecond

## Syntax

{{< diagram "type-timestamp.svg" >}}

Field | Use
------|-----
**WITH TIME ZONE** | Apply the _tz&lowbar;offset_ field.
**WITHOUT TIME ZONE** | Ignore the _tz&lowbar;offset_ field.<br>This is the default if neither `WITH TIME ZONE` nor `WITHOUT TIME ZONE` is specified.
**TIMESTAMPTZ** | A shorter alias for `TIMESTAMP WITH TIME ZONE`.
_precision_ | The number of digits of precision to use to represent fractional seconds. If unspecified, timestamps use six digits of precision—i.e., they have a resolution of one microsecond.
_date&lowbar;str_ | A string representing a date in `Y-M-D`, `Y M-D`, `Y M D` or `YMD` format.
_time&lowbar;str_ | A string representing a time of day in `H:M:S.NS` format.
_tz&lowbar;offset_ | The timezone's distance, in hours, from UTC.

## Details

- `timestamp` and `timestamp with time zone` store data in
  [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).
- The difference between the two types is that `timestamp with time zone` can read or write
  timestamps with the offset specified by the timezone. Importantly,
  `timestamp with time zone` itself doesn't store any timezone data; Materialize simply
  performs the conversion from the time provided and UTC.
- Materialize assumes all clients expect UTC time, and does not currently
  support any other timezones.

### Valid casts

In addition to the casts listed below, `timestamp` and `timestamptz` can be cast to and from each other implicitly.

#### From `timestamp` or `timestamptz`

You can [cast](../../functions/cast) `timestamp` or `timestamptz` to:

- [`date`](../date) (by assignment)
- [`text`](../text) (by assignment)
- [`time`](../time) (by assignment)

#### To `timestamp` or `timestamptz`

You can [cast](../../functions/cast) the following types to `timestamp` or `timestamptz`:

- [`date`](../date) (implicitly)
- [`text`](../text) (explicitly)

### Valid operations

`timestamp` and `timestamp with time zone` data (collectively referred to as
`timestamp/tz`) supports the following operations with other types.

Operation | Computes
----------|------------
[`date`](../date) `+` [`interval`](../interval) | [`timestamp/tz`](../timestamp)
[`date`](../date) `-` [`interval`](../interval) | [`timestamp/tz`](../timestamp)
[`date`](../date) `+` [`time`](../time) | [`timestamp/tz`](../timestamp)
[`timestamp/tz`](../timestamp) `+` [`interval`](../interval) | [`timestamp/tz`](../timestamp)
[`timestamp/tz`](../timestamp) `-` [`interval`](../interval) | [`timestamp/tz`](../timestamp)
[`timestamp/tz`](../timestamp) `-` [`timestamp/tz`](../timestamp) | [`interval`](../interval)

## Examples

### Return timestamp

```mzsql
SELECT TIMESTAMP '2007-02-01 15:04:05' AS ts_v;
```
```nofmt
        ts_v
---------------------
 2007-02-01 15:04:05
```

### Return timestamp with time zone

```mzsql
SELECT TIMESTAMPTZ '2007-02-01 15:04:05+06' AS tstz_v;
```
```nofmt
         tstz_v
-------------------------
 2007-02-01 09:04:05 UTC
```

## Related topics
* [`TIMEZONE` and `AT TIME ZONE` functions](../../functions/timezone-and-at-time-zone)




---

## Unsigned Integer types


## `uint2` info

Detail | Info
-------|------
**Size** | 2 bytes
**Catalog name** | `mz_catalog.uint2`
**OID** | 16,460
**Range** | [0, 65,535]

## `uint4` info

Detail | Info
-------|------
**Size** | 4 bytes
**Catalog name** | `mz_catalog.uint4`
**OID** | 16,462
**Range** | [0, 4,294,967,295]

## `uint8` info

Detail | Info
-------|------
**Size** | 8 bytes
**Catalog name** | `mz_catalog.uint8`
**OID** | 14,464
**Range** | [0, 18,446,744,073,709,551,615]

## Details

### Valid casts

For details about casting, including contexts, see [Functions:
Cast](../../functions/cast).

#### Between unsigned integer types

From    | To      | Required context
--------|---------|--------
`uint2` | `uint4` | Implicit
`uint2` | `uint8` | Implicit
`uint4` | `uint2` | Assignment
`uint4` | `uint8` | Implicit
`uint8` | `uint2` | Assignment
`uint8` | `uint4` | Assignment

#### From unsigned integer types

You can cast unsigned integer types to:

To | Required context
---|--------
[`numeric`](../numeric) | Implicit
[`real`/`double precision`](../float) | Implicit
[`text`](../text) | Assignment
[`smallint`/`integer`/`bigint`](../integer) | Depends on specific cast

#### To `uint4` or `uint8`

You can cast the following types to unsigned integer types:

From | Required context
---|--------
[`boolean`](../boolean) (`integer` only) | Explicit
[`jsonb`](../jsonb) | Explicit
[`oid`](../oid) (`integer` and `bigint` only) | Assignment
[`numeric`](../numeric) | Assignment
[`real`/`double precision`](../float) | Assignment
[`text`](../text) | Explicit
[`smallint`/`integer`/`bigint`](../integer) | Depends on specific cast

## Examples

```mzsql
SELECT 123::uint4 AS int_v;
```
```nofmt
 int_v
-------
   123
```

<hr/>

```mzsql
SELECT 1.23::uint4 AS int_v;
```
```nofmt
 int_v
-------
     1
```




---

## uuid type


`uuid` data expresses a universally-unique identifier (UUID).

Detail | Info
-------|------
**Quick Syntax** | `UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`
**Size** | 16 bytes
**Catalog name** | `pg_catalog.uuid`
**OID** | 2950

The `uuid` type is more space efficient than representing UUIDs as
[`text`](../text). A UUID stored as `text` requires either 32 or 36 bytes,
depending on the presence of hyphens, while the `uuid` type requires only 16
bytes.

## Syntax

{{< diagram "type-uuid.svg" >}}

The standard form of a UUID consists of five groups of lowercase hexadecimal
digits separated by hyphens, where the first group contains 8 digits, the next
three groups contain 4 digits each, and the last group contains 12 digits:

```
a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
```

Materialize also accepts UUID input where the hyphens are omitted, or where some
or all of the hexadecimal digits are uppercase:

```
a0eebc999c0b4ef8bb6d6bb9bd380a11
A0eeBc99-9c0b-4ef8-bB6d-6bb9bd380A11
A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11
```

Materialize will always output UUIDs in the standard form.

## Details

### Valid casts

You can [cast](../../functions/cast) `uuid` to [`text`](../text) by assignment and from [`text`](../text) explicitly.

## Examples

```mzsql
SELECT UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS uuid
```
```nofmt
                 uuid
--------------------------------------
 a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
```



