<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [SQL data
types](/docs/sql/types/)

</div>

# jsonb type

`jsonb` data expresses a JSON object similar to [PostgreSQL’s
implementation](https://www.postgresql.org/docs/current/datatype-json.html).

| Detail           | Info                     |
|------------------|--------------------------|
| **Quick Syntax** | `'{"1":2,"3":4}'::jsonb` |
| **Size**         | Variable                 |
| **Catalog name** | `pg_catalog.jsonb`       |
| **OID**          | 3802                     |

Materialize does not yet support a type more similar to PostgreSQL’s
implementation of `json`.

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIzMjkiIGhlaWdodD0iMzciPgogICA8cG9seWdvbiBwb2ludHM9IjkgMTcgMSAxMyAxIDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iMTcgMTcgOSAxMyA5IDIxIj48L3BvbHlnb24+CiAgIDxyZWN0IHg9IjMxIiB5PSIzIiB3aWR0aD0iMjIiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjkiIHk9IjEiIHdpZHRoPSIyMiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzkiIHk9IjIxIj4mIzM5OzwvdGV4dD4KICAgPHJlY3QgeD0iNzMiIHk9IjMiIHdpZHRoPSI5MCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iNzEiIHk9IjEiIHdpZHRoPSI5MCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjgxIiB5PSIyMSI+anNvbl9zdHJpbmc8L3RleHQ+CiAgIDxyZWN0IHg9IjE4MyIgeT0iMyIgd2lkdGg9IjIyIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjE4MSIgeT0iMSIgd2lkdGg9IjIyIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxOTEiIHk9IjIxIj4mIzM5OzwvdGV4dD4KICAgPHJlY3QgeD0iMjI1IiB5PSIzIiB3aWR0aD0iNzYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjIzIiB5PSIxIiB3aWR0aD0iNzYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIzMyIgeT0iMjEiPjo6SlNPTkI8L3RleHQ+CiAgIDxwYXRoIGNsYXNzPSJsaW5lIiBkPSJtMTcgMTcgaDIgbTAgMCBoMTAgbTIyIDAgaDEwIG0wIDAgaDEwIG05MCAwIGgxMCBtMCAwIGgxMCBtMjIgMCBoMTAgbTAgMCBoMTAgbTc2IDAgaDEwIG0zIDAgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjMxOSAxNyAzMjcgMTMgMzI3IDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iMzE5IDE3IDMxMSAxMyAzMTEgMjEiPjwvcG9seWdvbj4KPC9zdmc+)

</div>

| Field | Use |
|----|----|
| *json_string* | A well-formed [JSON object](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON). |

## `jsonb` functions + operators

Materialize supports the following operators and functions.

### Operators

| Operator | RHS Type | Description |
|----|----|----|
| `->` | `text`, `int` | Access field by name or index position, and return `jsonb` ([docs](/docs/sql/types/jsonb/#field-access-as-jsonb--)) |
| `->>` | `text`, `int` | Access field by name or index position, and return `text` ([docs](/docs/sql/types/jsonb/#field-access-as-text--)) |
| `#>` | `text[]` | Access field by path, and return `jsonb` ([docs](/docs/sql/types/jsonb/#path-access-as-jsonb-)) |
| `#>>` | `text[]` | Access field by path, and return `text` ([docs](/docs/sql/types/jsonb/#path-access-as-text-)) |
| `||` | `jsonb` | Concatenate LHS and RHS ([docs](/docs/sql/types/jsonb/#jsonb-concat-)) |
| `-` | `text` | Delete all values with key of RHS ([docs](/docs/sql/types/jsonb/#remove-key--)) |
| `@>` | `jsonb` | Does element contain RHS? ([docs](/docs/sql/types/jsonb/#lhs-contains-rhs-)) |
| `<@` | `jsonb` | Does RHS contain element? ([docs](/docs/sql/types/jsonb/#rhs-contains-lhs-)) |
| `?` | `text` | Is RHS a top-level key? ([docs](/docs/sql/types/jsonb/#search-top-level-keys-)) |

### Functions

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="jsonb_agg" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_agg(expression) -&gt; jsonb</code></pre>
</div>
</div>
<p>Aggregate values (including nulls) as a jsonb array <a
href="/docs/sql/functions/jsonb_agg">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_array_elements" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_array_elements(j: jsonb) -&gt; Col&lt;jsonb&gt;</code></pre>
</div>
</div>
<p><code>j</code>’s elements if <code>j</code> is an array <a
href="/docs/sql/types/jsonb#jsonb_array_elements">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_array_elements_text" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_array_elements_text(j: jsonb) -&gt; Col&lt;string&gt;</code></pre>
</div>
</div>
<p><code>j</code>’s elements if <code>j</code> is an array <a
href="/docs/sql/types/jsonb#jsonb_array_elements_text">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_array_length" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_array_length(j: jsonb) -&gt; int</code></pre>
</div>
</div>
<p>Number of elements in <code>j</code>’s outermost array <a
href="/docs/sql/types/jsonb#jsonb_array_length">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_build_array" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_build_array(x: ...) -&gt; jsonb</code></pre>
</div>
</div>
<p>Output each element of <code>x</code> as a <code>jsonb</code> array.
Elements can be of heterogenous types <a
href="/docs/sql/types/jsonb#jsonb_build_array">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_build_object" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_build_object(x: ...) -&gt; jsonb</code></pre>
</div>
</div>
<p>The elements of x as a <code>jsonb</code> object. The argument list
alternates between keys and values <a
href="/docs/sql/types/jsonb#jsonb_build_object">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_each" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_each(j: jsonb) -&gt; Col&lt;(key: string, value: jsonb)&gt;</code></pre>
</div>
</div>
<p><code>j</code>’s outermost elements if <code>j</code> is an object <a
href="/docs/sql/types/jsonb#jsonb_each">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_each_text" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_each_text(j: jsonb) -&gt; Col&lt;(key: string, value: string)&gt;</code></pre>
</div>
</div>
<p><code>j</code>’s outermost elements if <code>j</code> is an object <a
href="/docs/sql/types/jsonb#jsonb_each_text">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_object_agg" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_object_agg(keys, values) -&gt; jsonb</code></pre>
</div>
</div>
<p>Aggregate keys and values (including nulls) as a <code>jsonb</code>
object <a
href="/docs/sql/functions/jsonb_object_agg">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_object_keys" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_object_keys(j: jsonb) -&gt; Col&lt;string&gt;</code></pre>
</div>
</div>
<p><code>j</code>’s outermost keys if <code>j</code> is an object <a
href="/docs/sql/types/jsonb#jsonb_object_keys">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_pretty" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_pretty(j: jsonb) -&gt; string</code></pre>
</div>
</div>
<p>Pretty printed (i.e. indented) <code>j</code> <a
href="/docs/sql/types/jsonb#jsonb_pretty">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_typeof" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_typeof(j: jsonb) -&gt; string</code></pre>
</div>
</div>
<p>Type of <code>j</code>’s outermost value. One of <code>object</code>,
<code>array</code>, <code>string</code>, <code>number</code>,
<code>boolean</code>, and <code>null</code> <a
href="/docs/sql/types/jsonb#jsonb_typeof">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_strip_nulls" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_strip_nulls(j: jsonb) -&gt; jsonb</code></pre>
</div>
</div>
<p><code>j</code> with all object fields with a value of
<code>null</code> removed. Other <code>null</code> values remain <a
href="/docs/sql/types/jsonb#jsonb_strip_nulls">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="to_jsonb" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>to_jsonb(v: T) -&gt; jsonb</code></pre>
</div>
</div>
<p><code>v</code> as <code>jsonb</code> <a
href="/docs/sql/types/jsonb#to_jsonb">(docs)</a>.</p></td>
</tr>
</tbody>
</table>

#### Detail

Functions that return `Col`s are considered table functions and can only
be used as tables, i.e. you cannot use them as scalar values. For
example, you can only use `jsonb_object_keys` in the following way:

<div class="highlight">

``` chroma
SELECT * FROM jsonb_object_keys('{"1":2,"3":4}'::jsonb);
```

</div>

## Details

- `jsonb` elements can be of the following types:
  - Objects
  - Arrays
  - String
  - Number
  - Boolean
  - Null
- Numbers in `jsonb` elements are all equivalent to
  [`numeric`](/docs/sql/types/numeric) in SQL.

### Valid casts

#### From `jsonb`

You can [cast](../../functions/cast) `jsonb` to:

- [`boolean`](../boolean) (explicitly)
- [`numeric`](../numeric) (explicitly)
- [`int`](../integer) (explicitly)
- [`real`/`double precision`](../float) (explicitly)
- [`text`](../text) (by assignment) (stringifies `jsonb`)

#### To `jsonb`

You can explicitly [cast](../../functions/cast) from [`text`](../text)
to `jsonb`.

#### Notes about converting `jsonb` to `text`

`jsonb` can have some odd-feeling corner cases when converting to or
from [`text`](/docs/sql/types/text).

- `jsonb::text` always produces the printed version of the JSON.

  <div class="highlight">

  ``` chroma
  SELECT ('"a"'::jsonb)::text AS jsonb_elem;
  ```

  </div>

  ```
   jsonb_elem
  ------------
   "a"
  ```

- `->>` and the `_text` functions produce the printed version of the
  inner element, unless the output is a single JSON string in which case
  they print it without quotes, i.e. as a SQL `text` value.

  <div class="highlight">

  ``` chroma
  SELECT ('"a"'::jsonb)->>0 AS string_elem;
  ```

  </div>

  ```
   jsonb_elem
  ------------
   a
  ```

- `text` values passed to `to_jsonb` with quotes (`"`) produced `jsonb`
  strings with the quotes escaped.

  <div class="highlight">

  ``` chroma
  SELECT to_jsonb('"foo"') AS escaped_quotes;
  ```

  </div>

  ```
   escaped_quotes
  ----------------
   "\"foo\""
  ```

### Subscripting

You can use subscript notation (`[]`) to extract an element from a
`jsonb` array or object.

The returned value is always of type `jsonb`. If the requested array
element or object key does not exist, or if either the input value or
subscript value is `NULL`, the subscript operation returns `NULL`.

#### Arrays

To extract an element from an array, supply the 0-indexed position as
the subscript:

<div class="highlight">

``` chroma
SELECT ('[1, 2, 3]'::jsonb)[1]
```

</div>

```
 jsonb
-------
 2
```

Negative indexes count backwards from the end of the array. [Slice
syntax](/docs/sql/types/list#slicing-ranges) is not supported. Note also
that 0-indexed positions are at variance with
[`list`](/docs/sql/types/list) and [`array`](/docs/sql/types/array)
types, whose subscripting operation uses 1-indexed positions.

#### Objects

To extract a value from an object, supply the key as the subscript:

<div class="highlight">

``` chroma
SELECT ('{"a": 1, "b": 2, "c": 3}'::jsonb)['b'];
```

</div>

```
 jsonb
-------
 2
```

You can chain subscript operations to retrieve deeply nested elements:

<div class="highlight">

``` chroma
SELECT ('{"1": 2, "a": ["b", "c"]}'::jsonb)['a'][1];
```

</div>

```
 jsonb
-------
 "c"
```

#### Remarks

Because the output type of the subscript operation is always `jsonb`,
when comparing the output of a subscript to a string, you must supply a
JSON string to compare against:

<div class="highlight">

``` chroma
SELECT ('["a", "b"]::jsonb)[1] = '"b"'
```

</div>

Note the extra double quotes on the right-hand side of the comparison.

### Parsing

Manually parsing JSON-formatted data in SQL can be tedious. 🫠 You can
use the widget below to **automatically** turn a sample JSON payload
into a parsing view with the individual fields mapped to columns.

<div class="json_widget">

<div class="json">

<div id="error_span" class="error">

</div>

</div>

<span class="input_container"> <span class="input_container-text">
</span> </span>

Target object type View Materialized view

``` sql_output
```

</div>

## Examples

### Operators

#### Field access as `jsonb` (`->`)

The type of JSON element you’re accessing dictates the RHS’s type.

- Use a `string` to return the value for a specific key:

  <div class="highlight">

  ``` chroma
  SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'1' AS field_jsonb;
  ```

  </div>

  ```
   field_jsonb
  -------------
   2
  ```

- Use an `int` to return the value in an array at a specific index:

  <div class="highlight">

  ``` chroma
  SELECT '["1", "a", 2]'::jsonb->1 AS field_jsonb;
  ```

  </div>

  ```
   field_jsonb
  -------------
   "a"
  ```

Field accessors can also be chained together.

<div class="highlight">

``` chroma
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'a'->1 AS field_jsonb;
```

</div>

```
 field_jsonb
-------------
 "c"
```

Note that all returned values are `jsonb`.

------------------------------------------------------------------------

#### Field access as `text` (`->>`)

The type of JSON element you’re accessing dictates the RHS’s type.

- Use a `string` to return the value for a specific key:

  <div class="highlight">

  ``` chroma
  SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->>'1' AS field_text;
  ```

  </div>

  ```
   field_text
  -------------
   2
  ```

- Use an `int` to return the value in an array at a specific index:

  <div class="highlight">

  ``` chroma
  SELECT '["1", "a", 2]'::jsonb->>1 AS field_text;
  ```

  </div>

  ```
   field_text
  -------------
   a
  ```

Field accessors can also be chained together, as long as the LHS remains
`jsonb`.

<div class="highlight">

``` chroma
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'a'->>1 AS field_text;
```

</div>

```
 field_text
-------------
 c
```

Note that all returned values are `string`.

#### Path access as `jsonb` (`#>`)

You can access specific elements in a `jsonb` value using a “path”,
which is a [text array](/docs/sql/types/array) where each element is
either a field key or an array element:

<div class="highlight">

``` chroma
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb #> '{a,1}' AS field_jsonb;
```

</div>

```
 field_jsonb
-------------
 "c"
```

The operator returns a value of type `jsonb`. If the path is invalid, it
returns `NULL`.

#### Path access as `text` (`#>>`)

The `#>>` operator is equivalent to the [`#>`](#path-access-as-jsonb-)
operator, except that the operator returns a value of type `text`.

<div class="highlight">

``` chroma
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb #>> '{a,1}' AS field_text;
```

</div>

```
 field_text
-------------
 c
```

------------------------------------------------------------------------

#### `jsonb` concat (`||`)

<div class="highlight">

``` chroma
SELECT '{"1": 2}'::jsonb ||
       '{"a": ["b", "c"]}'::jsonb AS concat;
```

</div>

```
             concat
---------------------------------
 {"1":2,"a":["b","c"]}
```

------------------------------------------------------------------------

#### Remove key (`-`)

<div class="highlight">

``` chroma
 SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb - 'a' AS rm_key;
```

</div>

```
  rm_key
-----------
 {"1":2}
```

------------------------------------------------------------------------

#### LHS contains RHS (`@>`)

Here, the left hand side does contain the right hand side, so the result
is `t` for true.

<div class="highlight">

``` chroma
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb @>
       '{"1": 2}'::jsonb AS lhs_contains_rhs;
```

</div>

```
 lhs_contains_rhs
------------------
 t
```

------------------------------------------------------------------------

#### RHS contains LHS (`<@`)

Here, the right hand side does contain the left hand side, so the result
is `t` for true.

<div class="highlight">

``` chroma
SELECT '{"1": 2}'::jsonb <@
       '{"1": 2, "a": ["b", "c"]}'::jsonb AS lhs_contains_rhs;
```

</div>

```
 rhs_contains_lhs
------------------
 t
```

------------------------------------------------------------------------

#### Search top-level keys (`?`)

<div class="highlight">

``` chroma
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb ? 'a' AS search_for_key;
```

</div>

```
 search_for_key
----------------
 t
```

<div class="highlight">

``` chroma
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb ? 'b' AS search_for_key;
```

</div>

```
 search_for_key
----------------
 f
```

### Functions

#### `jsonb_array_elements`

##### Expanding a JSON array

<div class="highlight">

``` chroma
SELECT * FROM jsonb_array_elements('[true, 1, "a", {"b": 2}, null]'::jsonb);
```

</div>

```
   value
-----------
 true
 1.0
 "a"
 {"b":2.0}
 null
```

##### Flattening a JSON array

<div class="highlight">

``` chroma
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

</div>

```
 id | a | b
----+---+---
  1 | 1 | 2
  1 | 3 | 4
  2 | 5 | 6
  2 | 7 | 8
```

------------------------------------------------------------------------

#### `jsonb_array_elements_text`

<div class="highlight">

``` chroma
SELECT * FROM jsonb_array_elements_text('[true, 1, "a", {"b": 2}, null]'::jsonb);
```

</div>

```
   value
-----------
 true
 1.0
 "a"
 {"b":2.0}
 null
```

------------------------------------------------------------------------

#### `jsonb_array_length`

<div class="highlight">

``` chroma
SELECT jsonb_array_length('[true, 1, "a", {"b": 2}, null]'::jsonb);
```

</div>

```
 jsonb_array_length
--------------------
                  5
```

------------------------------------------------------------------------

#### `jsonb_build_array`

<div class="highlight">

``` chroma
SELECT jsonb_build_array('a', 1::float, 2.0::float, true);
```

</div>

```
 jsonb_build_array
--------------------
 ["a",1.0,2.0,true]
```

------------------------------------------------------------------------

#### `jsonb_build_object`

<div class="highlight">

``` chroma
SELECT jsonb_build_object(2.0::float, 'b', 'a', 1.1::float);
```

</div>

```
 jsonb_build_object
--------------------
 {"2":"b","a":1.1}
```

------------------------------------------------------------------------

#### `jsonb_each`

<div class="highlight">

``` chroma
SELECT * FROM jsonb_each('{"1": 2.1, "a": ["b", "c"]}'::jsonb);
```

</div>

```
 key |   value
-----+-----------
 1   | 2.1
 a   | ["b","c"]
```

Note that the `value` column is `jsonb`.

------------------------------------------------------------------------

#### `jsonb_each_text`

<div class="highlight">

``` chroma
SELECT * FROM jsonb_each_text('{"1": 2.1, "a": ["b", "c"]}'::jsonb);
```

</div>

```
 key |   value
-----+-----------
 1   | 2.1
 a   | ["b","c"]
```

Note that the `value` column is `string`.

------------------------------------------------------------------------

#### `jsonb_object_keys`

<div class="highlight">

``` chroma
SELECT * FROM jsonb_object_keys('{"1": 2, "a": ["b", "c"]}'::jsonb);
```

</div>

```
 jsonb_object_keys
-------------------
 1
 a
```

------------------------------------------------------------------------

#### `jsonb_pretty`

<div class="highlight">

``` chroma
SELECT jsonb_pretty('{"1": 2, "a": ["b", "c"]}'::jsonb);
```

</div>

```
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

------------------------------------------------------------------------

#### `jsonb_typeof`

<div class="highlight">

``` chroma
SELECT jsonb_typeof('[true, 1, "a", {"b": 2}, null]'::jsonb);
```

</div>

```
 jsonb_typeof
--------------
 array
```

<div class="highlight">

``` chroma
SELECT * FROM jsonb_typeof('{"1": 2, "a": ["b", "c"]}'::jsonb);
```

</div>

```
 jsonb_typeof
--------------
 object
```

------------------------------------------------------------------------

#### `jsonb_strip_nulls`

<div class="highlight">

``` chroma
SELECT jsonb_strip_nulls('[{"1":"a","2":null},"b",null,"c"]'::jsonb);
```

</div>

```
    jsonb_strip_nulls
--------------------------
 [{"1":"a"},"b",null,"c"]
```

------------------------------------------------------------------------

#### `to_jsonb`

<div class="highlight">

``` chroma
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

</div>

```
      jsonified_row
--------------------------
 {"content":"hi","id":3}
 {"content":"hey","id":1}
```

Note that the output is `jsonb`.

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/types/jsonb.md"
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
