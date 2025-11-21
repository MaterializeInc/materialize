<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [SQL data
types](/docs/sql/types/)

</div>

# map type

`map` data expresses an unordered map with [`text`](../text) keys and an
arbitrary uniform value type.

| Detail           | Info                                         |
|------------------|----------------------------------------------|
| **Quick Syntax** | `'{a=>123.4, b=>111.1}'::map[text=>double]'` |
| **Size**         | Variable                                     |
| **Catalog name** | Anonymous, but [nameable](../../create-type) |

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MjkiIGhlaWdodD0iMTAzIj4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iMyIgd2lkdGg9IjIyIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI5IiB5PSIxIiB3aWR0aD0iMjIiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM5IiB5PSIyMSI+JiMzOTs8L3RleHQ+CiAgIDxyZWN0IHg9IjczIiB5PSIzIiB3aWR0aD0iOTIiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjcxIiB5PSIxIiB3aWR0aD0iOTIiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI4MSIgeT0iMjEiPm1hcF9zdHJpbmc8L3RleHQ+CiAgIDxyZWN0IHg9IjE4NSIgeT0iMyIgd2lkdGg9IjIyIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjE4MyIgeT0iMSIgd2lkdGg9IjIyIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxOTMiIHk9IjIxIj4mIzM5OzwvdGV4dD4KICAgPHJlY3QgeD0iMjI3IiB5PSIzIiB3aWR0aD0iMjgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjI1IiB5PSIxIiB3aWR0aD0iMjgiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIzNSIgeT0iMjEiPjo6PC90ZXh0PgogICA8cmVjdCB4PSIyNzUiIHk9IjMiIHdpZHRoPSI1MiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyNzMiIHk9IjEiIHdpZHRoPSI1MiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjgzIiB5PSIyMSI+TUFQPC90ZXh0PgogICA8cmVjdCB4PSIzNDciIHk9IjMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzNDUiIHk9IjEiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzU1IiB5PSIyMSI+WzwvdGV4dD4KICAgPHJlY3QgeD0iMzkzIiB5PSIzIiB3aWR0aD0iNTYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzkxIiB5PSIxIiB3aWR0aD0iNTYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjQwMSIgeT0iMjEiPlRFWFQ8L3RleHQ+CiAgIDxyZWN0IHg9IjQ2OSIgeT0iMyIgd2lkdGg9IjM4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQ2NyIgeT0iMSIgd2lkdGg9IjM4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0NzciIHk9IjIxIj49Jmd0OzwvdGV4dD4KICAgPHJlY3QgeD0iMzY1IiB5PSI2OSIgd2lkdGg9IjkwIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzNjMiIHk9IjY3IiB3aWR0aD0iOTAiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzNzMiIHk9Ijg3Ij52YWx1ZV90eXBlPC90ZXh0PgogICA8cmVjdCB4PSI0NzUiIHk9IjY5IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDczIiB5PSI2NyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0ODMiIHk9Ijg3Ij5dPC90ZXh0PgogICA8cGF0aCBjbGFzcz0ibGluZSIgZD0ibTE3IDE3IGgyIG0wIDAgaDEwIG0yMiAwIGgxMCBtMCAwIGgxMCBtOTIgMCBoMTAgbTAgMCBoMTAgbTIyIDAgaDEwIG0wIDAgaDEwIG0yOCAwIGgxMCBtMCAwIGgxMCBtNTIgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0wIDAgaDEwIG01NiAwIGgxMCBtMCAwIGgxMCBtMzggMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0xODYgNjYgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgaDEwIG05MCAwIGgxMCBtMCAwIGgxMCBtMjYgMCBoMTAgbTMgMCBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTE5IDgzIDUyNyA3OSA1MjcgODciPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSI1MTkgODMgNTExIDc5IDUxMSA4NyI+PC9wb2x5Z29uPgo8L3N2Zz4=)

</div>

| Field        | Use                                          |
|--------------|----------------------------------------------|
| *map_string* | A well-formed map object.                    |
| *value_type* | The [type](../../types) of the map’s values. |

## Map functions + operators

### Operators

| Operator | RHS Type | Description |
|----|----|----|
| `->` | `string` | Access field by name, and return target field ([docs](/docs/sql/types/map/#retrieve-value-with-key--)) |
| `@>` | `map` | Does element contain RHS? ([docs](/docs/sql/types/map/#lhs-contains-rhs-)) |
| `<@` | `map` | Does RHS contain element? ([docs](/docs/sql/types/map/#rhs-contains-lhs-)) |
| `?` | `string` | Is RHS a top-level key? ([docs](/docs/sql/types/map/#search-top-level-keys-)) |
| `?&` | `string[]` | Does LHS contain all RHS top-level keys? ([docs](/docs/sql/types/map/#search-for-all-top-level-keys-)) |
| `?|` | `string[]` | Does LHS contain any RHS top-level keys? ([docs](/docs/sql/types/map/#search-for-any-top-level-keys-)) |

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
<td><div id="map_length" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>map_length(m: mapany) -&gt; int</code></pre>
</div>
</div>
<p>Return the number of elements in <code>m</code>.</p></td>
</tr>
<tr>
<td><div id="map_build" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>map_build(kvs: list record(text, T)) -&gt; map[text=&gt;T]</code></pre>
</div>
</div>
<p>Builds a map from a list of records whose fields are two elements,
the first of which is <code>text</code>. In the face of duplicate keys,
<code>map_build</code> retains value from the record in the latest
positition. This function is purpose-built to process <a
href="/docs/sql/create-source/kafka/#headers">Kafka
headers</a>.</p></td>
</tr>
<tr>
<td><div id="map_agg" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>map_agg(keys: text, values: T) -&gt; map[text=&gt;T]</code></pre>
</div>
</div>
<p>Aggregate keys and values (including nulls) as a map <a
href="/docs/sql/functions/map_agg">(docs)</a>.</p></td>
</tr>
</tbody>
</table>

## Details

### Construction

You can construct maps using the `MAP` expression:

<div class="highlight">

``` chroma
SELECT MAP['a' => 1, 'b' => 2];
```

</div>

```
     map
-------------
 {a=>1,b=>2}
```

You can nest `MAP` constructors:

<div class="highlight">

``` chroma
SELECT MAP['a' => MAP['b' => 'c']];
```

</div>

```
     map
-------------
 {a=>{b=>c}}
```

You can also elide the `MAP` keyword from the interior map expressions:

<div class="highlight">

``` chroma
SELECT MAP['a' => ['b' => 'c']];
```

</div>

```
     map
-------------
 {a=>{b=>c}}
```

`MAP` expressions evalute expressions for both keys and values:

<div class="highlight">

``` chroma
SELECT MAP['a' || 'b' => 1 + 2];
```

</div>

```
     map
-------------
 {ab=>3}
```

Alternatively, you can construct a map from the results of a subquery.
The subquery must return two columns: a key column of type `text` and a
value column of any type, in that order. Note that, in this form of the
`MAP` expression, parentheses are used rather than square brackets.

<div class="highlight">

``` chroma
SELECT MAP(SELECT key, value FROM test0 ORDER BY x DESC LIMIT 3);
```

</div>

```
       map
------------------
 {a=>1,b=>2,c=>3}
```

With all constructors, if the same key appears multiple times, the last
value for the key wins.

Note that you can also construct maps using the available [`text`
cast](#text-to-map-casts).

### Constraints

- Keys must be of type [`text`](../text).
- Values can be of any [type](../../types) as long as the type is
  uniform.
- Keys must be unique. If duplicate keys are present in a map, only one
  of the (`key`, `value`) pairs will be retained. There is no guarantee
  which will be retained.

### Custom types

You can create [custom `map` types](/docs/sql/types/#custom-types),
which lets you create a named entry in the catalog for a specific type
of `map`.

Currently, custom types only provides a shorthand for referring to
otherwise-annoying-to-type names.

### `text` to `map` casts

The textual format for a `map` is a sequence of `key => value` mappings
separated by commas and surrounded by curly braces (`{}`). For example:

<div class="highlight">

``` chroma
SELECT '{a=>123.4, b=>111.1}'::map[text=>double] as m;
```

</div>

```
  m
------------------
 {a=>123.4,b=>111.1}
```

You can create nested maps the same way:

<div class="highlight">

``` chroma
SELECT '{a=>{b=>{c=>d}}}'::map[text=>map[text=>map[text=>text]]] as nested_map;
```

</div>

```
  nested_map
------------------
 {a=>{b=>{c=>d}}}
```

### Valid casts

#### Between `map`s

Two `map` types can only be cast to and from one another if they are
structurally equivalent, e.g. one is a [custom map
type](/docs/sql/types#custom-types) and the other is a [built-in
map](/docs/sql/types#built-in-types) and their key-value types are
structurally equivalent.

#### From `map`

You can [cast](../../functions/cast) `map` to and from the following
types:

- [`text`](../text) (by assignment)
- Other `map`s as noted above.

#### To `map`

- [`text`](../text) (explicitly)
- Other `map`s as noted above.

## Examples

### Operators

#### Retrieve value with key (`->`)

Retrieves and returns the target value or `NULL`.

<div class="highlight">

``` chroma
SELECT MAP['a' => 1, 'b' => 2] -> 'a' as field_map;
```

</div>

```
 field_map
-----------
 1
```

<div class="highlight">

``` chroma
SELECT MAP['a' => 1, 'b' => 2] -> 'c' as field_map;
```

</div>

```
 field_map
----------
 NULL
```

Field accessors can also be chained together.

<div class="highlight">

``` chroma
SELECT MAP['a' => ['b' => 1], 'c' => ['d' => 2]] -> 'a' -> 'b' as field_map;
```

</div>

```
 field_map
-------------
 1
```

Note that all returned values are of the map’s value type.

------------------------------------------------------------------------

#### LHS contains RHS (`@>`)

<div class="highlight">

``` chroma
SELECT MAP['a' => 1, 'b' => 2] @> MAP['a' => 1] AS lhs_contains_rhs;
```

</div>

```
 lhs_contains_rhs
------------------
 t
```

------------------------------------------------------------------------

#### RHS contains LHS (`<@`)

<div class="highlight">

``` chroma
SELECT MAP['a' => 1, 'b' => 2] <@ MAP['a' => 1] as rhs_contains_lhs;
```

</div>

```
 rhs_contains_lhs
------------------
 f
```

------------------------------------------------------------------------

#### Search top-level keys (`?`)

<div class="highlight">

``` chroma
SELECT MAP['a' => 1.9, 'b' => 2.0] ? 'a' AS search_for_key;
```

</div>

```
 search_for_key
----------------
 t
```

<div class="highlight">

``` chroma
SELECT MAP['a' => ['aa' => 1.9], 'b' => ['bb' => 2.0]] ? 'aa' AS search_for_key;
```

</div>

```
 search_for_key
----------------
 f
```

#### Search for all top-level keys (`?&`)

Returns `true` if all keys provided on the RHS are present in the
top-level of the map, `false` otherwise.

<div class="highlight">

``` chroma
SELECT MAP['a' => 1, 'b' => 2] ?& ARRAY['b', 'a'] as search_for_all_keys;
```

</div>

```
 search_for_all_keys
---------------------
 t
```

<div class="highlight">

``` chroma
SELECT MAP['a' => 1, 'b' => 2] ?& ARRAY['c', 'b'] as search_for_all_keys;
```

</div>

```
 search_for_all_keys
---------------------
 f
```

#### Search for any top-level keys (`?|`)

Returns `true` if any keys provided on the RHS are present in the
top-level of the map, `false` otherwise.

<div class="highlight">

``` chroma
SELECT MAP['a' => 1, 'b' => 2] ?| ARRAY['c', 'b'] as search_for_any_keys;
```

</div>

```
 search_for_any_keys
---------------------
 t
```

<div class="highlight">

``` chroma
SELECT MAP['a' => 1, 'b' => 2] ?| ARRAY['c', 'd', '1'] as search_for_any_keys;
```

</div>

```
 search_for_any_keys
---------------------
 f
```

#### Count entries in map (`map_length`)

Returns the number of entries in the map.

<div class="highlight">

``` chroma
SELECT map_length(MAP['a' => 1, 'b' => 2]);
```

</div>

```
 map_length
------------
 2
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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/types/map.md"
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
