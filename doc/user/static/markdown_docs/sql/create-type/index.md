<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)

</div>

# CREATE TYPE

`CREATE TYPE` defines a new data type.

## Conceptual framework

`CREATE TYPE` creates custom types, which let you create named versions
of anonymous types. For more information, see [SQL Data Types: Custom
types](../types/#custom-types).

### Use

Currently, custom types provide a shorthand for referring to
otherwise-annoying-to-type names.

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MTkiIGhlaWdodD0iMjc5Ij4KICAgPHBvbHlnb24gcG9pbnRzPSIxMSAxNyAzIDEzIDMgMjEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSIxOSAxNyAxMSAxMyAxMSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMyIgeT0iMyIgd2lkdGg9Ijc2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjMxIiB5PSIxIiB3aWR0aD0iNzYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjQxIiB5PSIyMSI+Q1JFQVRFPC90ZXh0PgogICA8cmVjdCB4PSIxMjkiIHk9IjMiIHdpZHRoPSI1NiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMjciIHk9IjEiIHdpZHRoPSI1NiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTM3IiB5PSIyMSI+VFlQRTwvdGV4dD4KICAgPHJlY3QgeD0iMjA1IiB5PSIzIiB3aWR0aD0iOTIiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjIwMyIgeT0iMSIgd2lkdGg9IjkyIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjEzIiB5PSIyMSI+dHlwZV9uYW1lPC90ZXh0PgogICA8cmVjdCB4PSIzMTciIHk9IjMiIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzMTUiIHk9IjEiIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzI1IiB5PSIyMSI+QVM8L3RleHQ+CiAgIDxyZWN0IHg9IjQ1IiB5PSIxMTMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0MyIgeT0iMTExIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjUzIiB5PSIxMzEiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjExMSIgeT0iMTEzIiB3aWR0aD0iOTAiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjEwOSIgeT0iMTExIiB3aWR0aD0iOTAiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIxMTkiIHk9IjEzMSI+ZmllbGRfbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iMjIxIiB5PSIxMTMiIHdpZHRoPSI4NCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjE5IiB5PSIxMTEiIHdpZHRoPSI4NCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjIyOSIgeT0iMTMxIj5maWVsZF90eXBlPC90ZXh0PgogICA8cmVjdCB4PSIxMTEiIHk9IjY5IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTA5IiB5PSI2NyIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMTkiIHk9Ijg3Ij4sPC90ZXh0PgogICA8cmVjdCB4PSI2NSIgeT0iMjAxIiB3aWR0aD0iNTAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNjMiIHk9IjE5OSIgd2lkdGg9IjUwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI3MyIgeT0iMjE5Ij5MSVNUPC90ZXh0PgogICA8cmVjdCB4PSI2NSIgeT0iMjQ1IiB3aWR0aD0iNTIiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNjMiIHk9IjI0MyIgd2lkdGg9IjUyIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI3MyIgeT0iMjYzIj5NQVA8L3RleHQ+CiAgIDxyZWN0IHg9IjE1NyIgeT0iMjAxIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTU1IiB5PSIxOTkiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTY1IiB5PSIyMTkiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjIyMyIgeT0iMjAxIiB3aWR0aD0iNzYiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjIyMSIgeT0iMTk5IiB3aWR0aD0iNzYiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIyMzEiIHk9IjIxOSI+cHJvcGVydHk8L3RleHQ+CiAgIDxyZWN0IHg9IjMxOSIgeT0iMjAxIiB3aWR0aD0iMjgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzE3IiB5PSIxOTkiIHdpZHRoPSIyOCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzI3IiB5PSIyMTkiPj08L3RleHQ+CiAgIDxyZWN0IHg9IjM2NyIgeT0iMjAxIiB3aWR0aD0iMzgiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjM2NSIgeT0iMTk5IiB3aWR0aD0iMzgiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzNzUiIHk9IjIxOSI+dmFsPC90ZXh0PgogICA8cmVjdCB4PSIyMjMiIHk9IjE1NyIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIyMSIgeT0iMTU1IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIzMSIgeT0iMTc1Ij4sPC90ZXh0PgogICA8cmVjdCB4PSI0NjUiIHk9IjExMyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQ2MyIgeT0iMTExIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjQ3MyIgeT0iMTMxIj4pPC90ZXh0PgogICA8cGF0aCBjbGFzcz0ibGluZSIgZD0ibTE5IDE3IGgyIG0wIDAgaDEwIG03NiAwIGgxMCBtMCAwIGgxMCBtNTYgMCBoMTAgbTAgMCBoMTAgbTkyIDAgaDEwIG0wIDAgaDEwIG00MCAwIGgxMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtLTM3NiAxMTAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtMjYgMCBoMTAgbTIwIDAgaDEwIG05MCAwIGgxMCBtMCAwIGgxMCBtODQgMCBoMTAgbS0yMzQgMCBsMjAgMCBtLTEgMCBxLTkgMCAtOSAtMTAgbDAgLTI0IHEwIC0xMCAxMCAtMTAgbTIxNCA0NCBsMjAgMCBtLTIwIDAgcTEwIDAgMTAgLTEwIGwwIC0yNCBxMCAtMTAgLTEwIC0xMCBtLTIxNCAwIGgxMCBtMjQgMCBoMTAgbTAgMCBoMTcwIG0yMCA0NCBoMTAwIG0tNDIwIDAgaDIwIG00MDAgMCBoMjAgbS00NDAgMCBxMTAgMCAxMCAxMCBtNDIwIDAgcTAgLTEwIDEwIC0xMCBtLTQzMCAxMCB2NjggbTQyMCAwIHYtNjggbS00MjAgNjggcTAgMTAgMTAgMTAgbTQwMCAwIHExMCAwIDEwIC0xMCBtLTM5MCAxMCBoMTAgbTUwIDAgaDEwIG0wIDAgaDIgbS05MiAwIGgyMCBtNzIgMCBoMjAgbS0xMTIgMCBxMTAgMCAxMCAxMCBtOTIgMCBxMCAtMTAgMTAgLTEwIG0tMTAyIDEwIHYyNCBtOTIgMCB2LTI0IG0tOTIgMjQgcTAgMTAgMTAgMTAgbTcyIDAgcTEwIDAgMTAgLTEwIG0tODIgMTAgaDEwIG01MiAwIGgxMCBtMjAgLTQ0IGgxMCBtMjYgMCBoMTAgbTIwIDAgaDEwIG03NiAwIGgxMCBtMCAwIGgxMCBtMjggMCBoMTAgbTAgMCBoMTAgbTM4IDAgaDEwIG0tMjIyIDAgbDIwIDAgbS0xIDAgcS05IDAgLTkgLTEwIGwwIC0yNCBxMCAtMTAgMTAgLTEwIG0yMDIgNDQgbDIwIDAgbS0yMCAwIHExMCAwIDEwIC0xMCBsMCAtMjQgcTAgLTEwIC0xMCAtMTAgbS0yMDIgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDE1OCBtNDAgLTQ0IGgxMCBtMjYgMCBoMTAgbTMgMCBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTA5IDEyNyA1MTcgMTIzIDUxNyAxMzEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSI1MDkgMTI3IDUwMSAxMjMgNTAxIDEzMSI+PC9wb2x5Z29uPgo8L3N2Zz4=)

</div>

| Field | Use |
|----|----|
| *type_name* | A name for the type. |
| **MAP / LIST** | The data type. If not specified, a row type is assumed. |
| *property* **=** *val* | A property of the new type. This is required when specifying a `LIST` or `MAP` type. Note that type properties can only refer to data types within the catalog, i.e. they cannot refer to anonymous `list` or `map` types. |

### `row` properties

| Field        | Use                                                 |
|--------------|-----------------------------------------------------|
| *field_name* | The name of a field in a row type.                  |
| *field_type* | The data type of a field indicated by *field_name*. |

### `list` properties

| Field | Use |
|----|----|
| `ELEMENT TYPE` | Creates a custom [`list`](../types/list) whose elements are of `ELEMENT TYPE`. |

### `map` properties

| Field | Use |
|----|----|
| `KEY TYPE` | Creates a custom [`map`](../types/map) whose keys are of `KEY TYPE`. `KEY TYPE` must resolve to [`text`](../types/text). |
| `VALUE TYPE` | Creates a custom [`map`](../types/map) whose values are of `VALUE TYPE`. |

## Details

For details about the custom types `CREATE TYPE` creates, see [SQL Data
Types: Custom types](../types/#custom-types).

### Properties

All custom type properties’ values must refer to [named
types](/docs/self-managed/v25.2/sql/types), e.g. `integer`.

To create a custom nested `list` or `map`, you must first create a
custom `list` or `map`. This creates a named type, which can then be
referred to in another custom type’s properties.

## Examples

### Custom `list`

<div class="highlight">

``` chroma
CREATE TYPE int4_list AS LIST (ELEMENT TYPE = int4);

SELECT '{1,2}'::int4_list::text AS custom_list;
```

</div>

```
 custom_list
-------------
 {1,2}
```

### Nested custom `list`

<div class="highlight">

``` chroma
CREATE TYPE int4_list_list AS LIST (ELEMENT TYPE = int4_list);

SELECT '{{1,2}}'::int4_list_list::text AS custom_nested_list;
```

</div>

```
 custom_nested_list
--------------------
 {{1,2}}
```

### Custom `map`

<div class="highlight">

``` chroma
CREATE TYPE int4_map AS MAP (KEY TYPE = text, VALUE TYPE = int4);

SELECT '{a=>1}'::int4_map::text AS custom_map;
```

</div>

```
 custom_map
------------
 {a=>1}
```

### Nested custom `map`

<div class="highlight">

``` chroma
CREATE TYPE int4_map_map AS MAP (KEY TYPE = text, VALUE TYPE = int4_map);

SELECT '{a=>{a=>1}}'::int4_map_map::text AS custom_nested_map;
```

</div>

```
 custom_nested_map
-------------------
{a=>{a=>1}}
```

### Custom `row` type

<div class="highlight">

``` chroma
CREATE TYPE row_type AS (a int, b text);
SELECT ROW(1, 'a')::row_type as custom_row_type;
```

</div>

```
custom_row_type
-----------------
(1,a)
```

### Nested `row` type

<div class="highlight">

``` chroma
CREATE TYPE nested_row_type AS (a row_type, b float8);
SELECT ROW(ROW(1, 'a'), 2.3)::nested_row_type AS custom_nested_row_type;
```

</div>

```
custom_nested_row_type
------------------------
("(1,a)",2.3)
```

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the type definition.
- `USAGE` privileges on the schemas that all types in the statement are
  contained in.

## Related pages

- [`DROP TYPE`](../drop-type)
- [`SHOW TYPES`](../show-types)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-type.md"
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
