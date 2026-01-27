# CREATE TYPE
`CREATE TYPE` defines a new data type.
`CREATE TYPE` defines a custom data type, which let you create named versions of
anonymous types or provide a shorthand for other types. For more information,
see [SQL Data Types: Custom types](../types/#custom-types).

## Syntax


**Row type:**
### Row type


```mzsql
CREATE TYPE <type_name> AS (<field_name> <field_type>, ...);

```

| Syntax element | Description |
| --- | --- |
| `<type_name>` | A name for the type.  |
| `<field_name>` | The name of a field in a row type.  |
| `<field_type>` | The data type of a field indicated by `field_name`.  |



**List type:**
### List type


```mzsql
CREATE TYPE <type_name> AS LIST (ELEMENT TYPE = <element_type>);

```

| Syntax element | Description |
| --- | --- |
| `<type_name>` | A name for the type.  |
| `<element_type>` | Creates a custom [`list`](/sql/types/list) whose elements are of `<element_type>`.  |



**Map type:**
### Map type


```mzsql
CREATE TYPE <type_name> AS MAP (KEY TYPE = <key_type>, VALUE TYPE = <value_type>);

```

| Syntax element | Description |
| --- | --- |
| `<type_name>` | A name for the type.  |
| `<key_type>` | Creates a custom [`map`](/sql/types/map) whose keys are of `<key_type>`. Must resolve to [`text`](/sql/types/text).  |
| `<value_type>` | Creates a custom [`map`](/sql/types/map) whose values are of `<value_type>`.  |






## Details

For details about the custom types `CREATE TYPE` creates, see [SQL Data Types:
Custom types](../types/#custom-types).

### Properties

All custom type properties' values must refer to [named types](/sql/types), e.g.
`integer`.

To create a custom nested `list` or `map`, you must first create a custom `list`
or `map`. This creates a named type, which can then be referred to in another
custom type's properties.

## Examples

### Custom `list`

```mzsql
CREATE TYPE int4_list AS LIST (ELEMENT TYPE = int4);

SELECT '{1,2}'::int4_list::text AS custom_list;
```
```
 custom_list
-------------
 {1,2}
```

### Nested custom `list`

```mzsql
CREATE TYPE int4_list_list AS LIST (ELEMENT TYPE = int4_list);

SELECT '{{1,2}}'::int4_list_list::text AS custom_nested_list;
```
```
 custom_nested_list
--------------------
 {{1,2}}
```

### Custom `map`

```mzsql
CREATE TYPE int4_map AS MAP (KEY TYPE = text, VALUE TYPE = int4);

SELECT '{a=>1}'::int4_map::text AS custom_map;
```
```
 custom_map
------------
 {a=>1}
```

### Nested custom `map`

```mzsql
CREATE TYPE int4_map_map AS MAP (KEY TYPE = text, VALUE TYPE = int4_map);

SELECT '{a=>{a=>1}}'::int4_map_map::text AS custom_nested_map;
```
```
 custom_nested_map
-------------------
{a=>{a=>1}}
```

### Custom `row` type
```mzsql
CREATE TYPE row_type AS (a int, b text);
SELECT ROW(1, 'a')::row_type as custom_row_type;
```
```
custom_row_type
-----------------
(1,a)
```

### Nested `row` type
```mzsql
CREATE TYPE nested_row_type AS (a row_type, b float8);
SELECT ROW(ROW(1, 'a'), 2.3)::nested_row_type AS custom_nested_row_type;
```
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

* [`DROP TYPE`](../drop-type)
* [`SHOW TYPES`](../show-types)
