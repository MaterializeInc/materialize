---
title: "SHOW FOREIGN KEYS"
description: "SHOW FOREIGN KEYS provides details about the foreign keys declared on a relation"
menu:
  main:
    parent: commands
---

{{< private-preview />}}

`SHOW FOREIGN KEYS` provides details about the foreign keys declared on a table,
view, materialized view, or source.

## Syntax

```mzsql
SHOW FOREIGN KEYS [ FROM <schema_name> | ON <object_name> ]
[ LIKE <pattern> | WHERE <condition(s)> ]
;
```

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show foreign keys from the specified schema. Defaults to first resolvable schema in the search path if `ON <object_name>` is not specified. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**ON** <object_name>          | If specified, only show foreign keys that name the specified object, on either side of the relationship.
**LIKE** \<pattern\>          | If specified, only show foreign keys that match the pattern.
**WHERE** <condition(s)>      | If specified, only show foreign keys that match the condition(s).

## Details

### Output format

`SHOW FOREIGN KEYS`'s output is a table with the following structure:

```nofmt
name | on  | references | key | comment
-----+-----+------------+-----+--------
 ... | ... | ...        | ... | ...
```

Field | Meaning
------|--------
**name** | The name of the foreign key.
**on** | The name of the relation holding the key.
**references** | The name of the relation the key points at.
**key** | A text array describing the column pairs, each rendered as the equality it asserts.
**comment** | The comment on the foreign key, or the empty string if it has none.

### Both directions

`ON <object_name>` matches a foreign key that names the object on either side.
A relation's relationships are as much the ones pointing at it as the ones it
declares, and a consumer looking for join paths wants both.

## Examples

```mzsql
SHOW FOREIGN KEYS ON orders;
```

```nofmt
          name           |   on   | references |         key          | comment
-------------------------+--------+------------+----------------------+---------
 orders_customer_id_fkey | orders | customers  | {"customer_id = id"} |
```

```mzsql
SHOW FOREIGN KEYS FROM public WHERE "references" = 'customers';
```

```nofmt
          name           |   on   | references |         key          | comment
-------------------------+--------+------------+----------------------+---------
 orders_customer_id_fkey | orders | customers  | {"customer_id = id"} |
```

## Related pages

- [`CREATE FOREIGN KEY`](../create-foreign-key)
- [`SHOW CREATE FOREIGN KEY`](../show-create-foreign-key)
- [`DROP FOREIGN KEY`](../drop-foreign-key)
