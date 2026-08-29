---
title: "SHOW CREATE FOREIGN KEY"
description: "`SHOW CREATE FOREIGN KEY` returns the statement used to create the foreign key."
menu:
  main:
    parent: commands
---

{{< private-preview />}}

`SHOW CREATE FOREIGN KEY` returns the DDL statement used to create the foreign
key.

## Syntax

```sql
SHOW [REDACTED] CREATE FOREIGN KEY <constraint_name>;
```

{{< yaml-table data="show_create_redacted_option" >}}

For available foreign key names, see
[`SHOW FOREIGN KEYS`](/sql/show-foreign-keys).

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
SHOW CREATE FOREIGN KEY orders_customer_id_fkey;
```

```nofmt
                     name                     |                                          create_sql
----------------------------------------------+-----------------------------------------------------------------------------------------------
 materialize.public.orders_customer_id_fkey   | CREATE FOREIGN KEY orders_customer_id_fkey ON materialize.public.orders (customer_id) REFERENCES materialize.public.customers (id) NOT ENFORCED;
```

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/show-create-foreign-key" %}}

## Related pages

- [`SHOW FOREIGN KEYS`](../show-foreign-keys)
- [`CREATE FOREIGN KEY`](../create-foreign-key)
