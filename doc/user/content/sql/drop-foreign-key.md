---
title: "DROP FOREIGN KEY"
description: "DROP FOREIGN KEY removes a foreign key"
menu:
  main:
    parent: 'commands'
---

{{< private-preview />}}

`DROP FOREIGN KEY` removes a foreign key from Materialize. The relations it
named are left untouched.

## Syntax

```mzsql
DROP FOREIGN KEY [IF EXISTS] <constraint_name> [CASCADE|RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified foreign key does not exist.
`<constraint_name>` | Foreign key to drop.
**CASCADE** | Optional. If specified, remove the foreign key and its dependent objects.
**RESTRICT** | Optional. Remove the foreign key. _(Default.)_

{{< note >}}

Since foreign keys do not have dependent objects, `DROP FOREIGN KEY`, `DROP
FOREIGN KEY RESTRICT`, and `DROP FOREIGN KEY CASCADE` are equivalent.

{{< /note >}}

Dropping either relation a foreign key names also drops the foreign key, without
`CASCADE`. You only need `DROP FOREIGN KEY` to remove the relationship while
keeping both relations.

## Privileges

To execute the `DROP FOREIGN KEY` statement, you need:

{{% include-headless "/headless/sql-command-privileges/drop-foreign-key" %}}

## Examples

### Remove a foreign key

For the names of existing foreign keys, use
[`SHOW FOREIGN KEYS`](/sql/show-foreign-keys).

```mzsql
DROP FOREIGN KEY orders_customer_fkey;
```

If the foreign key `orders_customer_fkey` does not exist, the above operation
returns an error.

### Remove a foreign key without erroring if it does not exist

You can specify the `IF EXISTS` option so that the `DROP FOREIGN KEY` command
does not return an error if the foreign key to drop does not exist.

```mzsql
DROP FOREIGN KEY IF EXISTS orders_customer_fkey;
```

## Related pages

- [`CREATE FOREIGN KEY`](/sql/create-foreign-key)
- [`SHOW FOREIGN KEYS`](/sql/show-foreign-keys)
- [`DROP OWNED`](/sql/drop-owned)
