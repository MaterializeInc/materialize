---
title: "CREATE TABLE: Read-write table"
description: "Create a read-write, user-populated table in Materialize."
menu:
  main:
    parent: 'create-table'
    name: "Read-write table"
    identifier: 'create-table-user-populated'
    weight: 80
---

Read-write tables let you read ([`SELECT`](/sql/select/)) and write
([`INSERT`](/sql/insert/), [`UPDATE`](/sql/update/), [`DELETE`](/sql/delete/))
to the table.

## Syntax

{{% include-syntax file="examples/create_table_user_populated"
example="syntax" %}}

## Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

## Known limitations

Tables do not currently support:

- Primary keys
- Unique constraints
- Check constraints

See also the known limitations for [`INSERT`](/sql/insert#known-limitations),
[`UPDATE`](/sql/update#known-limitations), and [`DELETE`](/sql/delete#known-limitations).

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-table" %}}

## Examples

### Create a table

{{% include-example file="examples/create_table_user_populated"
 example="create-table" %}}

Once a user-populated table is created, you can perform CRUD
(Create/Read/Update/Delete) operations on it.

{{% include-example file="examples/create_table_user_populated"
 example="write-to-table" %}}

{{% include-example file="examples/create_table_user_populated"
 example="read-from-table" %}}

## Related pages

- [`INSERT`](/sql/insert/)
- [`DROP TABLE`](/sql/drop-table)
