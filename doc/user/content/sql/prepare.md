---
title: "PREPARE"
description: "`PREPARE` creates a prepared statement."
menu:
  main:
    parent: commands
---

`PREPARE` creates a prepared statement by parsing the initial `SELECT`, `INSERT`, `UPDATE`, or `DELETE` statement. A subsequent [`EXECUTE`] statement then plans and executes the statement.

## Syntax

{{< diagram "prepare.svg" >}}

Field | Use
------|-----
name | A name for this particular prepared statement that you can later use to execute or deallocate a statement. The name must be unique within a session.
statement  |  Any `SELECT`, `INSERT`, `UPDATE`, or `DELETE` statement.

## Details

Prepared statements can take parameters: values that are substituted into the statement when it is executed. The data type is inferred from the context in which the parameter is first referenced. To refer to the parameters in the prepared statement itself, use `$1`, `$2`, etc.

Prepared statements only last for the duration of the current database session. You can also delete them during a session with the `DEALLOCATE` command.

## Examples

### Create a prepared statement

```sql
PREPARE a AS SELECT 1 + $1;
```

### Execute a prepared statement

```sql
EXECUTE a ('a', 'b', 1 + 2)
```

### Deallocate a prepared statement

```sql
DEALLOCATE a;
```

## Related pages

- [`DEALLOCATE`]
- [`EXECUTE`]

[`DEALLOCATE`]:../deallocate
[`EXECUTE`]:../execute
