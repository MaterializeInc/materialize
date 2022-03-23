---
title: "ROLLBACK"
menu:
  main:
    parent: "sql"
---

`ROLLBACK` aborts the current transaction.

## Syntax

```sql
ROLLBACK
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "rollback.svg" >}}

</details>
<br/>

## Details

Rolls back the current transaction, discarding all changes made by the transaction.

## Example

Create and rollback a transaction:

```sql
BEGIN;

INSERT INTO untouchable_table VALUES (10);

ROLLBACK;
```