---
title: "ROLLBACK"
menu:
  main:
    parent: commands
---

`ROLLBACK` aborts the current [transaction](/sql/begin/#details) and rolls back
all changes made by the transaction.

## Syntax

```mzsql
ROLLBACK;
```

## Details

Rolls back the current transaction, discarding all changes made by the transaction.

## See also

- [`BEGIN`](/sql/begin)
- [`COMMIT`](/sql/commit)
