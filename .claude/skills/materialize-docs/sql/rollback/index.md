---
audience: developer
canonical_url: https://materialize.com/docs/sql/rollback/
complexity: intermediate
description: '`ROLLBACK` aborts the current [transaction](/sql/begin/#details) and
  all changes'
doc_type: reference
keywords:
- ROLLBACK
product_area: Indexes
status: stable
title: ROLLBACK
---

# ROLLBACK

## Purpose
`ROLLBACK` aborts the current [transaction](/sql/begin/#details) and all changes
in the transaction are discarded.

If you need to understand the syntax and options for this command, you're in the right place.




`ROLLBACK` aborts the current [transaction](/sql/begin/#details) and all changes
in the transaction are discarded.

## Syntax

This section covers syntax.

```mzsql
ROLLBACK;
```

## Details

Rolls back the current transaction, discarding all changes made by the transaction.

