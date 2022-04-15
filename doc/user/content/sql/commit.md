---
title: "COMMIT"
menu:
  main:
    parent: "commands"
---

`COMMIT` commits the current transaction.

## Syntax

{{< diagram "commit.svg" >}}

## Details

If the current transaction is **write only**, the changes occur at the transaction's chosen time.
