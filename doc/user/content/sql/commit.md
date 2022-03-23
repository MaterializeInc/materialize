---
title: "COMMIT"
menu:
  main:
    parent: "sql"
---

`COMMIT` commits the current transaction.

## Syntax

```sql
COMMIT
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "commit.svg" >}}

</details>
<br/>

## Details

If the current transaction is **write only**, the changes occur at the transaction's chosen time.

## Example

Commit the current transaction:

```sql
COMMIT;
```