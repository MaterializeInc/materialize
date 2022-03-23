---
title: "DECLARE"
description: "`DECLARE` creates a cursor."
menu:
  main:
    parent: "sql"
---

{{< version-added v0.5.3 />}}

`DECLARE` creates a cursor, which can be used with
[`FETCH`](/sql/fetch), to retrieve a limited number of rows at a time
from a larger query. Large queries or queries that don't ever complete
([`TAIL`](/sql/tail)) can be difficult to use with many PostgreSQL drivers
that wait until all rows are returned before returning control to an
application. Using `DECLARE` and [`FETCH`](/sql/fetch) allows you to
fetch only some of the rows at a time.

## Syntax

```sql
DECLARE cursor_name CURSOR [ WITHOUT HOLD ] FOR query
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "declare.svg" >}}

</details>
<br/>

Field | Use
------|-----
**WITHOUT HOLD** | Explicit mention of default behavior. The cursor closes at the end of a transaction
_cursor&lowbar;name_ | The name of the cursor to be created.
_query_ | The query ([`SELECT`](/sql/select) or [`TAIL`](/sql/tail)) that will provide the rows to be returned by the cursor.

## Example

Create a cursor for a query over millions of rows:

```sql
BEGIN;

DECLARE messages_cursor CURSOR FOR ( SELECT * FROM historical_messages );

-- Fetch only a hundred rows 
FETCH 100 messages_cursor;

COMMIT;
```