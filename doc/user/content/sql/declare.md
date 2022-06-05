---
title: "DECLARE"
description: "`DECLARE` creates a cursor."
menu:
  main:
    parent: "commands"
---

`DECLARE` creates a cursor, which can be used with
[`FETCH`](/sql/fetch), to retrieve a limited number of rows at a time
from a larger query. Large queries or queries that don't ever complete
([`TAIL`](/sql/tail)) can be difficult to use with many PostgreSQL drivers
that wait until all rows are returned before returning control to an
application. Using `DECLARE` and [`FETCH`](/sql/fetch) allows you to
fetch only some of the rows at a time.

## Syntax

{{< diagram "declare.svg" >}}

Field | Use
------|-----
_cursor&lowbar;name_ | The name of the cursor to be created.
_query_ | The query ([`SELECT`](/sql/select) or [`TAIL`](/sql/tail)) that will provide the rows to be returned by the cursor.
