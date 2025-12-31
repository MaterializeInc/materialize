---
audience: developer
canonical_url: https://materialize.com/docs/sql/deallocate/
complexity: intermediate
description: '`DEALLOCATE` clears a prepared statement.'
doc_type: reference
keywords:
- ALL
- DEALLOCATE
product_area: Indexes
status: stable
title: DEALLOCATE
---

# DEALLOCATE

## Purpose
`DEALLOCATE` clears a prepared statement.

If you need to understand the syntax and options for this command, you're in the right place.


`DEALLOCATE` clears a prepared statement.



`DEALLOCATE` clears [prepared statements](../prepare) that have been created during the current session. Even without an explicit `DEALLOCATE` command, all prepared statements will be cleared at the end of a session.

## Syntax

This section covers syntax.

```mzsql
DEALLOCATE <name>|ALL ;
```text

Syntax element | Description
---------------|------------
`<name>`  | The name of the prepared statement to clear.
**ALL**  |  Clear all prepared statements from this session.

## Example

This section covers example.

```mzsql
DEALLOCATE a;
```

## Related pages

- [`PREPARE`]
- [`EXECUTE`]

[`PREPARE`]:../prepare
[`EXECUTE`]:../execute

