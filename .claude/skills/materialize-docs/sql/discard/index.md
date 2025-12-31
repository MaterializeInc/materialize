---
audience: developer
canonical_url: https://materialize.com/docs/sql/discard/
complexity: intermediate
description: Discard resets state associated with the current session.
doc_type: reference
keywords:
- TEMP
- DISCARD
- TEMPORARY
- ALL
product_area: Indexes
status: stable
title: DISCARD
---

# DISCARD

## Purpose
Discard resets state associated with the current session.

If you need to understand the syntax and options for this command, you're in the right place.


Discard resets state associated with the current session.



`DISCARD` resets state associated with the current session.

## Syntax

This section covers syntax.

```mzsql
DISCARD TEMP|TEMPORARY|ALL ;
```


Syntax element | Description
---------------|------------
**TEMP**  | Drops any temporary objects created by the current session.
**TEMPORARY** | Alias for `TEMP`.
**ALL** | Drops any temporary objects, deallocates any extant prepared statements, and closes any extant cursors that were created by the current session.

