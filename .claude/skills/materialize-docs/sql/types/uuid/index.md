---
audience: developer
canonical_url: https://materialize.com/docs/sql/types/uuid/
complexity: intermediate
description: Expresses a universally-unique identifier
doc_type: reference
keywords:
- Quick Syntax
- Catalog name
- OID
- Size
- SELECT UUID
- uuid type
product_area: Indexes
status: stable
title: uuid type
---

# uuid type

## Purpose
Expresses a universally-unique identifier

If you need to understand the syntax and options for this command, you're in the right place.


Expresses a universally-unique identifier



`uuid` data expresses a universally-unique identifier (UUID).

Detail | Info
-------|------
**Quick Syntax** | `UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`
**Size** | 16 bytes
**Catalog name** | `pg_catalog.uuid`
**OID** | 2950

The `uuid` type is more space efficient than representing UUIDs as
[`text`](../text). A UUID stored as `text` requires either 32 or 36 bytes,
depending on the presence of hyphens, while the `uuid` type requires only 16
bytes.

## Syntax

[See diagram: type-uuid.svg]

The standard form of a UUID consists of five groups of lowercase hexadecimal
digits separated by hyphens, where the first group contains 8 digits, the next
three groups contain 4 digits each, and the last group contains 12 digits:

```text
a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
```

Materialize also accepts UUID input where the hyphens are omitted, or where some
or all of the hexadecimal digits are uppercase:

```text
a0eebc999c0b4ef8bb6d6bb9bd380a11
A0eeBc99-9c0b-4ef8-bB6d-6bb9bd380A11
A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11
```

Materialize will always output UUIDs in the standard form.

## Details

This section covers details.

### Valid casts

You can [cast](../../functions/cast) `uuid` to [`text`](../text) by assignment and from [`text`](../text) explicitly.

## Examples

This section covers examples.

```mzsql
SELECT UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS uuid
```text
```nofmt
                 uuid
--------------------------------------
 a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
```

