---
title: "CREATE SOURCE: Text or bytes over Kinesis"
description: "Learn how to connect Materialize o a text- or byte–formatted Kinesis stream"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro %}}
This document details how to connect Materialize to a text- or byte–formatted
Kinesis stream.

{{< kinesis-alpha >}}

{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-text-kinesis.svg" >}}

{{% create-source/syntax-details connector="kinesis" formats="text bytes" envelopes="append-only" %}}

## Examples

### Upsert on a Kinesis stream

```sql
CREATE SOURCE kinesis_source
FROM KINESIS ARN ... WITH (
    access_key_id = ...,
    secret_access_key = ...
)
FORMAT BYTES;
```

This creates a source that...

- Is append-only.
- Has one column, `data`, which represents the stream's incoming bytes.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
