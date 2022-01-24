---
title: "CREATE SOURCE: Text or bytes over Kinesis"
description: "Learn how to connect Materialize o a text- or byte–formatted Kinesis stream"
menu:
  main:
    parent: 'create-source'
---

{{< beta />}}

{{% create-source/intro %}}
This document details how to connect Materialize to a text- or byte–formatted
Kinesis stream.

{{< volatility-warning >}}Kinesis{{< /volatility-warning >}}

{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-text-kinesis.svg" >}}

#### `with_options`

{{< diagram "with-options-aws.svg" >}}

{{% create-source/syntax-details connector="kinesis" formats="text bytes" envelopes="append-only" keyConstraint=false %}}

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
