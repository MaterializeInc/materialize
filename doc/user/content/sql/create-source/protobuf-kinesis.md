---
title: "CREATE SOURCE: Protobuf over Kinesis"
description: "Learn how to connect Materialize to a Protobuf-formatted Kinesis topic"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro %}}
This document details how to connect Materialize to Protobuf-formatted Kinesis
stream.

{{< kinesis-alpha >}}

{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-protobuf-kinesis.svg" >}}

{{% create-source/syntax-details connector="kinesis" formats="protobuf" envelopes="append-only" %}}

## Examples

### Receiving Protobuf messages

Assuming you've already generated a [`FileDescriptorSet`](#filedescriptorset)
named `SCHEMA`:

```sql
CREATE SOURCE batches
FROM KINESIS ARN ... WITH (
    access_key_id = ...,
    secret_access_key = ...
)
FORMAT PROTOBUF MESSAGE '.billing.Batch' USING '[path to schema]';
```

This creates a source that...

- Is append-only.
- Decodes data as the `Batch` message from the `billing` package, as described
  in the [generated `FileDescriptorSet`](#filedescriptorset).

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
