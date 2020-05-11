---
title: "CREATE SOURCE: Avro from local file"
description: "Learn how to connect Materialize to an Avro-formatted local file"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro connector="local files" %}}
This document details how to connect Materialize to local [Avro Object Container
Files] (OCFs).

[Avro Object Container Files]: https://avro.apache.org/docs/current/spec.html#Object+Container+Files
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-avro-file.svg" >}}

{{% create-source/syntax-details connector="avro-ocf" formats="none" envelopes="append-only debezium" %}}

## Examples

```sql
CREATE SOURCE events
FROM AVRO OCF '[path to .ocf]'
WITH (tail = true)
ENVELOPE NONE;
```

This creates a source that...

- Automatically determines its schema from the OCF file's embedded schema.
- Materialize dynamically checks for new entries.
- Is append-only.

[Debezium]: http://debezium.io
