---
title: "CREATE SOURCE: Avro from local file"
description: "Learn how to connect Materialize to an Avro-formatted local file"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro format="[Avro Object Container File](https://avro.apache.org/docs/current/spec.html#Object+Container+Files)" connector="local files" %}}

## Syntax

{{< diagram "create-source-avro-file.html" >}}

{{% create-source/syntax-details connector="file" formats="avro-ocf text" envelopes="debezium append-only" %}}

## Examples

```sql
CREATE SOURCE events
FROM FILE '[path to .ocf]'
WITH (tail = true)
FORMAT AVRO OCF
ENVELOPE NONE;
```

This creates a source that...

- Automatically determines its schema from the OCF file's embedded schema.
- Materialize dynamically checks for new entries.
- Is append-only.

[Debezium]: http://debezium.io
