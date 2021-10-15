---
title: "Using Redpanda"
description: "Get details about using Materialize with Redpanda"
menu:
  main:
    parent: 'third-party'
---

{{< beta >}}
v0.9.9
{{< /beta >}}

Because [Redpanda] is Kafka API-compatible, Materialize can process data from it in the same way it processes data from Kafka sources.

## What's missing?

You can use most of the Kafka source options for Redpanda as a Kafka broker, with a few exceptions:

- The Protobuf schema registry isn't supported for Redpanda sources.
- The `start_offset` option may not work properly [Redpanda issue #2397](https://github.com/vectorizedio/redpanda/issues/2397).

## Redpanda setup

Two settings that are enabled by default in Kafka will need be enabled explicitly in Redpanda for `redpanda start`:

```nofmt
--set "redpanda.enable_transactions=true"
--set "redpanda.enable_idempotence=true"
```

For more information on general Redpanda configuration, see the [Redpanda documentation](https://vectorized.io/docs/configuration/).

[Redpanda]: https://vectorized.io/

## Related pages
- [`CREATE SOURCE`](/sql/create-source/)
