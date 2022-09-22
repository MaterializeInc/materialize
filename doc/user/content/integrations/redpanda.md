---
title: "How to use Redpanda with Materialize"
description: "Get details about using Materialize with Redpanda"
aliases:
  /third-party/redpanda/
menu:
  main:
    parent: "integration-guides"
    name: "Redpanda"
    weight: 10
---

Because [Redpanda](https://vectorized.io/) is Kafka API-compatible, Materialize can process data from it in the same way it processes data from Kafka sources.

## Configuration

Two configuration parameters that are enabled by default in Kafka need to be enabled explicitly in Redpanda:

```nofmt
--set redpanda.enable_transactions=true
--set redpanda.enable_idempotence=true
```

For more information on general Redpanda configuration, see the [Redpanda documentation](https://vectorized.io/docs/configuration/).

## Related pages

- [`CREATE SOURCE`](/sql/create-source/kafka/)
- [`CREATE SINK`](/sql/create-sink/#kafka-sinks)
