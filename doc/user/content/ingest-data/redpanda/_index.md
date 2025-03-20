---
title: "Redpanda"
description: "Get details about using Materialize with Redpanda"
aliases:
  - /third-party/redpanda/
  - /integrations/redpanda/
  - /connect-sources/redpanda/
menu:
  main:
    parent: "redpanda"
    name: "Self-hosted Redpanda"
    weight: 10
---

[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the Postgres ones. We should include spill to disk in the guidance then."

Because [Redpanda](https://www.redpanda.com/) is Kafka API-compatible,
Materialize can process data from it in the same way it processes data from
Kafka sources.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Configuration

Two configuration parameters that are enabled by default in Kafka need to be
enabled explicitly in Redpanda:

```nofmt
--set redpanda.enable_transactions=true
--set redpanda.enable_idempotence=true
```

For more information on general Redpanda configuration, see the
[Redpanda documentation](https://docs.redpanda.com/home/).

## Related pages

- [`CREATE SOURCE`](/sql/create-source/kafka/)
- [`CREATE SINK`](/sql/create-sink/)
