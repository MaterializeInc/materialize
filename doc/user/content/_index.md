---
title: "Materialize Docs"
htmltitle: "Home"
disable_toc: true
disable_list: true
disable_h1: true
weight: 1
aliases:
  - /self-managed/v25.1/
---

# Materialize: The live data layer for apps and agents

Materialize lets you transform siloed operational data into live context, using just SQL.

[Get started →](/get-started/quickstart/)

## How Materialize works

### Real-time ingestion from sources

Materialize connects natively to your operational systems — Postgres, MySQL, Kafka, webhooks — and ingests changes as they happen using Change Data Capture (CDC). Data flows in continuously; there are no batch windows or scheduled pulls.

### Incremental view maintenance

As new data arrives, Materialize updates view results incrementally rather than recalculating from scratch. Complex SQL — multi-way joins, aggregations, temporal filters — stays fresh within about a second. You write SQL once; Materialize handles the maintenance.

### Standard SQL & PGWire support

Materialize speaks standard SQL and the PostgreSQL wire protocol. Any application, dashboard, or BI tool that works with Postgres works with Materialize — no new query language, no new SDK. AI agents query the live context graph through the same SQL surface.

### Consistency guaranteed

By default, Materialize provides strict serializability: every query sees a consistent snapshot that respects real-time write ordering. No eventual consistency, no dual-write races, no stale reads. You can [tune the isolation level](/get-started/isolation-level/) depending on your performance requirements.

## Materialize offerings

{{% include-headless "/headless/materialize-intro/offerings" %}}

{{< callout >}}
## What's new!

- **MCP servers and Agent skills**:
  - [MCP server for agents: give your production AI agents fresh context from
    Materialize](/releases/#mcp-server-for-agents)
  - [MCP server for developers: give coding agents observability into your
    Materialize environment](/releases/#mcp-server-for-developers)
  - [Agent skills](/integrations/coding-agent-skills/)

For more information on these and other changes, see the [Release Notes](/releases/).

{{</ callout >}}
