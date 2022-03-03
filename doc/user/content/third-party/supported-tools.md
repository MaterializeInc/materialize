---
title: "Supported Tools Overview"
description: "Get details about third-party tool support with Materialize"
menu:
  main:
    parent: 'third-party'
weight: 1
---

## Production-level support

| Tool | Purpose |
|------|---------|
| [Docker](/third-party/docker) | Easily deploy Materialize and other required infrastructure.
| [Debezium](/third-party/debezium) | Propagate change data capture (CDC) data from an upstream database to Materialize.


## Beta-level support

Beta-level support means that there may be small performance issues and minor missing features, but Materialize supports the major use cases for this tool. You can file bug reports or feature requests for Materialize integration with these tools [here](https://github.com/MaterializeInc/materialize).

| Tool                    | Purpose                                                                    | What's missing?                                                                                                            |
| ----------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| [dbt](/third-party/dbt) | Create views and materialized views in your Materialize instance using dbt | Full `dbt-materialize` adapter support is a [work in progress](https://github.com/MaterializeInc/materialize/issues/5462). |
| [Redpanda](/third-party/redpanda) | Set up Redpanda as a data source. | Kafka offsets are ignored.
| [Metabase](/third-party/metabase) | Create business intelligence dashboards on top of your Materialize data | Connect Metabase to Materialize using the **PostgreSQL** database type. See [using Metabase](/third-party/metabase) page for more details.

<!--
## Alpha-level support

Alpha-level support means that some of our community members have made this integration work, but we haven't tested it ourselves and can't guarantee its stability.

| Tool | Purpose | What's missing? |
|------|---------|---------|
-->
