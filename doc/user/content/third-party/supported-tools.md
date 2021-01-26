---
title: "Supported Tools Overview"
description: "Get details about third-party tool support with Materialize"
menu:
  main:
    parent: 'third-party'
---

Materialize fully supports the following third-party tools:

| Tool | Purpose |
|------|---------|
| [Docker](docker.md) | Easily deploy Materialize and other required infrastructure.
| [Debezium](debezium.md) | Propagate change data capture (CDC) data from an upstream database to Materialize.


Materialize partially supports the following third-party tools:

| Tool | Purpose | What's missing? |
|------|---------|---------|
| [dbt](dbt.md)  | Create views and materialized views in your Materialize instance using dbt | Full `dbt-materialize` adapter support is a [work in progress](https://github.com/MaterializeInc/materialize/issues/5462).
| [metabase](metabase.md) | Create business intelligence dashboards on top of your Materialize data | Running Metabase cleanly, without our forked changes, requires further [pgjdbc support](https://github.com/MaterializeInc/materialize/issues/3727).
