---
title: Tools and Integrations
description: Third-party tools and integrations supported by Materialize
doc_type: overview
product_area: General
audience: developer
status: stable
complexity: beginner
keywords:
  - integrations
  - tools
  - CLI
  - SQL clients
  - PostgreSQL compatible
  - wire compatible
canonical_url: https://materialize.com/docs/integrations/
---

# Tools and Integrations

## Purpose
This section covers third-party tools and integrations supported by Materialize. Materialize is wire-compatible with PostgreSQL and works with many standard PostgreSQL tools and clients.

If you're looking to connect external tools to Materialize, start here.

## When to use
- Connecting SQL clients or BI tools
- Using the Materialize CLI
- Setting up programmatic access via client libraries
- Debugging with mz-debug

## Materialize Tools

- [mz - Materialize CLI](cli/index.md) — Command-line interface for Materialize
  - [CLI Reference](cli/reference/index.md)
  - [App Passwords](cli/reference/app-password/index.md)
  - [Config](cli/reference/config/index.md)
  - [Profile](cli/reference/profile/index.md)
  - [Region](cli/reference/region/index.md)
  - [Secret](cli/reference/secret/index.md)
  - [SQL](cli/reference/sql/index.md)
  - [User](cli/reference/user/index.md)
- [mz-debug](mz-debug/index.md) — Debug and troubleshooting tool
  - [Emulator](mz-debug/emulator/index.md)
  - [Self-managed](mz-debug/self-managed/index.md)

## SQL Clients

- [SQL Clients](sql-clients/index.md) — Compatible PostgreSQL clients (psql, DBeaver, DataGrip, etc.)

## Client Libraries

- [Client Libraries](client-libraries/index.md) — Language-specific drivers and SDKs

## APIs

- [HTTP API](http-api/index.md) — Connect via HTTP
- [WebSocket API](websocket-api/index.md) — Connect via WebSocket

## Connection Pooling

- [Connection Pooling](connection-pooling/index.md) — Use PgBouncer or similar tools

## Foreign Data Wrapper

- [Foreign Data Wrapper (FDW)](fdw/index.md) — Query Materialize from PostgreSQL

## LLM Integration

- [MCP Server](llm/index.md) — Model Context Protocol for LLM access

## Key Takeaways

- Materialize is wire-compatible with PostgreSQL
- Use standard PostgreSQL clients (psql, DBeaver, etc.) to connect
- The `mz` CLI provides Materialize-specific commands
- Client libraries are available for most programming languages
