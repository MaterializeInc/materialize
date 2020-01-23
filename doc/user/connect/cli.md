---
title: "CLI Connections"
description: "You can connect to Materialize from your favorite shell using Postgres-compatible tools, like psql or pgcli."
menu:
  main:
    parent: "connections"
---

You can connect to a running `materialized` process from your favorite shell using Postgres-compatible tools.

### Connection details

Detail | Info
-------|------
**Port** | 6875
**SSL** | Not supported yet; use `sslmode=disable`

### Supported tools

Tool | Description | Install
-----|-------------|--------
`mzcli` | Materialize adaptation of `pgcli` | [GitHub](https://github.com/MaterializeInc/mzcli#quick-start)
`psql` | Vanilla PostgreSQL CLI tool. Useful for executing queries from scripts using `-c`. | `libpq` or `postgresql-client`
`pgcli` | CLI tool with auto-completion & syntax highlighting | [`pgcli` documentation](https://www.pgcli.com/install)
