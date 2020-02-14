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
**Database** | `materialize`
**Port** | `6875`
**SSL** | Not supported yet

### Supported tools

Tool | Description | Install
-----|-------------|--------
`mzcli` | Materialize adaptation of `pgcli` | [GitHub](https://github.com/MaterializeInc/mzcli#quick-start)
`psql` | Vanilla PostgreSQL CLI tool. Useful for executing queries from scripts using `-c`. | `libpq` or `postgresql-client`
`pgcli` | CLI tool with auto-completion & syntax highlighting | [`pgcli` documentation](https://www.pgcli.com/install)

## Examples

### `mzcli` example

You can connect to `materialized` with `mzcli` using:

```shell
mzcli -h <host>
```

### `psql` example

You could use any of the following formats to connect to `materialized` with `psql`:

```shell
psql postgres://<host>:6875/materialize
psql -h <host> -p 6875 materialize
psql -h <host> -p 6875 -d materialize
psql host=<host>,port=6875,dbname=materialize
```
