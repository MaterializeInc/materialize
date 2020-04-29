---
title: "CLI Connections"
description: "You can connect to Materialize from your favorite shell using compatible tools, like mzcli or psql."
menu:
  main:
    parent: "connections"
---

You can connect to a running `materialized` process from your favorite shell
using a compatible command-line client.

### Connection details

Detail | Info
-------|------
**Database** | `materialize`
**Port** | `6875`
**SSL** | Not supported yet

### Supported tools

Tool | Description | Install
-----|-------------|--------
`mzcli` | Materialize-specific CLI | [GitHub](https://github.com/MaterializeInc/mzcli#quick-start)
`psql` | Vanilla PostgreSQL CLI tool | `postgresql` or `postgresql-client`

{{< warning >}}
Not all features of `psql` are supported by Materialize.
{{< /warning >}}

Other tools built for PostgreSQL can often be made to work with Materialize with
minor modifications, but are unlikely to work out of the box.
[File a GitHub issue](https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md)
if there is a PostgreSQL tool you would like us to consider supporting.

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
psql host=<host> port=6875 dbname=materialize
```
