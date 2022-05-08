---
title: "Connect to Materialize via a SQL CLI"
description: "How to connect to Materialize using PostgreSQL-compatible tools"
aliases:
  - /connect/
  - /connect/cli/
  - /integrations/psql/
menu:
  main:
    parent: "integrations"
    weight: 5
    name: "SQL clients"
---

You can connect to a running Materialize instance using a PostgreSQL-compatible client, like `psql`. For an overview of compatible SQL clients and their current level of support, check out [Tools and Integrations](/integrations/#sql-clients).

### Connection details

Detail       | Info
-------------|------
**Database** | `materialize`
**User**     | Any valid [role](/sql/create-role) (default: `materialize`)
**Port**     | `6875`
**SSL/TLS**  | [If enabled](/cli/#tls-encryption)

Materialize instances have a default user named `materialize` installed. You can manage users using the [`DROP USER`](/sql/drop-user) and [`CREATE USER`](/sql/create-user) statements.

### Supported tools

Tool     | Description                   | Install
---------|-------------------------------|-----------------------------------
`psql`   | Vanilla PostgreSQL CLI        | `postgresql` or `postgresql-client`
 DBeaver | Open source universal SQL CLI | [Download DBeaver](https://dbeaver.io/download/)

If there's a tool that you'd like to use with Materialize but is not listed here or in [Tools and Integrations](/integrations/#sql-clients), let us know by submitting a [feature request](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)!

## Examples

### Connecting with `psql`

{{< warning >}}
Not all features of `psql` are supported by Materialize yet, including some backslash meta-commands {{% gh 9721 %}}.
{{< /warning >}}

You can use any of the following connection string format to connect to `materialized` with `psql`:

```shell
psql postgres://materialize@<host>:6875/materialize
psql -U materialize -h <host> -p 6875 materialize
psql -U materialize -h <host> -p 6875 -d materialize
psql user=materialize host=<host> port=6875 dbname=materialize
```

#### Docker

For Docker environments, we provide the [`materialize/cli` image](https://hub.docker.com/r/materialize/cli), which bundles `psql` and can be used to spin up a minimal `docker-compose` setup:

```yaml
services:
  materialized:
    image: materialize/materialized:{{< version >}}
    ports:
      - "6875:6875"
  cli:
    image: materialize/cli:{{< version >}}
```
