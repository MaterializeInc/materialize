---
title: "SQL clients"
description: "How to connect to Materialize using common SQL clients"
aliases:
  - /connect/
  - /connect/cli/
  - /integrations/psql/
menu:
  main:
    parent: "integrations"
    weight: 10
    name: "SQL clients"
---

Materialize is **wire-compatible** with PostgreSQL, which means it integrates
with many SQL clients that support PostgreSQL (see [Tools and Integrations](/integrations/#sql-clients)).
In this guide, we’ll cover how to connect to your Materialize region using
common SQL clients.

## Connection parameters

You can find the credentials for your Materialize region in the
[Materialize console](https://console.materialize.com/), under **Connect
externally** in the navigation bar.

Field             | Value
----------------- | ----------------
Host              | Materialize host name.
Port              | **6875**
Database name     | Database to connect to (default: **materialize**).
Database username | Materialize user.
Database password | App password for the Materialize user.
SSL mode          | **Require**

Before connecting, double-check that you've created an **app-password** for your
user. This password is auto-generated, and prefixed with `mzp_`.

### Runtime connection parameters

{{< warning >}}
Parameters set in the connection string work for the **lifetime of the
session**, but do not affect other sessions. To permanently change the default
value of a session variable for a specific user (i.e. role), use the
[`ALTER ROLE...SET`](/sql/alter-role) command.
{{< /warning >}}

You can pass runtime connection parameters (like `cluster`, `isolation_level`,
or `search_path`) to Materialize using the [`options` connection string
parameter](https://www.postgresql.org/docs/14/libpq-connect.html#LIBPQ-PARAMKEYWORDS),
or the [`PGOPTIONS` environment variable](https://www.postgresql.org/docs/16/config-setting.html#CONFIG-SETTING-SHELL).
As an example, to specify a different cluster than the default defined for the
user and set the transactional isolation to serializable on connection using
`psql`:

```bash
# Using the options connection string parameter
psql "postgres://<MZ_USER>@<MZ_HOST>:6875/materialize?sslmode=require&options=--cluster%3Dprod%20--transaction_isolation%3Dserializable
```

```bash
# Using the PGOPTIONS environment variable
PGOPTIONS='--cluster=prod --transaction_isolation=serializable' \
psql \
    --username=<MZ_USER> \
    --host=<MZ_HOST> \
    --port=6875 \
    --dbname=materialize
```

## Tools

### DataGrip

{{< note >}}
As we work on extending the coverage of `pg_catalog` in Materialize,
some DataGrip features might not work as expected {{% gh 9720 %}}.
{{< /note >}}

To connect to Materialize using [DataGrip](https://www.jetbrains.com/help/datagrip/connecting-to-a-database.html),
follow the documentation to [create a connection](https://www.jetbrains.com/help/datagrip/connecting-to-a-database.html)
and use the **PostgreSQL database driver** with the credentials provided in the
Materialize console.

<img width="1131" alt="DataGrip Materialize Connection Details" src="https://user-images.githubusercontent.com/21223421/218108169-302c8597-35a9-4dce-b16d-050f49538b9e.png">

### DBeaver

**Minimum requirements:** DBeaver 23.1.3

To connect to Materialize using [DBeaver](https://dbeaver.com/docs/wiki/),
follow the documentation to [create a connection](https://dbeaver.com/docs/wiki/Create-Connection/)
and use the **Materialize database driver** with the credentials provided in the
Materialize console.

<img width="1314" alt="Connect using the credentials provided in the Materialize console" src="https://github.com/MaterializeInc/materialize/assets/23521087/ae98dc45-2e1a-4e78-8ca0-e8d53beed30f">

The Materialize database driver depends on the [PostgreSQL JDBC driver](https://jdbc.postgresql.org/download.html).
If you don't have the driver installed locally, DBeaver will prompt you to
automatically download and install the most recent version.

#### Connect to a specific cluster

By default, Materialize connects to the [pre-installed `default` cluster](/sql/show-clusters/#pre-installed-clusters).
To connect to a specific [cluster](/get-started/key-concepts/#clusters), you must
define a bootstrap query in the connection initialization settings.

<br>

1. Click on **Connection details**.

1. Click on **Connection initialization settings**.

1. Under **Bootstrap queries**, click **Configure** and add a new SQL query that
sets the active cluster for the connection:

    ```sql
    SET cluster = other_cluster;
    ```

Alternatively, you can change the default value of the `cluster` session
variable for a specific user (i.e. role) using the [`ALTER ROLE...SET`](/sql/alter-role)
command.

### PopSQL

[//]: # "TODO(morsapaes) Extend with instructions to use dbt-materialize once we
yank this out as a standalone guide."

To connect to Materialize using [PopSQL](https://popsql.com/), follow the
documentation to [create a Materialize connection](https://docs.popsql.com/docs/connecting-to-materialize)
and use the credentials provided in the Materialize console.

<img width="1552" alt="Screenshot 2023-12-28 at 18 24 35" src="https://github.com/MaterializeInc/materialize/assets/23521087/2e219233-9f6c-4326-8b5e-acdfd49da342">

### TablePlus

{{< note >}}
As we work on extending the coverage of `pg_catalog` in Materialize,
some TablePlus features might not work as expected {{% gh 19891 %}}.
{{< /note >}}

To connect to Materialize using [TablePlus](https://tableplus.com/),
follow the documentation to [create a connection](https://docs.tableplus.com/gui-tools/manage-connections#create-a-connection)
and use the **PostgreSQL database driver** with the credentials provided in the
Materialize console.

<img width="1440" alt="Screenshot 2023-12-28 at 18 45 11" src="https://github.com/MaterializeInc/materialize/assets/23521087/c530769e-3445-49c8-8f36-9f7166352ac4">

### `psql`

{{< warning >}}
Not all features of `psql` are supported by Materialize yet, including some backslash meta-commands {{% gh 9721 %}}.
{{< /warning >}}

{{< tabs >}}
{{< tab "macOS">}}

Start by double-checking whether you already have `psql` installed:

```shell
psql --version
```

Assuming you’ve installed [Homebrew](https://brew.sh/):

```shell
brew install libpq
```

Then symlink the `psql` binary to your `/usr/local/bin` directory:

```shell
brew link --force libpq
```

{{< /tab >}}

{{< tab "Linux">}}

Start by double-checking whether you already have `psql` installed:

```shell
psql --version
```

```bash
sudo apt-get update
sudo apt-get install postgresql-client
```

The `postgresql-client` package includes only the client binaries, not the PostgreSQL server.

For other Linux distributions, check out the [PostgreSQL documentation](https://www.postgresql.org/download/linux/).

{{< /tab >}}

{{< tab "Windows">}}

Start by double-checking whether you already have `psql` installed:

```shell
psql --version
```

Download and install the [PostgreSQL installer](https://www.postgresql.org/download/windows/) certified by EDB.
{{< /tab >}}
{{< /tabs >}}
