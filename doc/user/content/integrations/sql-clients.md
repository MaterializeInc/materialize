---
title: "SQL clients"
description: "How to connect to Materialize using PostgreSQL-compatible SQL clients"
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

## `psql`

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

## DBeaver

To connect to Materialize using [DBeaver](https://dbeaver.com/docs/wiki/), follow the documentation to [create a connection](https://dbeaver.com/docs/wiki/Create-Connection/) and use the **Materialize database driver** with the credentials provided in the Materialize UI.

<br>

<img width="1314" alt="DBeaver Materialize Connection Details" src="https://github-production-user-asset-6210df.s3.amazonaws.com/21223421/256839946-f13c941d-4857-4c87-b3d3-56ca29a525a2.png">

The Materialize database driver is available in DBeaver 23.1.3 and later and requires the PostgreSQL driver to be installed.

You can download the recent version of PostgreSQL JDBC Driver from PostgreSQL's [official website](https://jdbc.postgresql.org/download.html).

### Connect to a specific cluster

To connect to a specific [cluster](/get-started/key-concepts/#clusters) in Materialize, define a bootstrap query in the connection settings.

For example, to connect to the `materialize` cluster, go to the **Connection Settings**, click on **Initialization**, click on **Bootstrap Query** and enter the following query:

```sql
SET cluster = 'materialize';
```

<img width="1314" alt="DBeaver Materialize Specify Cluster" src="https://github-production-user-asset-6210df.s3.amazonaws.com/21223421/256840992-13a1556f-94b0-4f7f-88b5-72700cc2a0e5.png">


## DataGrip

To connect to Materialize using [DataGrip](https://www.jetbrains.com/help/datagrip/connecting-to-a-database.html), follow the documentation to [create a connection](https://www.jetbrains.com/help/datagrip/connecting-to-a-database.html) and use the **PostgreSQL database driver** with the credentials provided in the Materialize UI.

{{< note >}}
As we work on extending the coverage of `pg_catalog` in Materialize {{% gh 9720 %}}, you must [turn off automatic database introspection](https://intellij-support.jetbrains.com/hc/en-us/community/posts/360010694760/comments/360003100820) in DataGrip to connect.
{{< /note >}}

<img width="1131" alt="DataGrip Materialize Connection Details" src="https://user-images.githubusercontent.com/21223421/218108169-302c8597-35a9-4dce-b16d-050f49538b9e.png">
