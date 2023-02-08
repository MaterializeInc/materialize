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
    weight: 5
    name: "SQL clients"
---

Materialize is **wire-compatible** with PostgreSQL, which means it integrates with most SQL clients that support PostgreSQL (see [Tools and Integrations](/integrations/#sql-clients)). In this guide, we’ll cover how to connect to your Materialize region using common SQL clients.

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

To connect to Materialize using [DBeaver](https://dbeaver.com/docs/wiki/), follow the documentation to [create a connection](https://dbeaver.com/docs/wiki/Create-Connection/) and use the **PostgreSQL database driver** with the credentials provided in the UI for your Materialize region.

<img width="1314" alt="DBeaver Materialize Connection Details" src="https://user-images.githubusercontent.com/23521087/209447654-11a51e45-b68f-4e11-8e82-9036d8f7aed8.png">

## DataGrip

To connect to Materialize using [DataGrip](https://www.jetbrains.com/help/datagrip/connecting-to-a-database.html), follow the documentation to [create a connection](https://www.jetbrains.com/help/datagrip/connecting-to-a-database.html) and use the **PostgreSQL database driver** with the credentials provided in the UI for your Materialize region.

<img width="1131" alt="DataGrip Materialize Connection Details" src="https://user-images.githubusercontent.com/21223421/217556108-e61ff518-a475-489a-b382-c8caf4fecbf4.png">

As some [system tables](https://github.com/MaterializeInc/materialize/issues/9720) are not yet implemented in Materialize, turn off automatic database introspection in DataGrip:

- Go to the data source properties
- Click the _Options_ tab
- Uncheck _Auto sync_ option and click _OK_
