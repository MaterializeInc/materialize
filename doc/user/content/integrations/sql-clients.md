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

<img width="1314" alt="Screenshot 2022-12-24 at 18 32 20" src="https://user-images.githubusercontent.com/23521087/209447654-11a51e45-b68f-4e11-8e82-9036d8f7aed8.png">
