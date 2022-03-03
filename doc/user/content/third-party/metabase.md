---
title: "Using Metabase"
description: "Get details about using Materialize with Metabase"
menu:
  main:
    parent: 'third-party'
---

You can use [Metabase] to create business intelligence dashboards using the
real-time data streams in your Materialize instance.

{{< version-changed v0.11.0 >}}
Materialize no longer requires a patched version of Metabase. Install the
official release or use Metabase Cloud.
{{< /version-changed >}}

## Database connection details

Use the following parameters to connect Metabase to your Materialize instance:

Field             | Enter...
----------------- | ----------------
Database type     | **PostgreSQL**
Name              | **Materialize**
Host              | The hostname of the machine running Materialize.<br>Use **localhost** if Metabase and Materialize are running on the same machine.
Port              | **6875**
Database name     | Usually **materialize**.
Database username | Usually **materialize**.
Database password | Leave empty.

If your Materialize instance requires clients to [authenticate with TLS](/cli/#tls-encryption), see the Metabase documentation about
[Securing database connections using an SSL certificate][metabase-tls].

## What's missing?

{{< warning >}}
Materialize does not offer production-level support for Metabase.
{{< /warning >}}

Visualizing a table which contains a [`list`](/sql/types/list) or
  [`record`](/sql/types/record) column will fail {{% gh 9374 9375 %}}.

[Metabase]: https://www.metabase.com/
[metabase-tls]: https://www.metabase.com/docs/latest/administration-guide/secure-database-connections-with-ssl-certificates.html
