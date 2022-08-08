---
title: "Metabase"
description: "How to connect a Metabase instance to Materialize"
aliases:
  /third-party/metabase/
menu:
  main:
    parent: "integration-guides"
    name: "Metabase"
---

You can use [Metabase](https://www.metabase.com/) to create real-time dashboards based on the data maintained in Materialize.

## Database connection details

To set up a connection from a Metabase instance to Materialize, use the native [PostgreSQL database driver](https://www.metabase.com/docs/latest/administration-guide/databases/postgresql.html) with the following parameters:

Field             | Value
----------------- | ----------------
Database type     | **PostgreSQL**
Host              | Materialize host name.
Port              | **6875**
Database name     | **materialize**
Database username | Materialize user.
Database password | App-specific password.

If you require SSL/TLS encryption, you can configure the connection to use `sslmode=require`. For more details, check out the [Metabase documentation](https://www.metabase.com/docs/latest/administration-guide/databases/postgresql.html).

## Refresh rate

By default, the lowest [refresh rate](https://www.metabase.com/docs/latest/users-guide/07-dashboards.html#auto-refresh) for Metabase dashboards is 1 minute. You can manually set this to a lower interval by adding `#refresh=1` (as an example, for a `1` second interval) to the end of the URL, and opening the modified URL in a new tab.

Because Metabase queries are simply reading data out of self-updating views in Materialize, setting your dashboards to auto-refresh at lower rates will not have a significant impact on database performance.

[//]: # "TODO(morsapaes) Once we revamp quickstarts, add Related pages section pointing to a quickstart that uses Metabase"
