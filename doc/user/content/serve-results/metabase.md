---
title: "Metabase"
description: "How to create real-time dashboards with Metabase"
aliases:
  - /third-party/metabase/
  - /integrations/metabase/
menu:
  main:
    parent: "bi-tools"
    name: "Metabase"
    weight: 5
---

You can use [Metabase](https://www.metabase.com/) to create real-time dashboards
based on the data maintained in Materialize.

## Database connection details

To set up a connection from Metabase to Materialize, use the native
[PostgreSQL database driver](https://www.metabase.com/docs/latest/administration-guide/databases/postgresql.html)
with the following parameters:

Field             | Value
----------------- | ----------------
Database type     | **PostgreSQL**
Host              | Materialize host name.
Port              | **6875**
Database name     | **materialize**
Database username | Materialize user.
Database password | App-specific password.
SSL mode          | Require

For more details and troubleshooting, check the
[Metabase documentation](https://www.metabase.com/docs/latest/administration-guide/databases/postgresql.html).

## Configure a custom cluster

To direct queries to a specific cluster, [set the cluster at the role level](/sql/alter-role) using the following SQL statement:

```sql
ALTER ROLE <your_user> SET CLUSTER = <custom_cluster>;
```

Replace `<your_user>` with the name of your Materialize role and `<custom_cluster>` with the name of the cluster you want to use.

Once set, all new sessions for that user will automatically run in the specified cluster, eliminating the need to manually specify it in each query or connection.

## Refresh rate

By default, the lowest [refresh rate](https://www.metabase.com/docs/latest/users-guide/07-dashboards.html#auto-refresh)
for Metabase dashboards is 1 minute. You can manually set this to a lower
interval by adding `#refresh=1` (as an example, for a `1` second interval) to
the end of the URL, and opening the modified URL in a new tab.

Because Metabase queries are simply reading data out of self-updating views in
Materialize, setting your dashboards to auto-refresh at lower rates should not
have a significant impact on database performance. To minimize this impact, we
recommend carefully choosing an [indexing strategy](/sql/create-index/)
for any objects serving results to Metabase.

[//]: # "TODO(morsapaes) Once we revamp quickstarts, add Related pages section
pointing to a quickstart that uses Metabase"
