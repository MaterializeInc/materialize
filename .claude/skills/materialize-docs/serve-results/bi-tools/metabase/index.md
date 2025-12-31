---
audience: developer
canonical_url: https://materialize.com/docs/serve-results/bi-tools/metabase/
complexity: beginner
description: How to create real-time dashboards with Metabase
doc_type: reference
keywords:
- CREATE REAL
- materialize
- PostgreSQL
- '6875'
- Metabase
product_area: Sinks
status: stable
title: Metabase
---

# Metabase

## Purpose
How to create real-time dashboards with Metabase

If you need to understand the syntax and options for this command, you're in the right place.


How to create real-time dashboards with Metabase


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

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: alter-cluster/configure-cluster --> --> -->

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