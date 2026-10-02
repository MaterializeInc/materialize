---
title: "Protect sensitive columns"
description: "Expose a materialized view that excludes sensitive columns, such as PII, and grant roles access to only that view."
menu:
  main:
    parent: "security-patterns"
    weight: 10
---

To keep sensitive data, such as personally identifiable information (PII), away
from users, consider creating bespoke materialized views which exclude those
columns. Then, grant users access only to the bespoke materialized views.

{{< warning >}}
This pattern is not a strong security barrier. An error in any object upstream
of the exposed materialized view, including an intermediate materialized view,
reaches readers of the exposed view, and the error message can contain
sensitive values. See [Limitations](#limitations).
{{</ warning >}}

This guide uses the following setup:

- **Data:** `restricted.customers` (raw data) => `restricted.customers_enriched`
  (materialized view) => `analytics.customers_public` (materialized view that
  excludes sensitive columns).
- **Roles:** `admin` can read every object. `developer` can read only
  `analytics.customers_public`.

## Before you start

- In Self-Managed Materialize, [enable RBAC](/security/self-managed/access-control/#enabling-rbac).
  Without RBAC, all users are superusers. Materialize Cloud always enforces
  RBAC.
- Create the login roles that will receive access. In Materialize Cloud,
  [invite the users or create service accounts](/security/cloud/users-service-accounts/).
  In Self-Managed Materialize, create them with
  [`CREATE ROLE ... WITH LOGIN PASSWORD`](/sql/create-role/).
- Run the steps below as a role that can create roles and schemas, such as a
  superuser or Organization Admin.

## Step 1. Create the schemas

Keep the raw data and intermediate objects in one schema, and the objects you
expose in another. The `developer` role never gets `USAGE` on the first schema.

```mzsql
CREATE SCHEMA restricted;
CREATE SCHEMA analytics;
```

## Step 2. Create the raw data

In production this is usually a [source](/sql/create-source/). This guide uses a
table:

```mzsql
CREATE TABLE restricted.customers (
  id int, name text, email text, ssn text, region text, plan text
);

INSERT INTO restricted.customers VALUES
  (1, 'Ada Lovelace', 'ada@example.com',   '123-45-6789', 'EU', 'pro'),
  (2, 'Alan Turing',  'alan@example.com',  '987-65-4321', 'EU', 'free'),
  (3, 'Grace Hopper', 'grace@example.com', '555-12-3456', 'US', 'pro');
```

## Step 3. Create the materialized views

Create the intermediate materialized view in `restricted`, and the exposed one
in `analytics`. The exposed view selects only non-sensitive columns.

```mzsql
CREATE MATERIALIZED VIEW restricted.customers_enriched AS
  SELECT id, name, email, region, plan, right(ssn, 4) AS ssn_last4
  FROM restricted.customers;

CREATE MATERIALIZED VIEW analytics.customers_public AS
  SELECT id, region, plan
  FROM restricted.customers_enriched;
```

Use a materialized view, not a view, for the exposed object. A query against a
materialized view reads only its stored results. A query against a view is
optimized together with the view's definition, so whether an upstream error
surfaces can depend on the reader's query.

## Step 4. Create the roles and grant privileges

```mzsql
CREATE ROLE admin;
CREATE ROLE developer;

GRANT USAGE ON SCHEMA restricted, analytics TO admin;
GRANT SELECT ON ALL TABLES IN SCHEMA restricted, analytics TO admin;

GRANT USAGE ON SCHEMA analytics TO developer;
GRANT SELECT ON analytics.customers_public TO developer;
```

{{% include-headless "/headless/rbac-cloud/grant-privilege-all-tables" %}}
It covers only objects that exist when you run it. To cover objects you create
later, use [`ALTER DEFAULT PRIVILEGES`](/sql/alter-default-privileges/).

To run queries, a role also needs `USAGE` on a cluster. By default, `PUBLIC` has
`USAGE` on the `quickstart` cluster. For another cluster, grant it explicitly:

```mzsql
GRANT USAGE ON CLUSTER <cluster_name> TO admin, developer;
```

## Step 5. Grant the roles to users

Grant each functional role to the login roles that need it. In Materialize
Cloud, login roles are named after the user's email address or the service
account user.

```mzsql
GRANT admin TO "admin@example.com";
GRANT developer TO "dev@example.com";
```

## Verify

Connect as `dev@example.com` (a member of `developer`) and query the exposed
materialized view:

```mzsql
SELECT * FROM analytics.customers_public ORDER BY id;
```

```nofmt
 id | region | plan
----+--------+------
  1 | EU     | pro
  2 | EU     | free
  3 | US     | pro
```

Querying the restricted objects fails:

```mzsql
SELECT * FROM restricted.customers;
```

```nofmt
ERROR:  permission denied for SCHEMA "materialize.restricted"
DETAIL:  The 'dev@example.com' role needs USAGE privileges on SCHEMA "materialize.restricted"
```

## Limitations

### Upstream errors reach the exposed view

An error in any upstream object, such as a source or an intermediate
materialized view, propagates to every materialized view downstream of it. This
happens even when the downstream view does not select the column that caused
the error, and the error message can include the sensitive value.

For example, suppose the intermediate view casts `ssn` to a number, and a row
with a malformed SSN arrives:

```mzsql
CREATE MATERIALIZED VIEW restricted.customers_parsed AS
  SELECT id, region, plan, replace(ssn, '-', '')::bigint AS ssn_num
  FROM restricted.customers;

CREATE MATERIALIZED VIEW analytics.customers_public_v2 AS
  SELECT id, region, plan FROM restricted.customers_parsed;

GRANT SELECT ON analytics.customers_public_v2 TO developer;

INSERT INTO restricted.customers VALUES
  (4, 'Katherine Johnson', 'kj@example.com', '321-54-98X6', 'US', 'pro');
```

A member of `developer` who queries the exposed view sees the SSN in the error:

```mzsql
SELECT * FROM analytics.customers_public_v2;
```

```nofmt
ERROR:  Evaluation error: invalid input syntax for type bigint: invalid digit found in string: "3215498X6"
```

To reduce this risk, guard expressions on sensitive columns that can fail. For
example,
`CASE WHEN ssn ~ '^\d{3}-\d{2}-\d{4}$' THEN replace(ssn, '-', '')::bigint END`
returns `NULL` for a malformed value instead of an error.

Source errors, such as decoding errors, cannot be guarded this way.

To clean up the example, drop its views and the malformed row:

```mzsql
DROP MATERIALIZED VIEW restricted.customers_parsed CASCADE;
DELETE FROM restricted.customers WHERE id = 4;
```

### Metadata is visible to all roles

Any role can read the [system catalog](/sql/system-catalog/). This
includes the names of the restricted objects and their columns (for example, in
`mz_columns`), and the SQL definitions of objects (for example, in
`mz_materialized_views.create_sql`). Keep sensitive values out of object
definitions.
