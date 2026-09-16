---
title: "Row-level security"
description: "Restrict which rows and columns each role can read by combining RBAC, entitlement tables, and security views."
menu:
  main:
    parent: "security"
    name: "Row-level security"
    identifier: "row-level-security"
    weight: 50
---

Materialize enforces row-level and column-level access with three pieces it
already gives you: role-based access control, entitlement tables that map roles
to the rows and columns they may read, and views that join the two. Materialize's
privilege model makes those views a real boundary.

{{< note >}}
{{% include-headless "/headless/rbac-cloud/select-views-privileges" %}}
{{</ note >}}

Privileges stop at the view you name. A role holding `SELECT` on
`secure.orders` reads `secure.orders`. Reaching the relations underneath
requires privileges on those relations.

{{< warning >}}
Materialize views have no `security_barrier` equivalent, so the optimizer, not
the view, decides whether the filter runs before expressions supplied by the
querying role. An expression whose behavior varies with the row it sees can
disclose something about rows the filter removes. Where readers can run
arbitrary SQL, treat the filter as one layer and keep the most sensitive
columns out of the exposed views.
{{</ warning >}}

The result is one view per relation. Onboarding a tenant and widening a profile
are both an `INSERT`.

## Before you start

Confirm privilege checks are active, and know where your login roles come from.

{{< tabs >}}
{{< tab "Cloud" >}}

Materialize Cloud enforces RBAC at all times.

Adding a [user or service account](/security/cloud/users-service-accounts/)
creates a database role named after the email address or service account user.
Those are your login roles.

{{</ tab >}}
{{< tab "Self-managed" >}}

Enable RBAC so that privilege checks are enforced:

{{% include-headless "/headless/rbac-sm/enable-rbac" %}}

Login roles are yours to define. `mz_system` creates them:

{{% include-headless "/headless/rbac-sm/create-users" %}}

{{</ tab >}}
{{</ tabs >}}

## The model

```text
Login role  (alice@acme.example)
     │
     └── GRANT ──► tenant role   (acme_tenant)   ──► row entitlements
                   profile role  (orders_billing) ──► column entitlements
                   reader role   (orders_reader)  ──► SELECT on the exposed view

Every role the session inherits is a key into both entitlement tables.
```

Two tables, both keyed on role name:

| Table                          | Answers                               |
| ------------------------------ | ------------------------------------- |
| `security.row_entitlements`    | Which rows may this role read?         |
| `security.column_entitlements` | Which guarded columns may it read?     |

Three schemas keep the layers apart:

| Schema     | Contents                                                     | Who can use it |
| ---------- | ------------------------------------------------------------ | -------------- |
| `internal` | The maintained relations holding every row                    | The owner |
| `security` | The entitlement tables and the views that apply them          | The owner |
| `secure`   | The views tenants select from                                 | Reader roles |

## Build the layers

### Maintain the data once

Keep the expensive work below the filter, so every tenant reads one maintained
collection. `internal.orders` stands in for whatever holds your rows: a source,
a table, or an upstream view.

```mzsql
CREATE SCHEMA internal;

CREATE VIEW internal.enriched_orders AS
    SELECT id, customer_id, status, total, billing_email, created_at,
           date_trunc('day', created_at) AS order_day
    FROM internal.orders;

CREATE INDEX enriched_orders_by_customer
    ON internal.enriched_orders (customer_id);
```

Index the maintained view on the column the entitlement table keys on. That
turns the per-session filter into a lookup.

### Model entitlements as data

One table per dimension, each keyed and indexed on role name.

```mzsql
CREATE SCHEMA security;

CREATE TABLE security.row_entitlements (
    role_name   text,
    customer_id text
);

CREATE INDEX row_entitlements_by_role
    ON security.row_entitlements (role_name);

INSERT INTO security.row_entitlements VALUES
    ('acme_tenant',   'acme'),
    ('globex_tenant', 'globex');

CREATE TABLE security.column_entitlements (
    role_name   text,
    relation    text,
    column_name text
);

CREATE INDEX column_entitlements_by_role
    ON security.column_entitlements (role_name);

INSERT INTO security.column_entitlements VALUES
    ('orders_billing', 'orders', 'billing_email');
```

List only the columns you guard. Everything else is projected for every reader.

### Resolve the session's roles

`current_role()` returns only the role the session connected as. Entitlements
usually name a shared tenant role, so the filter has to follow role membership.
`pg_has_role` expands it:

```mzsql
CREATE VIEW security.session_roles AS
    SELECT name
    FROM mz_catalog.mz_roles
    WHERE pg_has_role(current_role(), oid, 'USAGE');
```

For a session connected as `alice@acme.example`, which is a member of
`acme_tenant`, which is a member of `orders_reader`:

```nofmt
      name
----------------
 acme_tenant
 alice@acme.example
 orders_reader
```

An entitlement row can name a tenant role or a login role. The same filter
handles both.

### Filter the rows

Join the maintained relation to the entitlement table and keep the rows whose
role the session inherits. This view carries every column, so it stays in
`security` and is never granted.

```mzsql
CREATE VIEW security.entitled_orders AS
    SELECT o.*
    FROM internal.enriched_orders o
    JOIN security.row_entitlements e ON e.customer_id = o.customer_id
    WHERE e.role_name IN (SELECT name FROM security.session_roles);
```

Entitlement rows, read at query time, decide what comes back.

### Mask the columns

Gather the columns the session is entitled to into one array, then guard each
sensitive column with a membership test. The array is a single-row aggregate, so
the cross join costs one row.

```mzsql
CREATE VIEW security.my_columns AS
    SELECT relation, column_name
    FROM security.column_entitlements
    WHERE role_name IN (SELECT name FROM security.session_roles);

CREATE SCHEMA secure;

CREATE VIEW secure.orders AS
WITH allowed AS (
    SELECT array_agg(column_name) AS cols
    FROM security.my_columns WHERE relation = 'orders'
)
SELECT o.id, o.customer_id, o.status, o.total, o.order_day,
       CASE WHEN 'billing_email' = ANY(a.cols) THEN o.billing_email END
           AS billing_email
FROM security.entitled_orders o CROSS JOIN allowed a;
```

A guarded column returns its value to entitled readers and `NULL` to everyone
else. One view serves every profile.

{{< note >}}
The guard fails closed. With no matching entitlements `array_agg` returns
`NULL`, and `'billing_email' = ANY(NULL)` is `NULL`. Prefer `array_agg` over a
construct that returns an empty set, which a membership test reads as "allow".
{{</ note >}}

### Grant the reader role

One view means one grant. Create a reader role, give it schema `USAGE` and
`SELECT`, and grant it to every tenant role. Column profiles are roles too,
carrying entitlement rows instead of privileges.

```mzsql
CREATE ROLE orders_reader;
GRANT USAGE ON SCHEMA secure TO orders_reader;
GRANT SELECT ON secure.orders TO orders_reader;

CREATE ROLE orders_billing;
```

Grant these roles to tenant roles rather than login roles. A role granted to a
tenant role reaches everyone who inherits it.

## Onboard a tenant

Onboarding is grants and inserts. The views stay as they are.

```mzsql
CREATE ROLE initech_tenant;
GRANT orders_reader TO initech_tenant;

INSERT INTO security.row_entitlements VALUES ('initech_tenant', 'initech');

GRANT initech_tenant TO "carol@initech.example";
```

Widening a profile later needs no DDL. Grant the tenant a column profile:

```mzsql
GRANT orders_billing TO initech_tenant;
```

or entitle that tenant to one more column:

```mzsql
INSERT INTO security.column_entitlements
    VALUES ('initech_tenant', 'orders', 'billing_email');
```

The final `GRANT` assumes the login role exists. Where it comes from depends on
your deployment.

{{< tabs >}}
{{< tab "Cloud" >}}

The database role already exists. [Inviting a
user](/security/cloud/users-service-accounts/invite-users/) or [creating a
service account](/security/cloud/users-service-accounts/create-service-accounts/)
creates it, so onboarding a person is the `GRANT` above.

{{< tip >}}
[Sync identity provider
groups](/security/cloud/users-service-accounts/sync-idp-groups/) to database
roles and that `GRANT` follows group membership in your IdP.
{{</ tip >}}

See [Manage database roles](/security/cloud/access-control/manage-roles/).

{{</ tab >}}
{{< tab "Self-managed" >}}

Create the login role as `mz_system` before granting it a tenant role:

```mzsql
CREATE ROLE "carol@initech.example" WITH LOGIN PASSWORD '<password>';
```

The name is arbitrary, so pick a convention and hold to it. Under
[OIDC](/security/self-managed/sso/), roles are provisioned from the identity
provider.

See [Manage database
roles](/security/self-managed/access-control/manage-roles/).

{{</ tab >}}
{{</ tabs >}}

Removing access is symmetric. Delete an entitlement row to take away rows or
columns, or [`REVOKE`](/sql/revoke-role/) the role to take away the account.
Both tables and role membership are read on every query, so these changes apply
to sessions that are already open.

## Verify the boundary

Check both dimensions before you rely on the pattern. Two readers query the
same view. `alice@acme.example` inherits `acme_tenant` and `orders_reader`,
while `bob@globex.example` also inherits `orders_billing`:

```mzsql
SELECT * FROM secure.orders ORDER BY id;
```
```nofmt
-- alice@acme.example
 id | customer_id | status  | total |       order_day        | billing_email
----+-------------+---------+-------+------------------------+---------------
  1 | acme        | shipped |   120 | 2026-09-01 00:00:00+00 |
  3 | acme        | open    | 45.25 | 2026-09-03 00:00:00+00 |

-- bob@globex.example
 id | customer_id | status | total |       order_day        |   billing_email
----+-------------+--------+-------+------------------------+-------------------
  2 | globex      | open   |  80.5 | 2026-09-02 00:00:00+00 | ap@globex.example
```

Each reader gets their own rows, and `billing_email` carries a value only for
the reader entitled to it. Everything else stays behind its own privileges:

```mzsql
SELECT * FROM internal.enriched_orders;
SELECT * FROM security.entitled_orders;
SELECT * FROM security.column_entitlements;
```
```nofmt
ERROR:  permission denied for SCHEMA "materialize.internal"
DETAIL:  The 'alice@acme.example' role needs USAGE privileges on SCHEMA "materialize.internal"
ERROR:  permission denied for SCHEMA "materialize.security"
DETAIL:  The 'alice@acme.example' role needs USAGE privileges on SCHEMA "materialize.security"
ERROR:  permission denied for SCHEMA "materialize.security"
DETAIL:  The 'alice@acme.example' role needs USAGE privileges on SCHEMA "materialize.security"
```

## Keep the filter fast

Materialize evaluates `secure.orders` per query and answers it from indexes
maintained underneath, so one view serves every tenant and every profile from
shared state. Keep the maintained work below the filter:

* Index the maintained relation on the entitlement key, as
  `enriched_orders_by_customer` does above.
* Index both entitlement tables on `role_name`.
* Build the indexes on the cluster that serves tenant queries, and grant that
  cluster's `USAGE` to the reader role.

{{< note >}}
Indexes and materialized views maintain one result for all readers, so the
per-session filter stays out of them:

```mzsql
CREATE INDEX secure_orders_idx ON secure.orders (customer_id);
```
```nofmt
ERROR:  cannot materialize call to current_user
```

Index the inputs instead, and the filter stays a per-session lookup over
them.
{{</ note >}}

[`EXPLAIN`](/sql/explain-plan/) confirms the indexes are doing the work:

```mzsql
EXPLAIN OPTIMIZED PLAN FOR SELECT * FROM secure.orders WHERE customer_id = 'acme';
```
```nofmt
 Used Indexes:
   - materialize.internal.enriched_orders_by_customer (differential join)
   - materialize.security.row_entitlements_by_role (differential join)
   - materialize.security.column_entitlements_by_role (differential join)
```

## Considerations

### Role names share one namespace

`security.session_roles` returns every role the session inherits, including the
reader role. An entitlement naming `orders_reader` therefore reaches every
tenant that reads through it. That is how you grant a baseline to everyone, and
how you leak one tenant to everyone, so decide which you mean. Keep tenant
roles, column profiles, and the reader role distinct, and restrict `INSERT` on
both entitlement tables to a controlled process.

### The view owner's privileges are the ones that matter

The owner of a security view runs its definition for every reader, so the owner
must hold `SELECT` and `USAGE` on everything it touches. Transfer ownership
elsewhere and the error names the owner rather than the caller:

```nofmt
ERROR:  permission denied for SCHEMA "materialize.internal"
DETAIL:  The 'weak_owner' role needs USAGE privileges on SCHEMA "materialize.internal"
```

### Superusers read through the boundary

Superusers are exempt from privilege checks on user objects, so the pattern
governs tenants and leaves administrators unrestricted.

{{< tabs >}}
{{< tab "Cloud" >}}

[Organization admins](/security/cloud/users-service-accounts/#organization-roles)
are superusers and read through every security view. Reserve that role for
people already trusted with all of the data.

{{</ tab >}}
{{< tab "Self-managed" >}}

`mz_system` is a superuser, and `enable_rbac_checks` decides whether everyone
else is one too. Treat that parameter as part of the boundary and verify it
after any change to system configuration:

```mzsql
SHOW enable_rbac_checks;
```

{{</ tab >}}
{{</ tabs >}}

### A masked column reads as null

A guarded column returns `NULL` when the reader has no entitlement, and the
column name stays in the result either way. Where that ambiguity matters,
publish a companion boolean built from the same array, or give that audience a
separate view that omits the column.

### Catalog metadata stays visible

The boundary governs data. A tenant role can still query
`mz_catalog.mz_roles` and `mz_catalog.mz_objects` to list other tenants' role
names and your object names, and can run [`SHOW
CREATE VIEW`](/sql/show-create-view/) on views it selects from. Choose names
that are safe to share.

## See also

* [Access control (Cloud)](/security/cloud/access-control/)
* [Access control (self-managed)](/security/self-managed/access-control/)
* [Appendix: Privileges](/security/appendix/appendix-privileges/)
* [`GRANT PRIVILEGE`](/sql/grant-privilege/)
* [`GRANT ROLE`](/sql/grant-role/)
* [`CREATE VIEW`](/sql/create-view/)
