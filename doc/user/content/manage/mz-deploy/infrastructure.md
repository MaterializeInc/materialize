---
title: "Infrastructure"
description: "Manage clusters, roles, secrets, connections, sources, and tables with mz-deploy apply."
menu:
  main:
    parent: manage-mz-deploy
    weight: 30
    identifier: "mz-deploy-infrastructure"
    name: "Infrastructure"
---

`mz-deploy apply` converges your infrastructure objects declaratively — it creates what's missing and alters what has drifted. This page covers all object types managed by `apply`.

## Overview

`mz-deploy apply` applies all types in dependency order: clusters → roles → network policies → secrets → connections → sources → tables. Each step is idempotent — running `apply` multiple times converges to the same state.

Preview changes before applying:

```bash
mz-deploy apply --dry-run
```

Skip secrets (useful in CI where secret values aren't available):

```bash
mz-deploy apply --skip-secrets
```

You can also target individual object types with subcommands for granular control:

```bash
mz-deploy apply clusters
mz-deploy apply secrets
```

## Clusters

```sql
-- clusters/orders.sql
CREATE CLUSTER orders (SIZE = 'xsmall');
```

`apply` creates missing clusters and alters drifted configuration. Grants and comments are applied idempotently.

## Roles

```sql
-- roles/order_reader.sql
CREATE ROLE order_reader;
```

## Secrets

Secret values use client-side provider functions that are resolved at `apply` time. This means `compile` works without access to actual secret values.

Use `env_var()` to read values from environment variables:

```sql
-- models/materialize/public/pg_user.sql
CREATE SECRET pg_user AS env_var('PG_USER');
```

```sql
-- models/materialize/public/pg_password.sql
CREATE SECRET pg_password AS env_var('PG_PASSWORD');
```

Alternatively, use `aws_secret()` to pull values from AWS Secrets Manager:

```sql
-- models/materialize/public/pg_password.sql
CREATE SECRET pg_password AS aws_secret('prod/pg-password');
```

`aws_secret()` requires an `aws_profile` in your `project.toml`:

```toml
[profiles.default.security]
aws_profile = "my-aws-profile"
```

`apply secrets` is idempotent — it runs `CREATE SECRET IF NOT EXISTS` then `ALTER SECRET` to update the value.

## Connections

Postgres connection using secrets:

```sql
-- models/materialize/public/pg_conn.sql
CREATE CONNECTION pg_conn TO POSTGRES (
    HOST 'my-postgres.example.com',
    DATABASE 'app',
    USER SECRET pg_user,
    PASSWORD SECRET pg_password,
    SSL MODE 'require'
);
```

## Sources

Postgres source:

```sql
-- models/materialize/public/pg_source.sql
CREATE SOURCE pg_source
IN CLUSTER orders
FROM POSTGRES CONNECTION pg_conn
(PUBLICATION 'mz_source');
```

## Tables

```sql
-- models/materialize/public/orders.sql
CREATE TABLE orders FROM SOURCE pg_source
(REFERENCE public.orders);
```

After `apply tables`, the table's column schema is automatically captured in `types.lock`. This is how `compile` knows what columns `orders` has when type-checking views that reference it. See [Local development — Lock types](/manage/mz-deploy/local-development/#lock-types) for details.
