---
title: "Stable APIs"
description: "Build cross-team data products with stable API schemas that never break downstream consumers."
menu:
  main:
    parent: manage-mz-deploy
    weight: 55
    identifier: "mz-deploy-stable-apis"
    name: "Stable APIs"
---

By default, when a materialized view changes, `mz-deploy` recreates it in a
staging schema and swaps the entire schema into production. This works well
within a single project — dependencies are tracked and redeployed
automatically. But consumers in **other** projects break, because the schema
swap drops and recreates the materialized view, severing any downstream
dependencies that reference it.

Stable API schemas solve this problem.

## Marking a schema as stable

Add `SET api = stable` to a [schema modifier](/manage/mz-deploy/project-structure/#schema-modifiers):

```sql
-- models/materialize/ontology.sql
SET api = stable;
```

With this in place, changed materialized views in the `ontology` schema are no
longer dropped and recreated. Instead, `mz-deploy` uses Materialize's
replacement protocol:

```sql
ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT ...
```

The materialized view's computation is updated in place and its identity is
preserved. Downstream consumers — whether in the same project or a different
one — do not need to be redeployed and do not need to know the update happened.

## How it works

You deploy the same way as always — `stage`, `wait`, `promote`. `mz-deploy`
automatically detects which schemas are marked stable and handles them
accordingly. Materialized views in stable schemas are updated in place while
preserving their identity. Everything else deploys normally.

Because the object identity is preserved, a changed stable MV does not
propagate dirtiness to its dependents. Objects that depend on a stable MV are
not redeployed, even if the MV's definition changed. This prevents cascading
redeployments across project boundaries.

## The two-schema pattern

The recommended way to build a stable API is with two schemas: an **internal**
schema for your transformation logic and a **stable** schema that exposes a
clean API surface.

```nofmt
models/materialize/
├── ontology.sql            # SET api = stable
├── ontology/
│   ├── customers.sql       # Thin MV — stable API surface
│   └── orders.sql          # Thin MV — stable API surface
├── internal/
│   ├── customers_cleaned.sql   # View + index — transformation logic
│   └── orders_enriched.sql     # View + index — transformation logic
```

The **internal** schema contains views with indexes that hold all your
transformation logic. These are regular objects deployed via the normal
schema-swap mechanism.

The **stable** schema contains thin materialized views that select from the
internal views. Each MV explicitly lists its columns (never `SELECT *`) and
includes a `COMMENT ON` describing its contract:

```sql
-- models/materialize/ontology/customers.sql
CREATE MATERIALIZED VIEW customers
IN CLUSTER ontology AS
SELECT
    id,
    name,
    email,
    status,
    created_at
FROM internal.customers_cleaned;

COMMENT ON MATERIALIZED VIEW customers IS
    'Canonical customer entity. Columns: id, name, email, status, created_at.';
```

```sql
-- models/materialize/internal/customers_cleaned.sql
CREATE VIEW customers_cleaned AS
SELECT
    id,
    trim(name) AS name,
    lower(email) AS email,
    CASE WHEN active THEN 'active' ELSE 'inactive' END AS status,
    created_at
FROM orders_db.public.raw_customers;

CREATE INDEX customers_cleaned_idx IN CLUSTER ontology
ON customers_cleaned (id);
```

This separation gives you:

- **All logic in the internal schema** — easy to change, tested with unit tests,
  deployed via normal schema swap.
- **A stable API surface** — thin MVs that preserve identity across deployments.
  Other teams depend on these and are never disrupted.
- **Explicit contracts** — column lists and comments define what consumers can
  rely on.

## Building a data mesh

Stable APIs are the foundation for a data mesh architecture in Materialize.
Multiple teams can maintain independent mz-deploy projects that depend on each
other's stable schemas:

```nofmt
Ontology project              Fulfillment project
========================      ========================

internal/                     internal/
  customers_cleaned             shipments_joined
  orders_enriched               delivery_tracking

ontology/ (stable)            references:
  customers          ──────▶    ontology.customers
  orders             ──────▶    ontology.orders
```

The **Ontology project** owns canonical business entities and exposes them
through a stable schema. The **Fulfillment project** builds domain-specific
views on top of the ontology. Each project:

- Has its own git repository and deployment lifecycle.
- Runs on its own cluster for independent scaling.
- Declares cross-project references as
  [external dependencies](/manage/mz-deploy/local-development/#external-dependencies)
  in `project.toml`.

When the Ontology team changes how `customers_cleaned` is computed, they deploy
normally. The `customers` MV in the stable schema is updated in place via the
replacement protocol. The Fulfillment project's views continue working without
redeployment or coordination.

## Constraints

- **Only materialized views** — stable schemas can only contain materialized
  views. Tables, views, sinks, and sources are not supported.
- **No dirtiness propagation** — a changed replacement MV does not mark its
  dependents as dirty. Dependent objects in the same project are not
  redeployed.
- **No new objects in existing stable schemas** — you cannot add a brand-new
  materialized view to a stable schema that already has production objects in a
  single deployment. Deploy the schema for the first time (or add objects when
  the schema is new), then update existing MVs in subsequent deployments.
- **Explicit column lists** — always list columns explicitly in stable MVs
  rather than using `SELECT *`. This makes the API contract visible and
  prevents accidental column additions from propagating to consumers.
