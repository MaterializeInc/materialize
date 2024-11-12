---
title: "Hydration"
description: "Learn about hydration in Materialize."
menu:
  main:
    parent: 'concepts'
    weight: 50
    identifier: 'concepts-hydration'
---

Hydration refers to the process of populating sources, materialized views, and
indexes using a snapshot of the data from the upstream source. 

## Hydration occurences

Hydration occurs:

- **When you create a source**.

  - During this hydration, queries that use the hydrating objects will block
    until the objects are hydrated.

- **When a cluster restarts** (such as during [cluster
  resizing](/sql/alter-cluster/#resizing) or Materialize's [weekly
  upgrades](/releases/#schedule)).

  - During this hydration (also referred to as rehydration),
  
    - If using the default *strict serializable* isolation, queries that use the
      hydrating objects will block until the objects are hydrated.

    - If using *serializable* isolation, queries can use the hydrating objects
      but with stale data.

Generally, the hydration time for an object is proportional to data volume and
query complexity. For sources with upsert envelope, the initial hydration may
take longer while the upsert envelope effectively dedupes your records.

## Hydration status

You can view the hydration status of an object in the [Materialize
console](/console/). For an object, go to its **Workflow** page in the
[Database object explorer](/console/data/).


Alternatively, you can query the following system catalog views:

- [`mz_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_hydration_statuses)

- [`mz_compute_hydration_statuses`](/sql/system-catalog/#mz_compute_hydration_statuses/)

## Memory considerations for hydration

- Materialized views require memory proportional to ~2x size of the output
  during hydration (to recalculate the output from scratch and compare with
  existing output to ensure correctness).

- Sinks need to load an entire snapshot of the data in memory when they start
  up.

For example, for clusters that contain the materialized views and the sinks from
those materialized views, the memory requirements is ~3x size of the output.
