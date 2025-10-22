---
title: "System clusters"
description: "Reference page on system clusters"
menu:
  main:
    parent: "reference"
    weight: 185
---

## Overview

When you enable a Materialize region, various [system
clusters](/sql/system-clusters/) are pre-installed to improve the user
experience as well as support system administration tasks.

### `quickstart` cluster

A cluster named `quickstart` with a size of `25cc` and a replication factor of
`1` will be pre-installed in every environment. You can modify or drop this
cluster at any time.

{{< note >}}
The default value for the `cluster` session parameter is `quickstart`.
This cluster functions as a default option, pre-created for your convenience.
It allows you to quickly start running queries without needing to configure a cluster first.
If the `quickstart` cluster is dropped, you must run [`SET cluster`](/sql/select/#ad-hoc-queries)
to choose a valid cluster in order to run `SELECT` queries. A _superuser_ (i.e. `Organization Admin`)
can also run [`ALTER SYSTEM SET cluster`](/sql/alter-system-set) to change the
default value.
{{< /note >}}

### `mz_catalog_server` system cluster

A system cluster named `mz_catalog_server` will be pre-installed in every
environment. This cluster has several indexes installed to speed up `SHOW`
commands and queries using the system catalog.

To take advantage of these indexes, Materialize will automatically re-route
`SHOW` commands and queries using system catalog objects to the
`mz_catalog_server` system cluster. You can disable this behavior in
your session via the `auto_route_catalog_queries`
[configuration parameter](/sql/show/#other-configuration-parameters).

The following characteristics apply to the `mz_catalog_server` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You can run `SELECT` or `SUBSCRIBE` queries in this cluster as long
    as you only reference objects in the [system catalog](/sql/system-catalog/).

### `mz_probe` system cluster

A system cluster named `mz_probe` will be pre-installed in every environment.
This cluster is used for internal uptime monitoring.

The following characteristics apply to the `mz_probe` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You cannot run `SELECT` or `SUBSCRIBE` queries in this cluster.

### `mz_support` system cluster

A system cluster named `mz_support` will be pre-installed in every environment.
This cluster is used for internal support tasks.

The following characteristics apply to the `mz_support` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You cannot run `SELECT` or `SUBSCRIBE` queries in this cluster.

### `mz_system` system cluster

A system cluster named `mz_system` will be pre-installed in every environment.
This cluster is used for internal system jobs.

The following characteristics apply to the `mz_system` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You cannot run `SELECT` or `SUBSCRIBE` queries in this cluster.


## Related pages

- [`CREATE CLUSTER`](/sql/create-cluster)
- [`SHOW CLUSTER`](/sql/show-clusters)
- [`DROP CLUSTER`](/sql/drop-cluster)
