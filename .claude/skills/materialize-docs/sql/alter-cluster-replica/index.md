---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-cluster-replica/
complexity: intermediate
description: '`ALTER CLUSTER REPLICA` changes properties of a cluster replica.'
doc_type: reference
keywords:
- ALTER CLUSTER
- 'Note:'
- ALTER CLUSTER REPLICA
product_area: Indexes
status: stable
title: ALTER CLUSTER REPLICA
---

# ALTER CLUSTER REPLICA

## Purpose
`ALTER CLUSTER REPLICA` changes properties of a cluster replica.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER CLUSTER REPLICA` changes properties of a cluster replica.



Use `ALTER CLUSTER REPLICA` to:
- Rename a cluster replica.
- Change owner of a cluster replica.

## Syntax

This section covers syntax.

#### Rename

### Rename

To rename a cluster replica:

<!-- Syntax example: examples/alter_cluster_replica / syntax-rename -->

> **Note:** 
You cannot rename replicas in system clusters.


#### Change owner

### Change owner

To change the owner of a cluster replica:

<!-- Syntax example: examples/alter_cluster_replica / syntax-change-owner -->

## Privileges

The privileges required to execute this statement are:

- Ownership of the cluster replica.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing cluster.


## Example

The following changes the owner of the cluster replica `production.r1` to
`admin`.  The user running the command must:
- Be the current owner;
- Be a member of `admin`; and
- Have `CREATE` privilege on the `production` cluster.

```mzsql
ALTER CLUSTER REPLICA production.r1 OWNER TO admin;
```

