---
title: "ALTER CLUSTER REPLICA"
description: "`ALTER CLUSTER REPLICA` changes properties of a cluster replica."
menu:
  main:
    parent: 'commands'
---

Use `ALTER CLUSTER REPLICA` to:
- Rename a cluster replica.
- Change owner of a cluster replica.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a cluster replica:

```mzsql
ALTER CLUSTER REPLICA <name> RENAME TO <new_name>;
```

Syntax element | Description
---------------|------------
`<name>`| The current name of the cluster replica.
`<new_name>`| The new name of the cluster replica.

{{< note >}}
You cannot rename replicas in system clusters.
{{< /note >}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a cluster replica:

```mzsql
ALTER CLUSTER REPLICA <name> OWNER TO <new_owner_role>;
```

Syntax element | Description
---------------|------------
`<name>`| The name of the cluster replica you want to change ownership of.
`<new_owner_role>`| The new owner of the cluster replica.

To change the owner of a cluster replica, you must be the current owner and have
membership in the `<new_owner_role>`.

{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-cluster-replica.md" >}}

## Example

The following changes the owner of the cluster replica `production.r1` to
`admin`.  The user running the command must:
- Be the current owner;
- Be a member of `admin`; and
- Have `CREATE` privilege on the `production` cluster.

```mzsql
ALTER CLUSTER REPLICA production.r1 OWNER TO admin;
```
