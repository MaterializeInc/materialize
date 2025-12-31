---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-secret/
complexity: intermediate
description: '`ALTER SECRET` changes the contents of a secret.'
doc_type: reference
keywords:
- CREATE CONNECTION
- ALTER SECRET
- not
product_area: Indexes
status: stable
title: ALTER SECRET
---

# ALTER SECRET

## Purpose
`ALTER SECRET` changes the contents of a secret.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER SECRET` changes the contents of a secret.



Use `ALTER SECRET` to:

- Change the value of the secret.
- Rename a secret.
- Change owner of a secret.

## Syntax

This section covers syntax.

#### Change value

### Change value

To change the value of a secret:

<!-- Syntax example: examples/alter_secret / syntax-change-value -->

#### Rename

### Rename

To rename a secret:

<!-- Syntax example: examples/alter_secret / syntax-rename -->

#### Change owner

### Change owner

To change the owner of a secret:

<!-- Syntax example: examples/alter_secret / syntax-change-owner -->

## Details

This section covers details.

### Changing the secret value

After an `ALTER SECRET` command is executed:

  * Future [`CREATE CONNECTION`], [`CREATE SOURCE`], and [`CREATE SINK`]
    commands will use the new value of the secret immediately.

  * Running sources and sinks that reference the secret will **not** immediately
    use the new value of the secret. Sources and sinks may cache the old secret
    value for several weeks.

    To force a running source or sink to refresh its secrets, drop and recreate
    all replicas of the cluster hosting the source or sink.

    For a managed cluster:

    ```sql
    ALTER CLUSTER storage_cluster SET (REPLICATION FACTOR = 0);
    ALTER CLUSTER storage_cluster SET (REPLICATION FACTOR = 1);
    ```

    For an unmanaged cluster:

    ```text
    DROP CLUSTER REPLICA storage_cluster.r1;
    CREATE CLUSTER REPLICA storage_cluster.r1 (SIZE = '<original size>');
        ```

## Examples

This section covers examples.

```mzsql
ALTER SECRET kafka_ca_cert AS decode('c2VjcmV0Cg==', 'base64');
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the secret being altered.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the secret is namespaced
  by a schema.


## Related pages

- [`SHOW SECRETS`](/sql/show-secrets)
- [`DROP SECRET`](/sql/drop-secret)

[`CREATE CONNECTION`]: /sql/create-connection/
[`CREATE SOURCE`]: /sql/create-source
[`CREATE SINK`]: /sql/create-sink
[`ALTER SOURCE`]: /sql/alter-source
[`ALTER SINK`]: /sql/alter-sink

