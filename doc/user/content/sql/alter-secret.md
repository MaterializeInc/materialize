---
title: "ALTER SECRET"
description: "`ALTER SECRET` changes the contents of a secret."
menu:
  main:
    parent: 'commands'
---

`ALTER SECRET` changes the contents of a secret. To rename a secret, see [`ALTER...RENAME`](/sql/alter-rename/).

## Syntax

{{< diagram "alter-secret.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the secret you want to alter.
_value_ | The new value for the secret. The _value_ expression may not reference any relations, and must be implicitly castable to `bytea`.

## Details

After an `ALTER SECRET` command is executed:

  * Future [`CREATE CONNECTION`], [`CREATE SOURCE`], and [`CREATE SINK`]
    commands will use the new value of the secret immediately.

  * Running sources and sinks that reference the secret will **not** immediately
    use the new value of the secret. Sources and sinks may cache the old secret
    value for several weeks.

    To force a running source or sink to refresh its secrets, drop and recreate
    all replicas of the cluster hosting the source or sink.

    For a managed cluster:

    ```
    ALTER CLUSTER storage_cluster SET (REPLICATION FACTOR = 0);
    ALTER CLUSTER storage_cluster SET (REPLICATION FACTOR = 1);
    ```

    For an unmanaged cluster:

    ```
    DROP CLUSTER REPLICA storage_cluster.r1;
    CREATE CLUSTER REPLICA storage_cluster.r1 (SIZE = '<original size>');
        ```

## Examples

```mzsql
ALTER SECRET kafka_ca_cert AS decode('c2VjcmV0Cg==', 'base64');
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-secret.md" >}}

## Related pages

- [`ALTER...RENAME`](/sql/alter-rename/)
- [`SHOW SECRETS`](/sql/show-secrets)
- [`DROP SECRET`](/sql/drop-secret)

[`CREATE CONNECTION`]: /sql/create-connection/
[`CREATE SOURCE`]: /sql/create-source
[`CREATE SINK`]: /sql/create-sink
[`ALTER SOURCE`]: /sql/alter-source
[`ALTER SINK`]: /sql/alter-sink
