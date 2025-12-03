---
title: "ALTER SECRET"
description: "`ALTER SECRET` changes the contents of a secret."
menu:
  main:
    parent: 'commands'
---

Use `ALTER SECRET` to:

- Change the value of the secret.
- Rename a secret.
- Change owner of a secret.

## Syntax

{{< tabs >}}
{{< tab "Change value" >}}

### Change value

To change the value of a secret:

```mzsql
ALTER SECRET [IF EXISTS] <name> AS <value>;
```

Syntax element | Description
---------------|------------
`<name>` | The identifier of the secret you want to alter.
`<value>` | The new value for the secret. The _value_ expression may not reference any relations, and must be implicitly castable to `bytea`.

{{< /tab >}}
{{< tab "Rename" >}}

### Rename

To rename a secret:

```mzsql
ALTER SECRET [IF EXISTS] <name> RENAME TO <new_name>;
```

Syntax element | Description
---------------|------------
`<name>`| The current name of the secret.
`<new_name>`| The new name of the secret.

See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a secret:

```mzsql
ALTER SECRET [IF EXISTS] <name> OWNER TO <new_owner_role>;
```

Syntax element | Description
---------------|------------
`<name>`| The name of the secret you want to change ownership of.
`<new_owner_role>`| The new owner of the secret.

To change the owner, you must be a current owner as well as have membership in
the `<new_owner_role>`.

{{< /tab >}}
{{< /tabs >}}

## Details

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

- [`SHOW SECRETS`](/sql/show-secrets)
- [`DROP SECRET`](/sql/drop-secret)

[`CREATE CONNECTION`]: /sql/create-connection/
[`CREATE SOURCE`]: /sql/create-source
[`CREATE SINK`]: /sql/create-sink
[`ALTER SOURCE`]: /sql/alter-source
[`ALTER SINK`]: /sql/alter-sink
