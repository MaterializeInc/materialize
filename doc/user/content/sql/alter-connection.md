---
title: "ALTER CONNECTION"
description: "`ALTER CONNECTION` allows you to modify the value of connection options; rotate secrets associated with connections; rename a connection; and change owner of a connection."
menu:
  main:
    parent: 'commands'
---

Use `ALTER CONNECTION` to:

- Modify the parameters of a connection, such as the hostname to which it
  points.
- Rotate the key pairs associated with an [SSH tunnel connection].
- Rename a connection.
- Change owner of a connection.

## Syntax

{{< tabs >}}
{{< tab "SET/DROP/RESET options" >}}

### SET/DROP/RESET options

To modify connection parameters:

```mzsql
ALTER CONNECTION [IF EXISTS] <name>
  SET (<option> = <value>) | DROP (<option>) | RESET (<option>)
  [, ...]
  [WITH (VALIDATE [true|false])]
;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified connection does not exist.
`<name>` | The identifier of the connection you want to alter.
**SET** | Sets the option to the specified value.
**DROP** | Resets the specified option to its default value. Synonym for **RESET**.
**RESET** | Resets the specified option to its default value. Synonym for **DROP**.
`<option>` | The connection option to modify. See [`CREATE CONNECTION`](/sql/create-connection) for available options.
`<value>` | The value to assign to the option.
**WITH (VALIDATE `<bool>`)** | Optional. Whether [connection validation](/sql/create-connection#connection-validation) should be performed. Defaults to `true`.

{{< /tab >}}
{{< tab "ROTATE KEYS" >}}

### ROTATE KEYS

To rotate SSH tunnel connection key pairs:

```mzsql
ALTER CONNECTION [IF EXISTS] <name> ROTATE KEYS;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified connection does not exist.
`<name>` | The identifier of the SSH tunnel connection.


{{< /tab >}}

{{< tab "Rename" >}}

### Rename

To rename a connection

```mzsql
ALTER CONNECTION <name> RENAME TO <new_name>;
```

Syntax element | Description
---------------|------------
`<name>`| The current name of the connection.
`<new_name>`| The new name of the connection.

See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a connection:

```mzsql
ALTER CONNECTION <name> OWNER TO <new_owner_role>;
```

Syntax element | Description
---------------|------------
`<name>`| The name of the connection you want to change ownership of.
`<new_owner_role>`| The new owner of the connection.

To change the owner of a connection, you must be the owner of the connection and
have membership in the `<new_owner_role>`. See also [Privileges](#privileges).

{{< /tab >}}
{{< /tabs >}}

## Details

### `SET`, `RESET`, `DROP`

These subcommands let you modify the parameters of a connection.

* **RESET** and **DROP** are synonyms and will return the parameter to its
    original state. For instance, if the connection has a default port, **DROP**
    will return it to the default value.
* All provided changes are applied atomically.
* The same parameter cannot have multiple modifications.

For the available parameters for each type of connection, see [`CREATE
CONNECTION`](/sql/create-connection).

### `ROTATE KEYS`

The `ROTATE KEYS` command can be used to change the key pairs associated with
an [SSH tunnel connection] without causing downtime.

Each SSH tunnel connection is associated with two key pairs. The public keys
for the key pairs are announced in the [`mz_ssh_tunnel_connections`]
system table in the `public_key_1` and `public_key_2` columns.

Upon executing the `ROTATE KEYS` command, Materialize deletes the first key
pair, promotes the second key pair to the first key pair, and generates a new
second key pair. The connection's row in `mz_ssh_tunnel_connections` is updated
accordingly: the `public_key_1` column will contain the public key that was
formely in the `public_key_2` column, and the `public_key_2` column will contain
a new public key.

After executing `ROTATE KEYS`, you should update your SSH bastion server with
the new public keys:

* Remove the public key that was formely in the `public_key_1` column.
* Add the new public key from the `public_key_2` column.

Throughout the entire process, the SSH bastion server is configured to permit
authentication from at least one of the keys that Materialize will authenticate
with, so Materialize's ability to connect is never interrupted.

You must take care to update the SSH bastion server with the new keys after
every execution of the `ROTATE KEYS` command. If you rotate keys twice in
succession without adding the new keys to the bastion server, Materialize will
be unable to authenticate with the bastion server.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-connection.md" >}}

## Related pages

-   [`CREATE CONNECTION`](/sql/create-connection/)
-   [`SHOW CONNECTIONS`](/sql/show-connections)

[SSH tunnel connection]: /sql/create-connection/#ssh-tunnel
[`mz_ssh_tunnel_connections`]: /sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections
