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

{{% include-syntax file="examples/alter_connection" example="syntax-set-drop-reset" %}}

{{< /tab >}}
{{< tab "ROTATE KEYS" >}}

### ROTATE KEYS

To rotate SSH tunnel connection key pairs:

{{% include-syntax file="examples/alter_connection" example="syntax-rotate-keys" %}}


{{< /tab >}}

{{< tab "Rename" >}}

### Rename

To rename a connection

{{% include-syntax file="examples/alter_connection" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a connection:

{{% include-syntax file="examples/alter_connection" example="syntax-change-owner" %}}

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
