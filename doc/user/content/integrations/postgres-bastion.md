---
title: "SSH Tunnel to PostgreSQL via Bastion Host"
description: "How to connect Materialize to a Postgres DB on a private network using an SSH tunnel through a bastion host."
menu:
  main:
    parent: "integration-guides"
    name: "SSH Tunnel (PostgreSQL)"
---

If it's not possible to provide Materialize with direct access to your Postgres source database, you can use an SSH tunnel to create a secure connection through a bastion host.

This guide walks through the process of creating a secure connection from Materialize to a Postgres source database in three steps:

* Create an SSH tunnel connection object in Materialize
* Place the public keys generated in Materialize on your bastion server
* Create a connection to the Postgres DB using the SSH tunnel

### Prerequisites

To follow this guide, you will need:

* A [compatible Postgres database](https://materialize.com/docs/integrations/#postgresql)
* An SSH bastion server - This is a server with SSH capabilities that is accessible from Materialize and has access to your Postgres DB.
* A Materialize region enabled

### Steps

1. In Materialize, create an [SSH connection](/sql/create-connection/#postgres-ssh-example) to the bastion server:
    ```sql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        USER '<SSH_BASTION_USER>',
        PORT <SSH_BASTION_PORT>
    );
    ```
    This command creates a connection object named `ssh_connection` and generates an SSH key pair.
    The public key is made available in the `mz_ssh_tunnel_connections` system table.

1. In Materialize, get the contents of the primary public key in `mz_ssh_tunnel_connections.public_key_1`
    ```sql
    SELECT * FROM mz_ssh_tunnel_connections;
    ```
    ```
    | id    | public_key_1           | public_key_2             |
    |-------|------------------------|--------------------------|
    | u75   | <PRIMARY_PUBLIC_KEY>   | <SECONDARY_PUBLIC_KEY>   |
    ```

1. In your bastion server, log in and add the public SSH key to your authorized keys file:
    ```bash
    # Command for Linux
    echo "<PRIMARY_PUBLIC_KEY>" >> ~/.ssh/authorized_keys
    ```

1. In Materialize, create a [Postgres connection](/sql/create-connection/#postgres-example) that specifies the previously created `ssh_connection` in the `SSH TUNNEL` option:
    ```sql
    CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

    CREATE CONNECTION pg_connection
    FOR POSTGRES
        HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
        PORT 5432,
        USER 'postgres',
        PASSWORD SECRET pgpass,
        SSL MODE 'require',
        DATABASE 'postgres'
        SSH TUNNEL ssh_connection;
    ```

1. In Materialize, create a [Postgres source](/sql/create-source/postgres/#create-source-example):
    ```sql
    CREATE SOURCE mz_source
    FROM POSTGRES
    CONNECTION pg_connection
    PUBLICATION 'mz_source'
    ```
Upon creating the source, Materialize will attempt to connect to your Postgres DB via the SSH tunnel.
