---
title: "SSH tunnel connections"
description: "How to connect Materialize to a PostgreSQL database using an SSH tunnel connection to a SSH bastion server"
menu:
  main:
    parent: "network-security"
    name: "SSH tunnel connections"
aliases:
  - /integrations/postgres-bastion/
---

Materialize can connect to a PostgreSQL database through a secure SSH bastion
server. In this guide, we'll cover how to:

* Create `SSH TUNNEL` and `POSTGRES` connections
* Create a PostgreSQL source that uses an SSH tunnel connection
* Configure Materialize public keys on the SSH bastion server

### Prerequisites

Before moving on, double check that you have:

* A running PostgreSQL database
* An SSH bastion server that has access to your PostgreSQL database, and can be
  accessed from Materialize
* A region enabled Materialize

### Steps

1. In Materialize, create an [SSH tunnel connection](/sql/create-connection/#ssh-tunnel) to the bastion server:

    ```sql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        USER '<SSH_BASTION_USER>',
        PORT <SSH_BASTION_PORT>
    );
    ```

1. Retrieve the **public keys** for the SSH tunnel connection you just created:

    ```sql
    SELECT * FROM mz_ssh_tunnel_connections;
    ```

    ```
    | id    | public_key_1                          | public_key_2                          |
    |-------|---------------------------------------|---------------------------------------|
    | u75   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize   |
    ```

1. Log in to your SSH bastion server and add the keys from the previous step:

    ```bash
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    ```

1. In Materialize, create a [PostgreSQL connection](/sql/create-connection/#postgres) that uses `ssh_connection`:

    ```sql
    CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

    CREATE CONNECTION pg_connection TO POSTGRES (
      HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
      PORT 5432,
      USER 'postgres',
      PASSWORD SECRET pgpass,
      SSL MODE 'require',
      DATABASE 'postgres'
      SSH TUNNEL ssh_connection
    );
    ```

1. Create a [PostgreSQL source](/sql/create-source/postgres/#create-source-example):

    ```sql
    CREATE SOURCE mz_source
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES
      WITH (SIZE = '3xsmall');
    ```

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: PostgreSQL](/sql/create-source/postgres/)
- [PostgreSQL CDC guide](/integrations/cdc-postgres/#direct-postgres-source)
