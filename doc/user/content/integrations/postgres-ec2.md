---
title: "SSH Bastion (PostgreSQL)"
description: "How to connect Postgres to Materialize through an SSH Bastion connection"
menu:
  main:
    parent: "integration-guides"
    name: "SSH Bastion (PostgreSQL)"
---

Materialize can connect to a Postgres database through a secure SSH bastion server. Through this guide, you will learn how to:
* Create the connections
* Create the sources
* Configure Materialize public keys on the bastion server.

### Pre-requirements

* Postgres database
* SSH bastion server
    * It must be accessible from Materialize and have access to Postgres.
* Materialize's region enabled

### Steps

1. Create in Materialize the [SSH connection](/sql/create-connection/#example-4) to the bastion server:
    ```sql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        USER '<SSH_BASTION_USER>',
        PORT 1
    );

1. Get your **region's public keys** in Materialize for the SSH tunnel connection:
    ```sql
    SELECT * FROM mz_ssh_tunnel_connections;
    ```

1. Add the keys to the SSH bastion server:
    ```bash
    # Command for Linux
    echo "ssh-ed25519 <KEY> materialize" >> ~/.ssh/authorized_keys
    ```

1. Create the Postgres **connection**:
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

1. Create the Postgres **source**:
    ```sql
    CREATE SOURCE mz_source
    FROM POSTGRES
    CONNECTION pg_connection
    PUBLICATION 'mz_source'
    ```