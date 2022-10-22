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
* Create the source
* Configure Materialize public keys on the bastion server

### Pre-requirements

Before following the steps, check meeting the next items:

* Running Postgres database
* SSH bastion server
    * It must be accessible from Materialize and have access to Postgres
* Materialize's region enabled

### Steps

1. Create in Materialize the [SSH connection](/sql/create-connection/#example-4) to the bastion server:
    ```sql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        USER '<SSH_BASTION_USER>',
        PORT <SSH_BASTION_PORT>
    );

1. Get your **region's public keys** in Materialize for the SSH tunnel connection:
    ```sql
    SELECT * FROM mz_ssh_tunnel_connections;
    ```
    ```
    | id    | public_key_1                          | public_key_2                          |
    |-------|---------------------------------------|---------------------------------------|
    | u75   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize   |
    ```

1. Log in to your SSH bastion server and add the keys from the previous step query:
    ```bash
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    ```

1. Create in Materialize the [Postgres connection](/sql/create-connection/#example-3):
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

1. Create in Materialize the [Postgres source](/sql/create-source/postgres/#creating-a-source-1):
    ```sql
    CREATE SOURCE mz_source
    FROM POSTGRES
    CONNECTION pg_connection
    PUBLICATION 'mz_source'
    ```
