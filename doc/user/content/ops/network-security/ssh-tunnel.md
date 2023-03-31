---
title: "SSH tunnel connections"
description: "How to connect Materialize to a Kafka broker, or a PostgreSQL database using an SSH tunnel connection to a SSH bastion server"
menu:
  main:
    parent: "network-security"
    name: "SSH tunnel connections"
aliases:
  - /integrations/postgres-bastion/
---

Materialize can connect to a Kafka broker, a Confluent Schema Registry server or a
PostgreSQL database through a secure SSH bastion server. In this guide, we'll
cover how to create `SSH TUNNEL` connections and retrieve the Materialize
public keys needed to configure the bastion server.

## Create an SSH tunnel connection

In Materialize, create an [SSH tunnel connection](/sql/create-connection/#kafka-network-security) to the bastion server:

```sql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);
```

## Configure the SSH bastion server

1. Retrieve the **public keys** for the SSH tunnel connection you just created:

    ```sql
    SELECT * FROM mz_ssh_tunnel_connections;
    ```

    ```
    | id    | public_key_1                          | public_key_2                          |
    |-------|---------------------------------------|---------------------------------------|
    | u75   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize   |
    ```

    You should configure your SSH bastion server to accept **both** key pairs,
    so you can routinely rotate them without downtime using
    [`ALTER CONNECTION`](/sql/alter-connection).

1. Log in to your SSH bastion server and add the keys from the previous step:

    ```bash
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    ```

1. Retrieve the static egress IPs from Materialize and configure the firewall rules (e.g. AWS Security Groups) for your SSH bastion to allow SSH traffic only for those IP addresses.

    ```sql
    SELECT * FROM mz_catalog.mz_egress_ips;
    ```

    ```
    XXX.140.90.33
    XXX.198.159.213
    XXX.100.27.23
    ```
## Create a source connection

In Materialize, create a source connection that uses the SSH tunnel connection you just configured:

{{< tabs tabID="1" >}}
{{< tab "Kafka">}}
```sql
CREATE CONNECTION kafka_connection TO KAFKA (
  BROKERS (
    'broker1:9092',
    'broker2:9092'
  ),
  SSH TUNNEL ssh_connection
);
```

This Kafka connection can then be reused across multiple [`CREATE SOURCE`](/sql/create-source/kafka/)
statements:

```sql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT BYTES
  WITH (SIZE = '3xsmall');
```

{{< /tab >}}
{{< tab "PostgreSQL">}}
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

This PostgreSQL connection can then be reused across multiple [`CREATE SOURCE`](/sql/create-source/postgres/)
statements:

```sql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES
  WITH (SIZE = '3xsmall');
```
{{< /tab >}} {{< /tabs >}}

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka/)
- [`CREATE SOURCE`: PostgreSQL](/sql/create-source/postgres/)
