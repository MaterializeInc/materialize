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

In Materialize, create an [SSH tunnel connection](/sql/create-connection/#ssh-tunnel) to the bastion server:

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

1. Configure your internal firewall to allow the SSH bastion host to connect to your Kafka cluster or PostgreSQL instance.

    If you are using a cloud provider like AWS or GCP, update the security group or firewall rules for your PostgreSQL instance or Kafka brokers.

    You'll need to allow incoming traffic from the IP address of the SSH bastion host on the necessary ports.

    For example, use port `5432` for PostgreSQL and ports `9092`, `9094`, and `9096` for Kafka.

    After that, test the connection from the bastion host to the Kafka cluster or PostgreSQL instance.

    ```bash
    telnet <KAFKA_BROKER_HOST> <KAFKA_BROKER_PORT>
    telnet <POSTGRES_HOST> <POSTGRES_PORT>
    ```

    If the command hangs, the security group or firewall rules are likely not configured correctly, and you need to double-check your settings. If the connection is successful, you can proceed to the next step.

1. Verify that you can create SSH tunnels.

    ```bash
    # Command for Linux
    ssh -L 9092:kafka-broker:9092 <SSH_BASTION_USER>@<SSH_BASTION_HOST>
    ```

    Once you have successfully created the tunnel, verify that you can connect to the Kafka broker or PostgreSQL instance via the tunnel:

    ```bash
    telnet localhost 9092
    ```

    If the connection is successful, you can proceed to the next step.

    If you are unable to connect using the `telnet` command, you will have to enable `AllowTcpForwarding` and `PermitTunnel` in the SSH config file.
    On your SSH bastion host, open the SSH config file (usually located at `/etc/ssh/sshd_config`) using a text editor:

    ```bash
    sudo nano /etc/ssh/sshd_config
    ```
    Ensure that the following lines are present and uncommented:

    ```bash
    AllowTcpForwarding yes
    PermitTunnel yes
    ```

    If these lines are not present, add them to the file.

    Save the changes and restart the SSH service:

    ```bash
    sudo systemctl restart sshd
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
    'broker1:9092' USING SSH TUNNEL ssh_connection,
    'broker2:9092' USING SSH TUNNEL ssh_connection
    -- Add all Kafka brokers
    )
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
