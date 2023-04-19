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

Materialize can connect to data sources like Kafka, Confluent, and PostgreSQL with a
secure SSH bastion server. In this guide, you will create an `SSH TUNNEL`
connection, configure your Materialize authentication settings, and create a
source connection.

## Prerequisites

Before you begin, make sure you have access to a bastion host. You will need:

* The bastion host IP address and port number
* The bastion host username

## Create an SSH tunnel connection

In Materialize, use a `CREATE CONNECTION` statement to create an [SSH tunnel connection](/sql/create-connection/#ssh-tunnel) to the bastion server:

```sql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);
```

## Configure the SSH bastion server

The bastion host needs a **public key** to connect to the Materialize tunnel you
created in the previous step.

1. Materialize stores public keys for SSH tunnels. Use a `SELECT` statement to
   return two public keys:

    ```sql
    SELECT * FROM mz_ssh_tunnel_connections;
    ```

    ```
    | id    | public_key_1                          | public_key_2                          |
    |-------|---------------------------------------|---------------------------------------|
    | u75   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize   |
    ```


    > Materialize provides two public keys to allow you to rotate keys without
    connection downtime. Review the [`ALTER CONNECTION`](/sql/alter-connection) documentation for
    more information on how to rotate your keys.

1. Log in to your SSH bastion server and add each key to the bastion `authorized_keys` file:

    ```bash
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

3. Configure your internal firewall to allow the SSH bastion host to connect to your Kafka cluster or PostgreSQL instance.

    If you are using a cloud provider like AWS or GCP, update the security group or firewall rules for your PostgreSQL instance or Kafka brokers.

    Allow incoming traffic from the SSH bastion host IP address on the necessary ports.

    For example, use port `5432` for PostgreSQL and ports `9092`, `9094`, and `9096` for Kafka.

    Test the connection from the bastion host to the Kafka cluster or PostgreSQL instance.

    ```bash
    telnet <KAFKA_BROKER_HOST> <KAFKA_BROKER_PORT>
    telnet <POSTGRES_HOST> <POSTGRES_PORT>
    ```

    If the command hangs, double-check your security group and firewall settings. If the connection is successful, you can proceed to the next step.

4. Verify the tunnel connection from your source to your SSH bastion host

    ```bash
    # Command for Linux
    ssh -L 9092:kafka-broker:9092 <SSH_BASTION_USER>@<SSH_BASTION_HOST>
    ```

    Verify that you can connect to the Kafka broker or PostgreSQL instance via the tunnel:

    ```bash
    telnet localhost 9092
    ```

    If you are unable to connect using the `telnet` command, enable `AllowTcpForwarding` and `PermitTunnel` on your bastion host SSH configuration file.

    On your SSH bastion host, open the SSH config file (usually located at `/etc/ssh/sshd_config`) using a text editor:

    ```bash
    sudo nano /etc/ssh/sshd_config
    ```

    Add or uncomment the following lines:

    ```bash
    AllowTcpForwarding yes
    PermitTunnel yes
    ```

    Save the changes and restart the SSH service:

    ```bash
    sudo systemctl restart sshd
    ```

5. Retrieve the static egress IPs from Materialize and configure the firewall rules (e.g. AWS Security Groups) for your SSH bastion to allow SSH traffic for those IP addresses only.

    ```sql
    SELECT * FROM mz_catalog.mz_egress_ips;
    ```

    ```
    XXX.140.90.33
    XXX.198.159.213
    XXX.100.27.23
    ```

## Create a source connection

In Materialize, create a source connection that uses the SSH tunnel connection you configured in the previous section:

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

You can reuse this Kafka connection across multiple [`CREATE SOURCE`](/sql/create-source/kafka/)
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

You can reuse this PostgreSQL connection across multiple [`CREATE SOURCE`](/sql/create-source/postgres/)
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
