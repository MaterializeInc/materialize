---
title: "Ingest data from Self-hosted Kafka"
description: "How to connect a self-hosted Kafka cluster as a source to Materialize."
aliases:
  - /integrations/aws-kafka/
  - /integrations/amazon-kafka/
  - /connect-sources/amazon-kafka/
  - /ingest-data/kafka-self-hosted/
  - /ingest-data/kafka/upstash-kafka/
menu:
  main:
    parent: "kafka"
    name: "Self-hosted Kafka"
---

[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the Postgres ones. We should include spill to disk in the guidance then."

This guide goes through the required steps to connect Materialize to a
self-hosted Kafka cluster.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

Before you begin, you must have:

- A Kafka cluster running Kafka 3.2 or later.
- A client machine that can interact with your cluster.

## Configure network security

There are various ways to configure your Kafka network to allow Materialize to
connect:

- **Use AWS PrivateLink**: If your Kafka cluster is running on AWS, you can use
    AWS PrivateLink to connect Materialize to the cluster.

- **Use an SSH tunnel**: If your Kafka cluster is running in a private network,
    you can use an SSH tunnel to connect Materialize to the cluster.

- **Allow Materialize IPs**: If your Kafka cluster is publicly accessible, you
    can configure your firewall to allow connections from a set of static
    Materialize IP addresses.

Select the option that works best for you.

{{< tabs >}}

{{< tab "Cloud" >}}
{{< tabs tabID="1" >}}

{{< tab "Privatelink">}}

{{< note >}}
Materialize provides Terraform modules for both [Amazon MSK clusters](https://github.com/MaterializeInc/terraform-aws-msk-privatelink)
and [self-managed Kafka clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink)
which can be used to create the target groups for each Kafka broker (step 1),
the network load balancer (step 2), the TCP listeners (step 3) and the VPC
endpoint service (step 5).

{{< /note >}}

{{% network-security/privatelink-kafka %}}

{{< /tab >}}

{{< tab "SSH Tunnel">}}

{{% network-security/ssh-tunnel %}}

1. In Materialize, create a source connection that uses the SSH tunnel
connection you configured in the previous section:

  ```mzsql
  CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'broker1:9092',
    SSH TUNNEL ssh_connection
  );
```

{{< /tab >}}

{{< tab "Allow Materialize IPs">}}

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your Kafka cluster firewall rules to allow traffic from each IP
   address from the previous step.

1. Create a [Kafka connection](/sql/create-connection/#kafka) that references
   your Kafka cluster:

    ```mzsql
    CREATE SECRET kafka_password AS '<your-password>';

    CREATE CONNECTION kafka_connection TO KAFKA (
        BROKER '<broker-url>',
        SASL MECHANISMS = 'SCRAM-SHA-512',
        SASL USERNAME = '<your-username>',
        SASL PASSWORD = SECRET kafka_password
    );
    ```

{{< /tab >}}

{{< /tabs >}}
{{< /tab >}}
{{< tab "Self-Managed" >}}

There are various ways to configure your Kafka network to allow Materialize to
connect:

- **Use an SSH tunnel**: If your Kafka cluster is running in a private network,
    you can use an SSH tunnel to connect Materialize to the cluster.

- **Allow Materialize IPs**: If your Kafka cluster is publicly accessible, you
    can configure your firewall to allow connections from a set of static
    Materialize IP addresses.

Select the option that works best for you.

{{< tabs >}}

{{< tab "SSH Tunnel">}}

{{% network-security/ssh-tunnel-sm %}}


1. In Materialize, create a source connection that uses the SSH tunnel
connection you configured in the previous section:

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
  BROKER 'broker1:9092',
  SSH TUNNEL ssh_connection
);
```

{{< /tab >}}

{{< tab "Allow Materialize IPs">}}

1. Update your Kafka cluster firewall rules to allow traffic from Materialize.

1. Create a [Kafka connection](/sql/create-connection/#kafka) that references
   your Kafka cluster:

    ```mzsql
    CREATE SECRET kafka_password AS '<your-password>';

    CREATE CONNECTION kafka_connection TO KAFKA (
        BROKER '<broker-url>',
        SASL MECHANISMS = 'SCRAM-SHA-512',
        SASL USERNAME = '<your-username>',
        SASL PASSWORD = SECRET kafka_password
    );
    ```

{{< /tab >}}

{{< /tabs >}}
{{< /tab >}}
{{< /tabs >}}

## Creating a source

The Kafka connection created in the previous section can then be reused across
multiple [`CREATE SOURCE`](/sql/create-source-v1/kafka/) statements:

```mzsql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT JSON;
```

By default, the source will be created in the active cluster; to use a different
cluster, use the `IN CLUSTER` clause.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source-v1/kafka)
