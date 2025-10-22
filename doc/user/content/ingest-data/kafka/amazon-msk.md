---
title: "Amazon Managed Streaming for Apache Kafka (Amazon MSK)"
description: "How to securely connect an Amazon MSK cluster as a source to Materialize."
aliases:
  - /integrations/aws-msk/
  - /integrations/amazon-msk/
  - /connect-sources/amazon-msk/
  - /ingest-data/amazon-msk/
menu:
  main:
    parent: "kafka"
    name: "Amazon MSK"
---

[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the PostgreSQL ones. We should add information about using AWS IAM
authentication then."

This guide goes through the required steps to connect Materialize to an Amazon
MSK cluster.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

Before you begin, you must have:

- An Amazon MSK cluster running on AWS.
- A client machine that can interact with your cluster.

## Creating a connection


{{< tabs >}}
{{< tab "Cloud" >}}

There are various ways to configure your Kafka network to allow Materialize to
connect:

- **Allow Materialize IPs:** If your Kafka cluster is publicly accessible, you
    can configure your firewall to allow connections from a set of static
    Materialize IP addresses.

- **Use AWS PrivateLink**: If your Kafka cluster is running in a private network, you
    can use [AWS PrivateLink](/ingest-data/network-security/privatelink/) to
    connect Materialize to the cluster. For details, see [AWS PrivateLink](/ingest-data/network-security/privatelink/).

- **Use an SSH tunnel:** If your Kafka cluster is running in a private network,
    you can use an SSH tunnel to connect Materialize to the cluster.

{{< tabs tabID="1" >}}

{{< tab "PrivateLink">}}

{{< note >}}
Materialize provides a Terraform module that automates the creation and
configuration of AWS resources for a PrivateLink connection. For more details,
see the Terraform module repositories for [Amazon MSK](https://github.com/MaterializeInc/terraform-aws-msk-privatelink)
and [self-managed Kafka clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink).
{{</ note >}}

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

{{< tab "Public cluster">}}

{{< include-md file="shared-content/kafka-amazon-msk-public-cluster-section.md"
>}}

{{< /tab >}}
{{< /tabs >}}
{{< /tab >}}
{{< tab "Self-Managed" >}}
Configure your Kafka network to allow Materialize to connect:

- **Use an SSH tunnel**: If your Kafka cluster is running in a private network, you can use an SSH tunnel to connect Materialize to the cluster.

- **Allow Materialize IPs**: If your Kafka cluster is publicly accessible, you can configure your firewall to allow connections from a set of static Materialize IP addresses.

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

{{< tab "Public cluster">}}

{{< include-md file="shared-content/kafka-amazon-msk-public-cluster-section.md"
>}}

{{< /tab >}}
{{< /tabs >}}
{{< /tab >}}
{{< /tabs >}}


## Creating a source

The Kafka connection created in the previous section can then be reused across
multiple [`CREATE SOURCE`](/sql/create-source-v1/kafka/) statements. By default,
the source will be created in the active cluster; to use a different cluster,
use the `IN CLUSTER` clause.

```mzsql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT JSON;
```

If the command executes without an error and outputs _CREATE SOURCE_, it means
that you have successfully connected Materialize to your cluster.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source-v1/kafka)
