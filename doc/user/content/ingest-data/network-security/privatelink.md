---
title: "AWS PrivateLink connections"
description: "How to connect Materialize to a Kafka broker, or a PostgreSQL database using an AWS PrivateLink connection"
aliases:
  - /ops/network-security/privatelink/
  - /connect-sources/privatelink/
menu:
  main:
    parent: "network-security"
    name: "AWS PrivateLink connections"
---

{{< public-preview />}}

Materialize can connect to a Kafka broker, a Confluent Schema Registry server or
a PostgreSQL database through an [AWS PrivateLink](https://aws.amazon.com/privatelink/) service.

In this guide, we'll cover how to create `AWS PRIVATELINK` connections
and retrieve the AWS principal needed to configure the AWS PrivateLink service.

## Create an AWS PrivateLink connection

{{< tabs tabID="1" >}}
{{< tab "Kafka on AWS">}}

{{< note >}}
Materialize provides Terraform modules for both [MSK cluster](https://github.com/MaterializeInc/terraform-aws-msk-privatelink) and [self-managed Kafka clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink) which can be used to create the target groups for each Kafka broker (step 1), the network load balancer (step 2),
the TCP listeners (step 3) and the VPC endpoint service (step 5).
{{< /note >}}

{{% privatelink/kafka %}}

{{< /tab >}}

{{< tab "AWS RDS">}}

{{% privatelink/postgres %}}

{{< /tab >}}

{{< /tabs >}}

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka)
- [AWS Kafka integration guide](/integrations/aws-kafka)
