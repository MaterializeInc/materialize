---
title: "AWS PrivateLink connections (Cloud-only)"
description: "How to connect Materialize Cloud to a Kafka broker, a Confluent Schema Registry server, a
PostgreSQL database, or a MySQL database through an AWS PrivateLink service."
aliases:
  - /ops/network-security/privatelink/
  - /connect-sources/privatelink/
menu:
  main:
    parent: "network-security"
    name: "AWS PrivateLink connections (Cloud-only)"
---

Materialize can connect to a Kafka broker, a Confluent Schema Registry server, a
PostgreSQL database, or a MySQL database through an [AWS PrivateLink](https://aws.amazon.com/privatelink/)
service.

In this guide, we'll cover how to create `AWS PRIVATELINK` connections and
retrieve the AWS principal needed to configure the AWS PrivateLink service.

## Create an AWS PrivateLink connection

{{< tabs tabID="1" >}}
{{< tab "Kafka on AWS">}}

{{< note >}}
Materialize provides a Terraform module that automates the creation and
configuration of AWS resources for a PrivateLink connection. For more details,
see the Terraform module repositories for [Amazon MSK](https://github.com/MaterializeInc/terraform-aws-msk-privatelink)
and [self-managed Kafka clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink).
{{</ note >}}

{{% network-security/privatelink-kafka %}}

{{< /tab >}}

{{< tab "PostgreSQL on AWS">}}

{{< note >}}
Materialize provides a Terraform module that automates the creation and
configuration of AWS resources for a PrivateLink connection. For more details,
see the [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).
{{</ note >}}

{{% network-security/privatelink-postgres %}}

{{< /tab >}}

{{< tab "MySQL on AWS">}}

{{< note >}}
Materialize provides a Terraform module that automates the creation and
configuration of AWS resources for a PrivateLink connection. For more details,
see the [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).
{{</ note >}}

{{% network-security/privatelink-mysql %}}

{{< /tab >}}

{{< /tabs >}}

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka)
- Integration guides: [Self-hosted
  PostgreSQL](/ingest-data/postgres/self-hosted/), [Amazon RDS for
  PostgreSQL](/ingest-data/postgres/amazon-rds/), [Self-hosted
  Kafka](/ingest-data/kafka/kafka-self-hosted), [Amazon
  MSK](/ingest-data/kafka/amazon-msk), [Redpanda
  Cloud](/ingest-data/redpanda/redpanda-cloud/)
