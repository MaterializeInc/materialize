---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/network-security/privatelink/
complexity: intermediate
description: How to connect Materialize Cloud to a Kafka broker, a Confluent Schema
  Registry server, a PostgreSQL database, or a MySQL database through an AWS PrivateLink
  service.
doc_type: reference
keywords:
- CREATE SECRET
- CREATE AN
- CREATE SOURCE
- 'Note:'
- CREATE CONNECTION
- AWS PrivateLink connections (Cloud-only)
product_area: Sources
status: stable
title: AWS PrivateLink connections (Cloud-only)
---

# AWS PrivateLink connections (Cloud-only)

## Purpose
How to connect Materialize Cloud to a Kafka broker, a Confluent Schema Registry server, a PostgreSQL database, or a MySQL database through an AWS PrivateLink service.

If you need to understand the syntax and options for this command, you're in the right place.


How to connect Materialize Cloud to a Kafka broker, a Confluent Schema Registry server, a PostgreSQL database, or a MySQL database through an AWS PrivateLink service.


Materialize can connect to a Kafka broker, a Confluent Schema Registry server, a
PostgreSQL database, or a MySQL database through an [AWS PrivateLink](https://aws.amazon.com/privatelink/)
service.

In this guide, we'll cover how to create `AWS PRIVATELINK` connections and
retrieve the AWS principal needed to configure the AWS PrivateLink service.

## Create an AWS PrivateLink connection


This section covers create an aws privatelink connection.

#### Kafka on AWS


> **Note:** 
Materialize provides a Terraform module that automates the creation and
configuration of AWS resources for a PrivateLink connection. For more details,
see the Terraform module repositories for [Amazon MSK](https://github.com/MaterializeInc/terraform-aws-msk-privatelink)
and [self-managed Kafka clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink).


<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: network-security/privatelink-kafka --> --> -->


#### PostgreSQL on AWS


> **Note:** 
Materialize provides a Terraform module that automates the creation and
configuration of AWS resources for a PrivateLink connection. For more details,
see the [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).


<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: network-security/privatelink-postgres --> --> -->


#### MySQL on AWS


> **Note:** 
Materialize provides a Terraform module that automates the creation and
configuration of AWS resources for a PrivateLink connection. For more details,
see the [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).


<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: network-security/privatelink-mysql --> --> -->


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