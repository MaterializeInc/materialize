---
title: "CREATE SOURCE: Avro over Kafka"
description: "Learn how to connect Materialize to an Avro-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
aliases:
    - /sql/create-source/kafka
    - /sql/create-source/avro
    - /sql/create-source/avro-source
---

{{% create-source/intro %}}
This document details how to connect Materialize to an Avro-formatted Kafka
topic.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-avro-kafka.svg" >}}

### format_spec

{{< diagram "format-spec-avro-kafka.svg" >}}

{{% create-source/syntax-details connector="kafka" formats="avro-ccsr" envelopes="debezium upsert append-only" %}}

## Examples

### Using a Confluent schema registry

```sql
CREATE SOURCE events
FROM KAFKA BROKER 'localhost:9092' TOPIC 'events'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```

This creates a source that...

- Automatically determines its schema from the Confluent Schema Registry.
- Decodes data received from the `events` topic published by Kafka running on
  `localhost:9092`.
- Decodes using an Avro schema.
- Is eligible to use the Debezium envelope because it's Avro-encoded and
  published by Kafka; however, this still depends on whether or not the upstream
  database publishes its data from a Debezium-enabled database.

### Inlining the Avro schema

```sql
CREATE SOURCE user
FROM KAFKA BROKER 'localhost:9092' TOPIC 'user'
FORMAT AVRO USING SCHEMA '{
  "type": "record",
  "name": "envelope",
  "fields": [
    ...
  ],
}'
ENVELOPE DEBEZIUM;
```

This creates a source that...

- Has its schema defined inline, and decodes data using that schema.
- Decodes data received from the `user` topic published by Kafka running on
  `localhost:9092`.
- Uses the Debezium envelope, meaning it supports delete, updates, and inserts.

### Upsert on a Kafka topic with string keys and Avro values

```sql
CREATE SOURCE current_predictions
FROM KAFKA BROKER 'localhost:9092' TOPIC 'current_predictions'
FORMAT AVRO USING SCHEMA FILE '/scratch/current_predictions.json'
ENVELOPE UPSERT;
```

This creates a source that...

- Has its schema in a file on disk, and decodes both messages and payload data using that schema.
- Decodes data received from the `current_predictions` topic published by Kafka running on
  `localhost:9092`.
- Uses message keys to determine what should be inserted, deleted, and updated.

### Connecting to a Kafka broker using SSL authentication

```sql
CREATE MATERIALIZED SOURCE data_v1
FROM KAFKA BROKER 'localhost:9092' TOPIC 'top-secret' WITH (
    security_protocol = 'SSL',
    ssl_key_location = '/secrets/materialized.key',
    ssl_certificate_location = '/secrets/materialized.crt',
    ssl_ca_location = '/secrets/ca.crt',
    ssl_key_password = 'mzmzmz'
)
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://localhost:8081';
```

This creates a source that...

- Connects to a Kafka broker and a Confluent Schema Registry that require SSL
  authentication, and whose certificates were both signed by the same CA
  certificate.
- Automatically determines its schema from the Confluent Schema Registry.
- Decodes data received from the `top-secret` topic published by Kafka running on
  `localhost:9092`.
- Is append-only.

### Connecting to a Kafka broker using SASL authentication

```sql
CREATE MATERIALIZED SOURCE data_v1
FROM KAFKA BROKER 'broker.tld:9092' TOPIC 'top-secret' WITH (
    security_protocol = 'SASL_SSL',
    sasl_mechanisms = 'PLAIN',
    sasl_username = '<BROKER_USERNAME>',
    sasl_password = '<BROKER_PASSWORD>',
)
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://schema-registry.tld' WITH (
    username = '<SCHEMA_REGISTRY_USERNAME>',
    password = '<SCHEMA_REGISTRY_PASSWORD>'
);
```

This creates a source that...

- Connects to a Kafka broker that requires SASL PLAIN authentication.
- Connects to a Confluent Schema Registry that requires HTTPS and basic
  authentication.
- Automatically determines its schema from the Confluent Schema Registry.
- Decodes data received from the `top-secret` topic published by Kafka running on
  `localhost:9092`.
- Is append-only.

If you are connecting to a Kafka cluster on Confluent Cloud, this is the
example to follow.

### Connecting to a Kafka broker using Kerberos

```sql
CREATE MATERIALIZED SOURCE data_v1
FROM KAFKA BROKER 'broker.tld:9092' TOPIC 'tps-reports' WITH (
    security_protocol = 'sasl_plaintext',
    sasl_kerberos_keytab = '/secrets/materialized.keytab',
    sasl_kerberos_service_name = 'kafka',
    sasl_kerberos_principal = 'materialized@CI.MATERIALIZE.IO'
)
FORMAT AVRO USING SCHEMA FILE '/tps-reports-schema.json'
```

This creates a source that...

- Connects to a Kerberized Kafka broker whose service principal name is
  `kafka/broker.tld@CI.MATERIALIZE.IO`.
- Has its schema in a file on disk, and decodes payload data using that schema.
- Decodes data received from the `top-secret` topic published by Kafka running on
  `broker.tld:9092`.
- Is append-only.

### Caching records to local disk

```sql
CREATE MATERIALIZED SOURCE cached_source
FROM KAFKA BROKER 'broker.tld:9092' TOPIC 'data' WITH (
    cache = true
)
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://schema-registry.tld'
```

This creates a source that...

- Automatically determines its schema from the Confluent Schema Registry.
- Decodes data received from the `data` topic published by Kafka running on
  `tld:9092`.
- Decodes using an Avro schema.
- Caches messages from the `data` topic to local disk.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)

[Debezium]: http://debezium.io
