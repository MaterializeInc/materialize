---
title: "CREATE CONNECTION"
description: "Creating connections to external data sources"
menu:
  main:
    parent: 'commands'

---

A connection in Materialize describes how to connect and authenticate to an external source.
Multiple [`CREATE SOURCE`](/sql/create-source) commands can use the same connection so that authentication and other common parameters are defined in a single place.

## Syntax

{{< diagram "create-connection.svg" >}}

{{< tabs tabID="1" >}}
{{< tab "Kafka">}}

Connect to a Kafka server.

Field | Value | Description
-|-|-
`BROKER`                    | `text`           | Kafka broker. Exclusive with `BROKERS`.
`BROKERS`                   | `text[]`         | Kafka brokers. Exclusive with `BROKER`.
`SSL CERTIFICATE AUTHORITY` | secret or `text` | Optional. Root certificate. Defaults to system.
`SSL CERTIFICATE`           | secret or `text` | Optional. Client SSL certificate.
`SSL KEY`                   | secret           | Optional. Client SSL key.
`SASL MECHANISMS`           | `text`           | Optional. SASL mechanisms.
`SASL PASSWORD`             | secret           | Optional. SASL password.
`SASL USERNAME`             | secret or `text` | Optional. SASL username.

{{< /tab >}}
{{< tab "Confluent Schema Registry">}}

Connect to a Confluent Schema Registry.

Field | Value | Description
-|-|-
`URL`                       | `text`           | Schema registry URL.
`SSL CERTIFICATE AUTHORITY` | secret or `text` | Optional. Root certificate. Defaults to system.
`SSL CERTIFICATE`           | secret or `text` | Optional. Client SSL certificate.
`SSL KEY`                   | secret           | Optional. Client SSL key.
`PASSWORD`                  | secret           | Optional. HTTP password.
`USERNAME`                  | secret or `text` | Optional. HTTP username.

{{< /tab >}}
{{< tab "Postgres">}}

Connect to a Postgres server.

Field | Value | Description
-|-|-
`DATABASE`                  | `text`           | Target database.
`HOST`                      | `text`           | Database hostname.
`PORT`                      | `int4`           | Optional. Database port. Defaults to `5432`.
`PASSWORD`                  | secret           | Optional. Password for the connection
`SSH TUNNEL`                | `text`           | `SSH TUNNEL` connection name.
`SSL CERTIFICATE AUTHORITY` | secret or `text` | Optional. Root certificate. Defaults to system.
`SSL MODE`                  | `text`           | Optional. Enables SSL connections if set to `require`, `verify_ca`, or `verify_full`. Defaults to `disable`.
`SSL CERTIFICATE`           | secret or `text` | Optional. Client SSL certificate.
`SSL KEY`                   | secret           | Optional. Client SSL key.
`USER`                      | `text`           | Database username.

{{< /tab >}}
{{< tab "SSH tunnel">}}

Connect to a SSH bastion host.

Field | Value | Description
-|-|-
`HOST` | `text` | Hostname for the connection
`PORT` | `int4` | Port for the connection.
`USER` | `text` | Username for the connection.

{{< /tab >}}
{{< /tabs >}}
