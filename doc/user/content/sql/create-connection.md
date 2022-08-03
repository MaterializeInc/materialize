---
title: "CREATE CONNECTION"
description: "`CREATE CONNECTION` describes how to connect and authenticate to an external system in Materialize"
menu:
  main:
    parent: 'commands'

---

A connection describes how to connect and authenticate to an external system you want Materialize to read data from. Once created, a connection is **reusable** across multiple [`CREATE SOURCE`](/sql/create-source) statements.

To use credentials that contain sensitive information (like passwords and SSL keys) in a connection, you must first [create a secret](/sql/create-secret) to securely store them in Materialize's secret management system. Credentials that are generally not sensitive (like usernames and SSL certificates) can be specified as plain `text`, or also stored as secrets.

[//]: # "TODO(morsapaes) Adapt once sinks are wired up to use connections."

## Syntax

{{< diagram "create-connection.svg" >}}

## Kafka

### SSL {#kafka-ssl}

To connect to a Kafka broker that requires [SSL authentication](https://docs.confluent.io/platform/current/kafka/authentication_ssl.html), use the provided options.

#### SSL options

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|------------------
`BROKER`                    | `text`           | ✓        | The Kafka bootstrap server. Exclusive with `BROKERS`.
`BROKERS`                   | `text[]`         |          | A comma-separated list of Kafka bootstrap servers. Exclusive with `BROKER`.
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | The absolute path to the certificate authority (CA) certificate. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`SSL CERTIFICATE`           | secret or `text` | ✓        | Your SSL certificate. Required for SSL client authentication.
`SSL KEY`                   | secret           | ✓        | Your SSL certificate's key. Required for SSL client authentication.

##### Example

```sql
CREATE SECRET kafka_ssl_crt AS '<BROKER_SSL_CRT>';
CREATE SECRET kafka_ssl_key AS '<BROKER_SSL_KEY>';

CREATE CONNECTION kafka_connection
  FOR KAFKA
    BROKER 'rp-f00000bar.data.vectorized.cloud:30365',
    SSL KEY = SECRET kafka_ssl_key,
    SSL CERTIFICATE = SECRET kafka_ssl_crt;
```

#### Confluent Schema Registry SSL options {#csr-ssl}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:| ------------
`URL`                       | `text`           | ✓        | The schema registry URL.
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | The absolute path to the certificate authority (CA) certificate. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`SSL CERTIFICATE`           | secret or `text` | ✓        | Your SSL certificate. Required for SSL client authentication.
`SSL KEY`                   | secret           | ✓        | Your SSL certificate's key. Required for SSL client authentication.
`PASSWORD`                  | secret           |          | The password used to connect to the schema registry with basic HTTP authentication. This is compatible with the `ssl` options, which control the transport between Materialize and the CSR.
`USERNAME`                  | secret or `text` |          | The username used to connect to the schema registry with basic HTTP authentication. This is compatible with the `ssl` options, which control the transport between Materialize and the CSR.

##### Example

```sql
CREATE SECRET csr_ssl_crt AS '<CSR_SSL_CRT>';
CREATE SECRET csr_ssl_key AS '<CSR_SSL_KEY>';
CREATE SECRET csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_ssl
  FOR CONFLUENT SCHEMA REGISTRY
    URL 'rp-f00000bar.data.vectorized.cloud:30993',
    SSL KEY = SECRET csr_ssl_key,
    SSL CERTIFICATE = SECRET csr_ssl_crt,
    USERNAME = 'foo',
    PASSWORD = SECRET csr_password;
```

### SASL {#kafka-sasl}

To create a connection to a Kafka broker that requires [SASL authentication](https://docs.confluent.io/platform/current/kafka/authentication_sasl/auth-sasl-overview.html), use the provided options.

#### SASL options

Field                                   | Value            | Required | Description
----------------------------------------|------------------|:--------:|-------------------------------
`BROKER`                                | `text`           | ✓        | The Kafka bootstrap server. Exclusive with `BROKERS`.
`BROKERS`                               | `text[]`         |          | A comma-separated list of Kafka bootstrap servers. Exclusive with `BROKER`.
`SASL MECHANISMS`                       | `text`           | ✓        | The SASL mechanism to use for authentication. Supported: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`.
`SASL USERNAME`                         | secret or `text` | ✓        | Your SASL username, if any. Required if `SASL MECHANISMS` is `PLAIN`.
`SASL PASSWORD`                         | secret           | ✓        | Your SASL password, if any. Required if `SASL MECHANISMS` is `PLAIN`.
`SSL CERTIFICATE AUTHORITY`             | secret or `text` |          | The absolute path to the certificate authority (CA) certificate. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.

##### Example

```sql
CREATE SECRET kafka_password AS '<BROKER_PASSWORD>';

CREATE CONNECTION kafka_connection
  FOR KAFKA
    BROKER 'unique-jellyfish-0000-kafka.upstash.io:9092',
    SASL MECHANISMS = 'SCRAM-SHA-256',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password;
```

## Postgres

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`DATABASE`                  | `text`           | ✓        | Target database.
`HOST`                      | `text`           | ✓        | Database hostname.
`PORT`                      | `int4`           |          | Default: `5432`. Port number to connect to at the server host.
`PASSWORD`                  | secret           |          | Password for the connection
`SSH TUNNEL`                | `text`           |          | `SSH TUNNEL` connection name. See [SSH tunneling](#postgres-ssh).
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | The absolute path to the certificate authority (CA) certificate. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`SSL MODE`                  | `text`           |          | Default: `disable`. Enables SSL connections if set to `require`, `verify_ca`, or `verify_full`.
`SSL CERTIFICATE`           | secret or `text` |          | Client SSL certificate.
`SSL KEY`                   | secret           |          | Client SSL key.
`USER`                      | `text`           | ✓        | Database username.

##### Example

```sql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection
  FOR POSTGRES
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    SSL MODE 'require',
    DATABASE 'postgres';
```

### SSH tunneling {#postgres-ssh}

If your PostgreSQL instance is running in a Virtual Private Cloud (VPC), you can securely connect via an SSH bastion host.

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|------------------------------
`HOST`                      | `text`           | ✓        | Hostname for the connection
`PORT`                      | `int4`           | ✓        | Port for the connection.
`USER`                      | `text`           | ✓        | Username for the connection.

##### Example

```sql
CREAT CONNECTION ssh_connection
  FOR SSH TUNNEL
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT 1;
```

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE SOURCE`](/sql/create-source)
