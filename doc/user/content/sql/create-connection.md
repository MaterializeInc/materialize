---
title: "CREATE CONNECTION"
description: "`CREATE CONNECTION` describes how to connect and authenticate to an external system in Materialize"
pagerank: 40
menu:
  main:
    parent: 'commands'

---

A connection describes how to connect and authenticate to an external system you want Materialize to read data from. Once created, a connection is **reusable** across multiple [`CREATE SOURCE`](/sql/create-source) and [`CREATE SINK`](/sql/create-sink) connections.

To use credentials that contain sensitive information (like passwords and SSL keys) in a connection, you must first [create secrets](/sql/create-secret) to securely store each credential in Materialize's secret management system. Credentials that are generally not sensitive (like usernames and SSL certificates) can be specified as plain `text`, or also stored as secrets.

## Syntax

{{< diagram "create-connection.svg" >}}

## Confluent Schema Registry

A Confluent Schema Registry connection establishes a link to a [Confluent Schema
Registry] server.

You can use Confluent Schema Registry connections in the [`FORMAT`] clause of
[`CREATE SOURCE`] and [`CREATE SINK`].

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:| ------------
`URL`                       | `text`           | ✓        | The schema registry URL.
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | The absolute path to the certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`SSL CERTIFICATE`           | secret or `text` | ✓        | Your SSL certificate in PEM format. Required for SSL client authentication.
`SSL KEY`                   | secret           | ✓        | Your SSL certificate's key in PEM format. Required for SSL client authentication.
`PASSWORD`                  | secret           |          | The password used to connect to the schema registry with basic HTTP authentication. This is compatible with the `ssl` options, which control the transport between Materialize and the CSR.
`USERNAME`                  | secret or `text` |          | The username used to connect to the schema registry with basic HTTP authentication. This is compatible with the `ssl` options, which control the transport between Materialize and the CSR.

### Example

```sql
CREATE SECRET csr_ssl_crt AS '<CSR_SSL_CRT>';
CREATE SECRET csr_ssl_key AS '<CSR_SSL_KEY>';
CREATE SECRET csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_ssl TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://rp-f00000bar.data.vectorized.cloud:30993',
    SSL KEY = SECRET csr_ssl_key,
    SSL CERTIFICATE = SECRET csr_ssl_crt,
    USERNAME = 'foo',
    PASSWORD = SECRET csr_password
);
```

## Kafka

A Kafka connection establishes a link to a [Kafka] cluster.

You can use Kafka connections to create [Kafka
sources](/sql/create-source/kafka) and [Kafka sinks](/sql/create-sink).

### SSL {#kafka-ssl}

To connect to a Kafka broker that requires [SSL authentication](https://docs.confluent.io/platform/current/kafka/authentication_ssl.html), use the provided options.

#### SSL options

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|------------------
`BROKER`                    | `text`           | ✓        | The Kafka bootstrap server. Exclusive with `BROKERS`.
`BROKERS`                   | `text[]`         |          | A comma-separated list of Kafka bootstrap servers. Exclusive with `BROKER`.
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | The absolute path to the certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`SSL CERTIFICATE`           | secret or `text` | ✓        | Your SSL certificate in PEM format. Required for SSL client authentication.
`SSL KEY`                   | secret           | ✓        | Your SSL certificate's key in PEM format. Required for SSL client authentication.

#### Examples

Create an SSL-authenticated Kafka connection:

```sql
CREATE SECRET kafka_ssl_crt AS '<BROKER_SSL_CRT>';
CREATE SECRET kafka_ssl_key AS '<BROKER_SSL_KEY>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'rp-f00000bar.data.vectorized.cloud:30365',
    SSL KEY = SECRET kafka_ssl_key,
    SSL CERTIFICATE = SECRET kafka_ssl_crt
);
```

Create a connection to multiple Kafka brokers:

```sql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS ('broker1:9092', 'broker2:9092')
);
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

#### Example

```sql
CREATE SECRET kafka_password AS '<BROKER_PASSWORD>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000-kafka.upstash.io:9092',
    SASL MECHANISMS = 'SCRAM-SHA-256',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password
);
```

### Other {#kafka-other}

Field                                   | Value            | Required | Description
----------------------------------------|------------------|:--------:|-------------------------------
`PROGRESS TOPIC`                        | `text`           |          | The name of a topic that Kafka sinks can use to track internal consistency metadata. If this is not specified, a default topic name will be selected.

## Postgres

A Postgres connection establishes a link to a single database of a [PostgreSQL]
server.

You can use Postgres connections to create [Postgres
sources](/sql/create-source/postgres).

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`DATABASE`                  | `text`           | ✓        | Target database.
`HOST`                      | `text`           | ✓        | Database hostname.
`PORT`                      | `integer`        |          | Default: `5432`. Port number to connect to at the server host.
`PASSWORD`                  | secret           |          | Password for the connection
`SSH TUNNEL`                | object name      |          | The name of an [SSH tunnel connection](#ssh-tunnel) through which network traffic should be routed.
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | The absolute path to the certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`SSL MODE`                  | `text`           |          | Default: `disable`. Enables SSL connections if set to `require`, `verify_ca`, or `verify_full`.
`SSL CERTIFICATE`           | secret or `text` |          | Client SSL certificate in PEM format.
`SSL KEY`                   | secret           |          | Client SSL key in PEM format.
`USER`                      | `text`           | ✓        | Database username.

### Examples

Connect directly to a PostgreSQL server:

```sql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    SSL MODE 'require',
    DATABASE 'postgres'
);
```

If your PostgreSQL server is not exposed to the public internet, you can
tunnel the connection through an SSH bastion host:

```sql
CREATE CONNECTION tunnel TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize',
);

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    SSH TUNNEL tunnel,
    DATABASE 'postgres'
);
```

## SSH tunnel

An SSH tunnel connection establishes a link to an SSH bastion server.

You can use SSH tunnel connections in [Postgres connections](#postgres).

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|------------------------------
`HOST`                      | `text`           | ✓        | The hostname of the SSH bastion server.
`PORT`                      | `integer`        | ✓        | The port to connect to.
`USER`                      | `text`           | ✓        | The name of the user to connect as.

### Key pairs

Materialize automatically manages the key pairs for an SSH tunnel connection.
Each connection is associated with two key pairs. The private key for each key
pair is stored securely within your region and cannot be retrieved. The public
key for each key pair is announced in the [`mz_ssh_tunnel_connections`] system
table.

When Materialize connects to the SSH bastion server, it will present both keys
for authentication. You should configure your SSH bastion server to accept both
key pairs to permit key pair rotation without downtime.

To rotate the key pairs associated with a connection, use [`ALTER CONNECTION`].

Materialize currently generates SSH key pairs using the [Ed25519 algorithm],
which is fast, secure, and [recommended by security
professionals][latacora-crypto]. Some legacy SSH servers do not support the
Ed25519 algorithm. You will not be able to use these servers with Materialize's
SSH tunnel connections.

We routinely evaluate the security of the cryptographic algorithms in use in
Materialize. Future versions of Materialize may use a different SSH key
generation algorithm as security best practices evolve.

#### Examples {#ssh-tunnel-example}

Create an SSH tunnel connection:

```sql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize',
);
```

Retrieve the public keys for all SSH tunnel connections:

```sql
SELECT * FROM mz_ssh_tunnel_connections;
| id    | public_key_1                          | public_key_2                          |
|-------|---------------------------------------|---------------------------------------|
| ...   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize   |
```


## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE SOURCE`](/sql/create-source)

[Confluent Schema Registry]: https://docs.confluent.io/platform/current/schema-registry/index.html#sr-overview
[Kafka]: https://kafka.apache.org
[PostgreSQL]: https://www.postgresql.org
[`ALTER CONNECTION`]: /sql/alter-connection
[`CREATE SOURCE`]: /sql/create-source
[`CREATE SINK`]: /sql/create-sink
[`FORMAT`]: /sql/create-source/#formats
[`mz_ssh_tunnel_connections`]: /sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections
[Ed25519 algorithm]: https://ed25519.cr.yp.to
[latacora-crypto]: https://latacora.micro.blog/2018/04/03/cryptographic-right-answers.html
