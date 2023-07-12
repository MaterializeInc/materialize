---
title: "CREATE CONNECTION"
description: "`CREATE CONNECTION` describes how to connect and authenticate to an external system in Materialize"
pagerank: 40
menu:
  main:
    parent: 'commands'

---

A connection describes how to connect and authenticate to an external system you
want Materialize to read data from. Once created, a connection is **reusable**
across multiple [`CREATE SOURCE`](/sql/create-source) and [`CREATE SINK`](/sql/create-sink)
statements.

To use credentials that contain sensitive information (like passwords and SSL
keys) in a connection, you must first [create secrets](/sql/create-secret) to
securely store each credential in Materialize's secret management system.
Credentials that are generally not sensitive (like usernames and SSL
certificates) can be specified as plain `text`, or also stored as secrets.

## Source and sink connections

### Kafka

A Kafka connection establishes a link to a [Kafka] cluster. You can use Kafka
connections to create [sources](/sql/create-source/kafka) and [sinks](/sql/create-sink/kafka/).

#### Syntax {#kafka-syntax}

{{< diagram "create-connection-kafka.svg" >}}

#### Connection options {#kafka-options}

Field                                   | Value            | Required | Description
----------------------------------------|------------------|:--------:|-------------------------------
`BROKER`                                | `text`           | ✓        | The Kafka bootstrap server. Exclusive with `BROKERS`.
`BROKERS`                               | `text[]`         |          | A comma-separated list of Kafka bootstrap servers. Exclusive with `BROKER`.
`PROGRESS TOPIC`                        | `text`           |          | The name of a topic that Kafka sinks can use to track internal consistency metadata. If this is not specified, a default topic name will be selected.

#### Authentication {#kafka-auth}

{{< tabs >}}
{{< tab "SSL">}}

To connect to a Kafka broker that requires [SSL authentication](https://docs.confluent.io/platform/current/kafka/authentication_ssl.html),
use the following options:

##### SSL options {#kafka-auth-ssl-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|------------------
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | The certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`SSL CERTIFICATE`           | secret or `text` | ✓        | Your SSL certificate in PEM format. Required for SSL client authentication.
`SSL KEY`                   | secret           | ✓        | Your SSL certificate's key in PEM format. Required for SSL client authentication.

##### Example {#kafka-auth-ssl-example}

```sql
CREATE SECRET kafka_ssl_crt AS '<BROKER_SSL_CRT>';
CREATE SECRET kafka_ssl_key AS '<BROKER_SSL_KEY>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'rp-f00000bar.data.vectorized.cloud:30365',
    SSL KEY = SECRET kafka_ssl_key,
    SSL CERTIFICATE = SECRET kafka_ssl_crt
);
```

{{< /tab >}}

{{< tab "SASL">}}

To connect to a Kafka broker that requires [SASL authentication](https://docs.confluent.io/platform/current/kafka/authentication_sasl/auth-sasl-overview.html),
use the following options.

##### SASL options {#kafka-auth-sasl-options}

Field                                   | Value            | Required | Description
----------------------------------------|------------------|:--------:|-------------------------------
`SASL MECHANISMS`                       | `text`           | ✓        | The SASL mechanism to use for authentication. Supported: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`.
`SASL USERNAME`                         | secret or `text` | ✓        | Your SASL username, if any. Required if `SASL MECHANISMS` is `PLAIN`.
`SASL PASSWORD`                         | secret           | ✓        | Your SASL password, if any. Required if `SASL MECHANISMS` is `PLAIN`.
`SSL CERTIFICATE AUTHORITY`             | secret or `text` |          | The certificate authority (CA) certificate. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.

##### Example {#kafka-auth-sasl-example}

```sql
CREATE SECRET kafka_password AS '<BROKER_PASSWORD>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000-kafka.upstash.io:9092',
    SASL MECHANISMS = 'SCRAM-SHA-256',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password
);
```

{{< /tab >}}
{{< /tabs >}}

To connect to a Kafka cluster with multiple bootstrap servers, use the `BROKERS`
option:

```sql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS ('broker1:9092', 'broker2:9092')
);
```

#### Network security {#kafka-network-security}

If your Kafka broker is not exposed to the public internet, you can tunnel the
connection through an AWS PrivateLink service or an SSH bastion host.

{{< tabs >}}
{{< tab "AWS PrivateLink">}}

##### Syntax {#kafka-privatelink-syntax}

{{< warning >}}
If your Kafka cluster advertises brokers that are not specified
in the `BROKERS` clause, Materialize will attempt to connect to
those brokers without any tunneling.
{{< /warning >}}

{{< diagram "create-connection-kafka-brokers.svg" >}}

##### `kafka_broker`

{{< diagram "create-connection-kafka-broker-aws-privatelink.svg" >}}

##### `broker_option`

{{< diagram "broker-option.svg" >}}

The `USING` clause specifies that Materialize should connect to the designated
broker via an AWS PrivateLink service. Brokers do not need to be configured the
same way, but the clause must be individually attached to each broker that you
want to connect to via the tunnel.

##### Connection options {#kafka-privatelink-options}

Field                                   | Value            | Required | Description
----------------------------------------|------------------|:--------:|-------------------------------
`AWS PRIVATELINK`                       | object name      | ✓        | The name of an [AWS PrivateLink connection](#aws-privatelink) through which network traffic for this broker should be routed.
`AVAILABILITY ZONE`                     | `text`           |          | The ID of the availability zone of the AWS PrivateLink service in which the broker is accessible. If unspecified, traffic will be routed to each availability zone declared in the [AWS PrivateLink connection](#aws-privatelink) in sequence until the correct availability zone for the broker is discovered. If specified, Materialize will always route connections via the specified availability zone.
`PORT`                                  | `integer`        |          | The port of the AWS PrivateLink service to connect to. Defaults to the broker's port.

##### Example {#kafka-privatelink-example}

Suppose you have the following infrastructure:

  * A Kafka cluster consisting of two brokers named `broker1` and `broker2`,
    both listening on port 9092.

  * A Network Load Balancer that forwards port 9092 to `broker1:9092` and port
    9093 to `broker2:9092`.

  * A PrivateLink endpoint service attached to the load balancer.

You can create a connection to this Kafka broker in Materialize like so:

```sql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS (
        'broker1:9092' USING AWS PRIVATELINK privatelink_svc,
        'broker2:9092' USING AWS PRIVATELINK privatelink_svc (PORT 9093)
    )
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}

##### Syntax {#kafka-ssh-syntax}

{{< warning >}}
If your Kafka cluster advertises brokers that are not specified
in the `BROKERS` clause, Materialize will attempt to connect to
those brokers without any tunneling.
{{< /warning >}}

{{< diagram "create-connection-kafka-brokers.svg" >}}

##### `kafka_broker`

{{< diagram "create-connection-kafka-broker-ssh-tunnel.svg" >}}

The `USING` clause specifies that Materialize should connect to the designated
broker via an SSH bastion server. Brokers do not need to be configured the same
way, but the clause must be individually attached to each broker that you want
to connect to via the tunnel.

##### Connection options {#kafka-ssh-options}

Field           | Value            | Required | Description
----------------|------------------|:--------:|-------------------------------
`SSH TUNNEL`    | object name      | ✓        | The name of an [SSH tunnel connection](#ssh-tunnel) through which network traffic for this broker should be routed.

##### Example {#kafka-ssh-example}

```sql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);

CREATE CONNECTION kafka_connection TO KAFKA (
BROKERS (
    'broker1:9092' USING SSH TUNNEL ssh_connection,
    'broker2:9092' USING SSH TUNNEL ssh_connection
    -- Add all Kafka brokers
    )
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}
{{< /tabs >}}

### Confluent Schema Registry

A Confluent Schema Registry connection establishes a link to a [Confluent Schema
Registry] server. You can use Confluent Schema Registry connections in the
[`FORMAT`] clause of [`CREATE SOURCE`] and [`CREATE SINK`] statements.

#### Syntax {#csr-syntax}

{{< diagram "create-connection-csr.svg" >}}

#### Connection options {#csr-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:| ------------
`URL`                       | `text`           | ✓        | The schema registry URL.
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | The certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`SSL CERTIFICATE`           | secret or `text` | ✓        | Your SSL certificate in PEM format. Required for SSL client authentication.
`SSL KEY`                   | secret           | ✓        | Your SSL certificate's key in PEM format. Required for SSL client authentication.
`PASSWORD`                  | secret           |          | The password used to connect to the schema registry with basic HTTP authentication. This is compatible with the `ssl` options, which control the transport between Materialize and the CSR.
`USERNAME`                  | secret or `text` |          | The username used to connect to the schema registry with basic HTTP authentication. This is compatible with the `ssl` options, which control the transport between Materialize and the CSR.

#### Examples {#csr-example}

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

#### Network security {#csr-network-security}

If your Confluent Schema Registry server is not exposed to the public internet,
you can tunnel the connection through an AWS PrivateLink service or an SSH
bastion host.

{{< tabs >}}
{{< tab "AWS PrivateLink">}}

##### Connection options {#csr-privatelink-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`AWS PRIVATELINK`           | object name      | ✓        | The name of an [AWS PrivateLink connection](#aws-privatelink) through which network traffic should be routed.

##### Example {#csr-privatelink-example}

```sql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);

CREATE CONNECTION csr_privatelink TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    AWS PRIVATELINK privatelink_svc
);
```

{{< /tab >}}
{{< tab "SSH tunnel">}}

##### Connection options {#csr-ssh-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`SSH TUNNEL`                | object name      | ✓        | The name of an [SSH tunnel connection](#ssh-tunnel) through which network traffic should be routed.

##### Example {#csr-ssh-example}

```sql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);

CREATE CONNECTION csr_privatelink TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    SSH TUNNEL ssh_connection
);
```

{{< /tab >}}
{{< /tabs >}}

### PostgreSQL

A Postgres connection establishes a link to a single database of a
[PostgreSQL] server. You can use Postgres connections to create [sources](/sql/create-source/postgres).

#### Syntax {#postgres-syntax}

{{< diagram "create-connection-postgres.svg" >}}

#### Connection options {#postgres-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`HOST`                      | `text`           | ✓        | Database hostname.
`PORT`                      | `integer`        |          | Default: `5432`. Port number to connect to at the server host.
`DATABASE`                  | `text`           | ✓        | Target database.
`USER`                      | `text`           | ✓        | Database username.
`PASSWORD`                  | secret           |          | Password for the connection
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | The certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`SSL MODE`                  | `text`           |          | Default: `disable`. Enables SSL connections if set to `require`, `verify_ca`, or `verify_full`.
`SSL CERTIFICATE`           | secret or `text` |          | Client SSL certificate in PEM format.
`SSL KEY`                   | secret           |          | Client SSL key in PEM format.

#### Example {#postgres-example}

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

#### Network security {#postgres-network-security}

If your PostgreSQL server is not exposed to the public internet, you can tunnel
the connection through an AWS PrivateLink service or an SSH bastion host.

{{< tabs >}}
{{< tab "AWS PrivateLink">}}

##### Connection options {#postgres-privatelink-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`AWS PRIVATELINK`           | object name      | ✓        | The name of an [AWS PrivateLink connection](#aws-privatelink) through which network traffic should be routed.

##### Example {#postgres-privatelink-example}

```sql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
   SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
   AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass,
    AWS PRIVATELINK privatelink_svc
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}

##### Connection options {#postgres-ssh-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`SSH TUNNEL`                | object name      | ✓        | The name of an [SSH tunnel connection](#ssh-tunnel) through which network traffic should be routed.

##### Example {#postgres-ssh-example}

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

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}
{{< /tabs >}}

## Network security connections

### AWS PrivateLink

{{< private-preview />}}

An AWS PrivateLink connection establishes a link to an [AWS PrivateLink] service.
You can use AWS PrivateLink connections in [Confluent Schema Registry connections](#confluent-schema-registry),
[Kafka connections](#kafka), and [Postgres connections](#postgresql).

#### Syntax {#aws-privatelink-syntax}

{{< diagram "create-connection-aws-privatelink.svg" >}}

#### Connection options {#aws-privatelink-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:| ------------
`SERVICE NAME`              | `text`           | ✓        | The name of the AWS PrivateLink service.
`AVAILABILITY ZONES`        | `text[]`         | ✓        | The IDs of the AWS availability zones in which the service is accessible.

#### Permissions {#aws-privatelink-permissions}

Materialize assigns a unique principal to each AWS PrivateLink connection in
your region using an Amazon Resource Name of the
following form:

```
arn:aws:iam::664411391173:role/mz_<REGION-ID>_<CONNECTION-ID>
```

After creating the connection, you must configure the AWS PrivateLink service
to accept connections from the AWS principal Materialize will connect as. The
principals for AWS PrivateLink connections in your region are stored in
the [`mz_aws_privatelink_connections`](/sql/system-catalog/mz_catalog/#mz_aws_privatelink_connections)
system table.

```sql
SELECT * FROM mz_aws_privatelink_connections;
```
```
   id   |                                 principal
--------+---------------------------------------------------------------------------
 u1     | arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
 u7     | arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u7
```

For more details on configuring a trusted principal for your AWS PrivateLink service,
see the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#add-remove-permissions).

{{< warning >}}
Do **not** grant access to the root principal for the Materialize AWS account.
Doing so will allow any Materialize customer to create a connection to your
AWS PrivateLink service.
{{< /warning >}}

#### Accepting connection requests {#aws-privatelink-requests}

If your AWS PrivateLink service is configured to require acceptance of
connection requests, you must additionally approve the connection request from
Materialize after creating the connection. For more details on manually
accepting connection requests, see the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

#### Example {#aws-privatelink-example}

```sql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

### SSH tunnel

An SSH tunnel connection establishes a link to an SSH bastion server. You can
use SSH tunnel connections in [Kafka connections](#kafka), and
[Postgres connections](#postgresql).

#### Syntax {#ssh-tunnel-syntax}

{{< diagram "create-connection-ssh-tunnel.svg" >}}

#### Connection options {#ssh-tunnel-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|------------------------------
`HOST`                      | `text`           | ✓        | The hostname of the SSH bastion server.
`PORT`                      | `integer`        | ✓        | The port to connect to.
`USER`                      | `text`           | ✓        | The name of the user to connect as.

#### Key pairs {#ssh-tunnel-keypairs}

Materialize automatically manages the key pairs for an SSH tunnel connection.
Each connection is associated with two key pairs. The private key for each key
pair is stored securely within your region and cannot be retrieved. The public
key for each key pair is stored in the [`mz_ssh_tunnel_connections`] system
table.

When Materialize connects to the SSH bastion server, it presents both keys for
authentication. To allow key pair rotation without downtime, you should
configure your SSH bastion server to accept both key pairs. You can
then **rotate the key pairs** using [`ALTER CONNECTION`].

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
    USER 'materialize'
);
```

Retrieve the public keys for all SSH tunnel connections:

```sql
SELECT * FROM mz_ssh_tunnel_connections;
```
```
 id    | public_key_1                          | public_key_2
-------+---------------------------------------+---------------------------------------
 ...   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize
```

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all connections and secrets used in the connection definition.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE SINK`](/sql/create-sink)

[AWS PrivateLink]: https://aws.amazon.com/privatelink/
[Confluent Schema Registry]: https://docs.confluent.io/platform/current/schema-registry/index.html#sr-overview
[Kafka]: https://kafka.apache.org
[PostgreSQL]: https://www.postgresql.org
[`ALTER CONNECTION`]: /sql/alter-connection
[`CREATE SOURCE`]: /sql/create-source
[`CREATE SINK`]: /sql/create-sink
[`FORMAT`]: /sql/create-source/#formats
[`mz_aws_privatelink_connections`]: /sql/system-catalog/mz_catalog/#mz_aws_privatelink_connections
[`mz_connections`]: /sql/system-catalog/mz_catalog/#mz_connections
[`mz_ssh_tunnel_connections`]: /sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections
[Ed25519 algorithm]: https://ed25519.cr.yp.to
[latacora-crypto]: https://latacora.micro.blog/2018/04/03/cryptographic-right-answers.html
