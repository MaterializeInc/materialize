---
title: "CREATE CONNECTION"
description: "`CREATE CONNECTION` describes how to connect and authenticate to an external system in Materialize"
pagerank: 40
menu:
  main:
    parent: 'commands'

---

A connection describes how to connect and authenticate to an external system you
want Materialize to read from or write to. Once created, a connection
is **reusable** across multiple [`CREATE SOURCE`](/sql/create-source) and
[`CREATE SINK`](/sql/create-sink) statements.

To use credentials that contain sensitive information (like passwords and SSL
keys) in a connection, you must first [create secrets](/sql/create-secret) to
securely store each credential in Materialize's secret management system.
Credentials that are generally not sensitive (like usernames and SSL
certificates) can be specified as plain `text`, or also stored as secrets.

## Source and sink connections

### AWS

An Amazon Web Services (AWS) connection provides Materialize with access to an
Identity and Access Management (IAM) user or role in your AWS account. You can
use AWS connections to perform [bulk exports to Amazon S3](/serve-results/s3/),
perform [authentication with an Amazon MSK cluster](#kafka-aws-connection), or
perform [authentication with an Amazon RDS MySQL database](#mysql-aws-connection).

{{< diagram "create-connection-aws.svg" >}}

#### Connection options {#aws-options}

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------
| `ENDPOINT`                                | `text`           | *Advanced.* Override the default AWS endpoint URL. Allows targeting S3-compatible services like MinIO.
| `REGION`                                  | `text`           | The AWS region to connect to.
| `ACCESS KEY ID`                           | secret or `text` | The access key ID to connect with. Triggers credentials-based authentication.<br><br><strong>Warning!</strong> Use of credentials-based authentication is deprecated. AWS strongly encourages the use of role assumption-based authentication instead.
| `SECRET ACCESS KEY`                       | secret           | The secret access key corresponding to the specified access key ID.<br><br>Required and only valid when `ACCESS KEY ID` is specified.
| `SESSION TOKEN`                           | secret or `text` | The session token corresponding to the specified access key ID.<br><br>Only valid when `ACCESS KEY ID` is specified.
| `ASSUME ROLE ARN`                         | `text`           | The Amazon Resource Name (ARN) of the IAM role to assume. Triggers role assumption-based authentication.
| `ASSUME ROLE SESSION NAME`                | `text`           | The session name to use when assuming the role.<br><br>Only valid when `ASSUME ROLE ARN` is specified.

#### `WITH` options {#aws-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Whether [connection validation](#connection-validation) should be performed on connection creation.<br><br>Defaults to `false`.

#### Permissions {#aws-permissions}

{{< warning >}}
Failing to constrain the external ID in your role trust policy will allow
other Materialize customers to assume your role and use AWS privileges you
have granted the role!
{{< /warning >}}

When using role assumption-based authentication, you must configure a [trust
policy] on the IAM role that permits Materialize to assume the role.

Materialize always uses the following IAM principal to assume the role:

```
arn:aws:iam::664411391173:role/MaterializeConnection
```

Materialize additionally generates an [external ID] which uniquely identifies
your AWS connection across all Materialize regions. To ensure that other
Materialize customers cannot assume your role, your IAM trust policy **must**
constrain access to only the external ID that Materialize generates for the
connection:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::664411391173:role/MaterializeConnection"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<EXTERNAL ID FOR CONNECTION>"
                }
            }
        }
    ]
}
```

You can retrieve the external ID for the connection, as well as an example trust
policy, by querying the
[`mz_internal.mz_aws_connections`](/sql/system-catalog/mz_internal/#mz_aws_connections)
table:

```mzsql
SELECT id, external_id, example_trust_policy FROM mz_internal.mz_aws_connections;
```

#### Examples {#aws-examples}

{{< tabs >}}
{{< tab "Role assumption">}}

In this example, we have created the following IAM role for Materialize to
assume:

<table>
<tr>
<th>Name</th>
<th>AWS account ID</th>
<th>Trust policy</th>
<tr>
<td><code>WarehouseExport</code></td>
<td>000000000000</td>
<td>

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::000000000000:role/MaterializeConnection"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "mz_00000000-0000-0000-0000-000000000000_u0"
                }
            }
        }
    ]
}
```

</td>
</tr>
</table>

To create an AWS connection that will assume the `WarehouseExport` role:

```mzsql
CREATE CONNECTION aws_role_assumption TO AWS (
    ASSUME ROLE ARN = 'arn:aws:iam::000000000000:role/WarehouseExport'
);
```
{{< /tab >}}

{{< tab "Credentials">}}
{{< warning >}}

Use of credentials-based authentication is deprecated.  AWS strongly encourages
the use of role assumption-based authentication instead.

{{< /warning >}}

To create an AWS connection that uses static access key credentials:

```mzsql
CREATE SECRET aws_secret_access_key AS '...';
CREATE CONNECTION aws_credentials TO AWS (
    ACCESS KEY ID = 'ASIAV2KIV5LPTG6HGXG6',
    SECRET ACCESS KEY = SECRET aws_secret_access_key
);
```
{{< /tab >}}

{{< /tabs >}}

### Kafka

A Kafka connection establishes a link to a [Kafka] cluster. You can use Kafka
connections to create [sources](/sql/create-source/kafka) and [sinks](/sql/create-sink/kafka/).

#### Syntax {#kafka-syntax}

{{< diagram "create-connection-kafka.svg" >}}

#### Connection options {#kafka-options}

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------
| `BROKER`                                  | `text`           | The Kafka bootstrap server.<br><br>Exactly one of `BROKER`, `BROKERS`, or `AWS PRIVATELINK` must be specified.
| `BROKERS`                                 | `text[]`         | A comma-separated list of Kafka bootstrap servers.<br><br>Exactly one of `BROKER`, `BROKERS`, or `AWS PRIVATELINK` must be specified.
| `SECURITY PROTOCOL`                       | `text`           | The security protocol to use: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`.<br><br>Defaults to `SASL_SSL` if any `SASL ...` options are specified or if the `AWS CONNECTION` option is specified, otherwise defaults to `SSL`.
| `SASL MECHANISMS`                         | `text`           | The SASL mechanism to use for authentication: `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`. Despite the name, this option only allows a single mechanism to be specified.<br><br>Required if the security protocol is `SASL_PLAINTEXT` or `SASL_SSL`.<br>Cannot be specified if `AWS CONNECTION` is specified.
| `SASL USERNAME`                           | secret or `text` | Your SASL username.<br><br>Required and only valid when the security protocol is `SASL_PLAINTEXT` or `SASL_SSL`.
| `SASL PASSWORD`                           | secret           | Your SASL password.<br><br>Required and only valid when the security protocol is `SASL_PLAINTEXT` or `SASL_SSL`.
| `SSL CERTIFICATE AUTHORITY`               | secret or `text` | The certificate authority (CA) certificate in PEM format. Used to validate the brokers' TLS certificates. If unspecified, uses the system's default CA certificates.<br><br>Only valid when the security protocol is `SSL` or `SASL_SSL`.
| `SSL CERTIFICATE`                         | secret or `text` | Your TLS certificate in PEM format for SSL client authentication. If unspecified, no client authentication is performed.<br><br>Only valid when the security protocol is `SSL` or `SASL_SSL`.
| `SSL KEY`                                 | secret           | Your TLS certificate's key in PEM format.<br><br>Required and only valid when `SSL CERTIFICATE` is specified.
| `SSH TUNNEL`                              | object name      | The name of an [SSH tunnel connection](#ssh-tunnel) to route network traffic through by default.
| `AWS CONNECTION` <a name="kafka-aws-connection"></a>  | object name      | The name of an [AWS connection](#aws) to use when performing IAM authentication with an Amazon MSK cluster.<br><br>Only valid if the security protocol is `SASL_PLAINTEXT` or `SASL_SSL`.
| `AWS PRIVATELINK`                         | object name      | The name of an [AWS PrivateLink connection](#aws-privatelink) to route network traffic through. <br><br>Exactly one of `BROKER`, `BROKERS`, or `AWS PRIVATELINK` must be specified.
| `PROGRESS TOPIC`                          | `text`           | The name of a topic that Kafka sinks can use to track internal consistency metadata. Default: `_materialize-progress-{REGION ID}-{CONNECTION ID}`.
| `PROGRESS TOPIC REPLICATION FACTOR`       | `int`            | {{< warn-if-unreleased-inline "v0.106" >}} The partition count to use when creating the progress topic (if the Kafka topic does not already exist).<br>Default: Broker's default.

#### `WITH` options {#kafka-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Whether [connection validation](#connection-validation) should be performed on connection creation.<br><br>Defaults to `true`.

To connect to a Kafka cluster with multiple bootstrap servers, use the `BROKERS`
option:

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS ('broker1:9092', 'broker2:9092')
);
```

#### Security protocol examples {#kafka-auth}

{{< tabs >}}
{{< tab "PLAINTEXT">}}
{{< warning >}}
It is insecure to use the `PLAINTEXT` security protocol unless
you are using a [network security connection](#network-security-connections)
to tunnel into a private network, as shown below.
{{< /warning >}}
```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.prd.cloud.redpanda.com:9092',
    SECURITY PROTOCOL = 'PLAINTEXT',
    SSH TUNNEL ssh_connection
);
```
{{< /tab >}}

{{< tab "SSL">}}
With both TLS encryption and TLS client authentication:
```mzsql
CREATE SECRET kafka_ssl_cert AS '-----BEGIN CERTIFICATE----- ...';
CREATE SECRET kafka_ssl_key AS '-----BEGIN PRIVATE KEY----- ...';
CREATE SECRET ca_cert AS '-----BEGIN CERTIFICATE----- ...';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'rp-f00000bar.cloud.redpanda.com:30365',
    SECURITY PROTOCOL = 'SSL'
    SSL CERTIFICATE = SECRET kafka_ssl_cert,
    SSL KEY = SECRET kafka_ssl_key,
    -- Specifying a certificate authority is only required if your cluster's
    -- certificates are not issued by a CA trusted by the Mozilla root store.
    SSL CERTIFICATE AUTHORITY = SECRET ca_cert
);
```

With only TLS encryption:
{{< warning >}}
It is insecure to use TLS encryption with no authentication unless
you are using a [network security connection](#network-security-connections)
to tunnel into a private network as shown below.
{{< /warning >}}
```mzsql
CREATE SECRET ca_cert AS '-----BEGIN CERTIFICATE----- ...';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER = 'rp-f00000bar.cloud.redpanda.com:30365',
    SECURITY PROTOCOL = 'SSL',
    SSH TUNNEL ssh_connection,
    -- Specifying a certificate authority is only required if your cluster's
    -- certificates are not issued by a CA trusted by the Mozilla root store.
    SSL CERTIFICATE AUTHORITY = SECRET ca_cert
);
```
{{< /tab >}}

{{< tab "SASL_PLAINTEXT">}}
{{< warning >}}
It is insecure to use the `SASL_PLAINTEXT` security protocol unless
you are using a [network security connection](#network-security-connections)
to tunnel into a private network, as shown below.
{{< /warning >}}

```mzsql
CREATE SECRET kafka_password AS '...';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9092',
    SECURITY PROTOCOL = 'SASL_PLAINTEXT',
    SASL MECHANISMS = 'SCRAM-SHA-256', -- or `PLAIN` or `SCRAM-SHA-512`
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password,
    SSH TUNNEL ssh_connection
);
```
{{< /tab >}}

{{< tab "SASL_SSL">}}
```mzsql
CREATE SECRET kafka_password AS '...';
CREATE SECRET ca_cert AS '-----BEGIN CERTIFICATE----- ...';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9092',
    SECURITY PROTOCOL = 'SASL_SSL',
    SASL MECHANISMS = 'SCRAM-SHA-256', -- or `PLAIN` or `SCRAM-SHA-512`
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password,
    -- Specifying a certificate authority is only required if your cluster's
    -- certificates are not issued by a CA trusted by the Mozilla root store.
    SSL CERTIFICATE AUTHORITY = SECRET ca_cert
);
```
{{< /tab >}}

{{< tab "AWS IAM">}}

```mzsql
CREATE CONNECTION aws_msk TO AWS (
    ASSUME ROLE ARN = 'arn:aws:iam::000000000000:role/MaterializeMSK'
);

CREATE CONNECTION kafka_msk TO KAFKA (
    BROKER 'msk.mycorp.com:9092',
    SECURITY PROTOCOL = 'SASL_SSL',
    AWS CONNECTION = aws_msk
);
```
{{< /tab >}}
{{< /tabs >}}

#### Network security {#kafka-network-security}

If your Kafka broker is not exposed to the public internet, you can tunnel the
connection through an AWS PrivateLink service or an SSH bastion host.

{{< tabs >}}
{{< tab "AWS PrivateLink">}}

Depending on the hosted service you are connecting to, you might need to specify
a PrivateLink connection [per advertised broker](#kafka-privatelink-syntax)
(e.g. Amazon MSK), or a single [default PrivateLink connection](#kafka-privatelink-default) (e.g. Redpanda Cloud).

##### Broker connection syntax {#kafka-privatelink-syntax}

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

##### Broker connection options {#kafka-privatelink-options}

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

```mzsql
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

##### Default connections {#kafka-privatelink-default}

[Redpanda Cloud](/ingest-data/redpanda/redpanda-cloud/)) does not require
listing every broker individually. In this case, you should specify a
PrivateLink connection and the port of the bootstrap server instead.

##### Default connection syntax {#kafka-privatelink-default-syntax}

{{< diagram "create-connection-kafka-default-aws-privatelink.svg" >}}

##### Default connection options {#kafka-privatelink-default-options}

Field                                   | Value            | Required | Description
----------------------------------------|------------------|:--------:|-------------------------------
`AWS PRIVATELINK`                       | object name      | ✓        | The name of an [AWS PrivateLink connection](#aws-privatelink) through which network traffic for this broker should be routed.
`PORT`                                  | `integer`        |          | The port of the AWS PrivateLink service to connect to. Defaults to the broker's port.

##### Example {#kafka-privatelink-default-example}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1')
);

CREATE CONNECTION kafka_connection TO KAFKA (
    AWS PRIVATELINK (PORT 30292)
    SECURITY PROTOCOL = 'SASL_PLAINTEXT',
    SASL MECHANISMS = 'SCRAM-SHA-256',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET red_panda_password
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}

##### Syntax {#kafka-ssh-syntax}

{{< warning >}}
If you do not specify a default [`SSH TUNNEL`](#kafka-options) and your Kafka
cluster advertises brokers that are not listed in the `BROKERS` clause,
Materialize will attempt to connect to those brokers without any tunneling.
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

Using a default SSH tunnel:

```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'broker1:9092',
    SSH TUNNEL ssh_connection
);
```

Using different SSH tunnels for each broker, with a default for brokers that are
not listed:

```mzsql
CREATE CONNECTION ssh1 TO SSH TUNNEL (HOST 'ssh1', ...);
CREATE CONNECTION ssh2 TO SSH TUNNEL (HOST 'ssh2', ...);

CREATE CONNECTION kafka_connection TO KAFKA (
BROKERS (
    'broker1:9092' USING SSH TUNNEL ssh1,
    'broker2:9092' USING SSH TUNNEL ssh2
    )
    SSH TUNNEL ssh_1
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

| <div style="min-width:220px">Field</div>    | Value            | Description
| --------------------------------------------|------------------|------------
| `URL`                                       | `text`           | The schema registry URL.<br><br>Required.
| `USERNAME`                                  | secret or `text` | The username to use for basic HTTP authentication.
| `PASSWORD`                                  | secret           | The password to use for basic HTTP authentication.<br><br>Required and only valid if `USERNAME` is specified.
| `SSL CERTIFICATE`                           | secret or `text` | Your TLS certificate in PEM format for TLS client authentication. If unspecified, no TLS client authentication is performed.<br><br>Only respected if the URL uses the `https` protocol.
| `SSL KEY`                                   | secret           | Your TLS certificate's key in PEM format.<br><br>Required and only valid if `SSL CERTIFICATE` is specified.
| `SSL CERTIFICATE AUTHORITY`                 | secret or `text` | The certificate authority (CA) certificate in PEM format. Used to validate the server's TLS certificate. If unspecified, uses the system's default CA certificates.<br><br>Only respected if the URL uses the `https` protocol.

#### `WITH` options {#csr-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Default: `true`. Whether [connection validation](#connection-validation) should be performed on connection creation.

#### Examples {#csr-example}

Using username and password authentication with TLS encryption:

```mzsql
CREATE SECRET csr_password AS '...';
CREATE SECRET ca_cert AS '-----BEGIN CERTIFICATE----- ...';

CREATE CONNECTION csr_basic TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://rp-f00000bar.cloud.redpanda.com:30993',
    USERNAME = 'foo',
    PASSWORD = SECRET csr_password
    -- Specifying a certificate authority is only required if your cluster's
    -- certificates are not issued by a CA trusted by the Mozilla root store.
    SSL CERTIFICATE AUTHORITY = SECRET ca_cert
);
```

Using TLS for encryption and authentication:

```mzsql
CREATE SECRET csr_ssl_cert AS '-----BEGIN CERTIFICATE----- ...';
CREATE SECRET csr_ssl_key AS '-----BEGIN PRIVATE KEY----- ...';
CREATE SECRET ca_cert AS '-----BEGIN CERTIFICATE----- ...';

CREATE CONNECTION csr_ssl TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://rp-f00000bar.cloud.redpanda.com:30993',
    SSL CERTIFICATE = SECRET csr_ssl_cert,
    SSL KEY = SECRET csr_ssl_key,
    -- Specifying a certificate authority is only required if your cluster's
    -- certificates are not issued by a CA trusted by the Mozilla root store.
    SSL CERTIFICATE AUTHORITY = SECRET ca_cert
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

```mzsql
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

```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);

CREATE CONNECTION csr_ssh TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    SSH TUNNEL ssh_connection
);
```

{{< /tab >}}
{{< /tabs >}}

### MySQL

A MySQL connection establishes a link to a [MySQL] server. You can use
MySQL connections to create [sources](/sql/create-source/mysql).

#### Syntax {#mysql-syntax}

{{< diagram "create-connection-mysql.svg" >}}

#### Connection options {#mysql-options}

Field                                                | Value            | Required | Description
-----------------------------------------------------|------------------|:--------:|-----------------------------
`HOST`                                               | `text`           | ✓        | Database hostname.
`PORT`                                               | `integer`        |          | Default: `3306`. Port number to connect to at the server host.
`USER`                                               | `text`           | ✓        | Database username.
`PASSWORD`                                           | secret           |          | Password for the connection.
`SSL CERTIFICATE AUTHORITY`                          | secret or `text` |          | The certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`AWS CONNECTION` <a name="mysql-aws-connection"></a> | object name      |          | The name of an [AWS connection](#aws) to use when performing IAM authentication with an Amazon RDS MySQL cluster.<br><br>Only valid if `SSL MODE` is set to `required`, `verify_ca`, or `verify_identity`. <br><br>Incompatible with `PASSWORD` being set.
`SSL MODE`                                           | `text`           |          | Default: `disabled`. Enables SSL connections if set to `required`, `verify_ca`, or `verify_identity`. See the [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/using-encrypted-connections.html) for more details.
`SSL CERTIFICATE`                                    | secret or `text` |          | Client SSL certificate in PEM format.
`SSL KEY`                                            | secret           |          | Client SSL key in PEM format.

#### `WITH` options {#mysql-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Default: `true`. Whether [connection validation](#connection-validation) should be performed on connection creation.

#### Example {#mysql-example}

```mzsql
CREATE SECRET mysqlpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'root',
    PASSWORD SECRET mysqlpass
);
```

#### Network security {#mysql-network-security}

If your MySQL server is not exposed to the public internet, you can tunnel
the connection through an AWS PrivateLink service or an SSH bastion host.

{{< tabs >}}
{{< tab "AWS PrivateLink">}}

##### Connection options {#mysql-privatelink-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`AWS PRIVATELINK`           | object name      | ✓        | The name of an [AWS PrivateLink connection](#aws-privatelink) through which network traffic should be routed.

##### Example {#mysql-privatelink-example}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
   SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
   AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'root',
    PASSWORD SECRET mysqlpass,
    AWS PRIVATELINK privatelink_svc
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}

##### Connection options {#mysql-ssh-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`SSH TUNNEL`                | object name      | ✓        | The name of an [SSH tunnel connection](#ssh-tunnel) through which network traffic should be routed.

##### Example {#mysql-ssh-example}

```mzsql
CREATE CONNECTION tunnel TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize'
);

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    SSH TUNNEL ssh_connection
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}

{{< tab "AWS IAM">}}

##### Example {#mysql-aws-connection-example}

```mzsql
CREATE CONNECTION aws_rds_mysql TO AWS (
    ASSUME ROLE ARN = 'arn:aws:iam::000000000000:role/MaterializeRDS'
);

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'root',
    AWS CONNECTION aws_rds_mysql,
    SSL MODE 'verify_identity'
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
`PASSWORD`                  | secret           |          | Password for the connection.
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | The certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`SSL MODE`                  | `text`           |          | Default: `disable`. Enables SSL connections if set to `require`, `verify_ca`, or `verify_full`.
`SSL CERTIFICATE`           | secret or `text` |          | Client SSL certificate in PEM format.
`SSL KEY`                   | secret           |          | Client SSL key in PEM format.

#### `WITH` options {#postgres-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Default: `true`. Whether [connection validation](#connection-validation) should be performed on connection creation.

#### Example {#postgres-example}

```mzsql
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

```mzsql
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

```mzsql
CREATE CONNECTION tunnel TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize'
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

### SQL Server

{{< private-preview />}}

A SQL Server connection establishes a link to a single database of a
[SQL Server] instance. You can use SQL Server connections to create [sources](/sql/create-source/sql-server).

#### Syntax {#sql-server-syntax}

{{< diagram "create-connection-sql-server.svg" >}}

#### Connection options {#sql-server-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`HOST`                      | `text`           | ✓        | Database hostname.
`PORT`                      | `integer`        |          | Default: `1433`. Port number to connect to at the server host.
`DATABASE`                  | `text`           | ✓        | Target database.
`USER`                      | `text`           | ✓        | Database username.
`PASSWORD`                  | secret           | ✓        | Password for the connection.
`SSL MODE`                  | `text`           |          | Default: `disabled`. Enables SSL connections if set to `required`, `verify_ca`, or `verify`. See the [SQL Server documentation](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-sql-server-encryption) for more details.
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | One or more client SSL certificates in PEM format.

##### SSL MODE
`disabled` - no encryption.
`required` - encryption required, no certificate validation.
`verify` - encryption required, validate server certificate using OS configured CA.
`verify_ca` - encryption required, validate server certificate using provided CA certificates (requires `SSL CERTIFICATE AUTHORITY`).

#### `WITH` options {#sql-server-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Default: `true`. Whether [connection validation](#connection-validation) should be performed on connection creation.

#### Example {#sql-server-example}

```mzsql
CREATE SECRET sqlserver_pass AS '<SQL_SERVER_PASSWORD>';

CREATE CONNECTION sqlserver_connection TO SQL SERVER (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 1433,
    USER 'SA',
    PASSWORD SECRET sqlserver_pass,
    DATABASE 'my_db'
);
```

## Network security connections

### AWS PrivateLink

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

```mzsql
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

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

### SSH tunnel

An SSH tunnel connection establishes a link to an SSH bastion server. You can
use SSH tunnel connections in [Kafka connections](#kafka), [MySQL connections](#mysql),
and [Postgres connections](#postgresql).

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

```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize'
);
```

Retrieve the public keys for the SSH tunnel connection you just created:

```mzsql
SELECT
    mz_connections.name,
    mz_ssh_tunnel_connections.*
FROM
    mz_connections
JOIN
    mz_ssh_tunnel_connections USING(id)
WHERE
    mz_connections.name = 'ssh_connection';
```
```
 id    | public_key_1                          | public_key_2
-------+---------------------------------------+---------------------------------------
 ...   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize
```

## Connection validation {#connection-validation}

Materialize automatically validates the connection and authentication parameters
for most connection types on connection creation:

Connection type             | Validated by default |
----------------------------|----------------------|
AWS                         |                      |
Kafka                       | ✓                    |
Confluent Schema Registry   | ✓                    |
MySQL                       | ✓                    |
PostgreSQL                  | ✓                    |
SSH Tunnel                  |                      |
AWS PrivateLink             |                      |

For connection types that are validated by default, if the validation step
fails, the creation of the connection will also fail and a validation error is
returned. You can disable connection validation by setting the `VALIDATE`
option to `false`. This is useful, for example, when the parameters are known
to be correct but the external system is unavailable at the time of creation.

Connection types that require additional setup steps after creation, like AWS
and SSH tunnel connections, can be **manually validated** using the [`VALIDATE
CONNECTION`](/sql/validate-connection) syntax once all setup steps are
completed.

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all connections and secrets used in the connection definition.
- `USAGE` privileges on the schemas that all connections and secrets in the statement are contained in.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE SINK`](/sql/create-sink)

[AWS PrivateLink]: https://aws.amazon.com/privatelink/
[Confluent Schema Registry]: https://docs.confluent.io/platform/current/schema-registry/index.html#sr-overview
[Kafka]: https://kafka.apache.org
[MySQL]: https://www.mysql.com/
[PostgreSQL]: https://www.postgresql.org
[SQL Server]: https://www.microsoft.com/en-us/sql-server
[`ALTER CONNECTION`]: /sql/alter-connection
[`CREATE SOURCE`]: /sql/create-source
[`CREATE SINK`]: /sql/create-sink
[`FORMAT`]: /sql/create-source/#formats
[`mz_aws_privatelink_connections`]: /sql/system-catalog/mz_catalog/#mz_aws_privatelink_connections
[`mz_connections`]: /sql/system-catalog/mz_catalog/#mz_connections
[`mz_ssh_tunnel_connections`]: /sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections
[Ed25519 algorithm]: https://ed25519.cr.yp.to
[latacora-crypto]: https://latacora.micro.blog/2018/04/03/cryptographic-right-answers.html
[trust policy]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_terms-and-concepts.html#term_trust-policy
[external ID]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
