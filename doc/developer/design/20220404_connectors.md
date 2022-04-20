# Connectors

## Summary

This document proposes a design for **connectors**, a new type of catalog object
that allows common configuration parameters to be shared across sources and
sinks.

## Overview

Many users of Materialize create families of sources and sinks that share many
configuration parameters. The current design of `CREATE SOURCE` and `CREATE
SINK` make this a verbose affair. The problem is particularly acute with
Avro-formatted Kafka sources that configure authentication:

```sql
CREATE SOURCE kafka1
FROM KAFKA BROKER 'kafka:9092' TOPIC 'top1' WITH (
    security_protocol = 'SASL_SSL',
    sasl_mechanisms = 'PLAIN',
    sasl_username = 'username',
    sasl_password = 'password',
)
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://schema-registry' WITH (
    username = 'username',
    password = 'password'
);

CREATE SOURCE kafka2
FROM KAFKA BROKER 'kafka:9092' TOPIC 'top2' WITH (
    security_protocol = 'SASL_SSL',
    sasl_mechanisms = 'PLAIN',
    sasl_username = 'username',
    sasl_password = 'password'
)
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://schema-registry' WITH (
    username = 'username',
    password = 'password'
);
```

These two source definition differ only in their topic specification, but
must duplicate eight other configuration parameters.

With the connectors proposed in this document, the `CREATE SOURCE` can instead
share all the relevant configuration:

```sql
CREATE CONNECTOR kafka FOR
KAFKA BROKER 'kafka:9092' WITH (
    security_protocol = 'SASL_SSL',
    sasl_mechanisms = 'PLAIN',
    sasl_username = 'username',
    sasl_password = 'password'
);

CREATE CONNECTOR schema_registry FOR
CONFLUENT SCHEMA REGISTRY 'https://schema-registry' WITH (
    username = 'username',
    password = 'password'
);

CREATE SOURCE kafka1
FROM KAFKA CONNECTOR kafka TOPIC 'top1'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTOR schema_registry;

CREATE SOURCE kafka2
FROM KAFKA CONNECTOR kafka TOPIC 'top2'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTOR schema_registry;
```

## Design

### SQL syntax and semantics

The eventual goal is for connector create syntax to come in type specific variants and not include `WITH` options except in cases of client implementation specific details; however, the initial implementation will preserve existing syntax for connectors to avoid entangling separating connectors into their own objects from reworking the overall connection option syntax.

All Sources in the catalog will have a reference to their connector which will also indicate if the connector is implicit or explicit. Implicit connectors are created when a traditional `CREATE SOURCE` command is executed and are invisible unless the catalog is directly inspected, i.e. `SHOW CREATE SOURCE` for a source with an implicit connector will return the same output as if connectors did not exist. Explicit connectors are created with `CREATE CONNECTOR` commands and used by specifying the connector in the `CREATE SOURCE` statement.

## Implementation Phases

1. Connectors marked `experimental` with support for implicit and explicit connectors using otherwise identical syntax to now (`WITH` options, etc) in `CREATE SOURCE` statements when enabled and when reading from the catalog in all cases
2. During `experimental` phase evaluate restructuring the syntax to remove as many `WITH` options as is reasonable, limiting them to client implementation details such as `librdkafka` options which are not generic Kafka options.
3. Finalize syntax and determine necessary migrations, if any, to allow for removing `experimental` status.
4. Enable `ALTER CONNECTOR` with transactional consistency in the catalog but eventual consistency for existing Sources as `experimental` feature
  - A `CREATE` which follows an `ALTER` will be created at the STORAGE layer with the values set in the `ALTER`
  - An `ALTER` which follows a `CREATE` will take non-zero time to propagate to the STORAGE layer
  - TBD exact semantics of assuring convergence
5. Make `ALTER CONNECTOR` stable once semantics and implementation are stable

## Reference
The longer term syntax for `CREATE CONNECTOR` is expected to look similar to these examples.
### Kafka connector
```
CREATE CONNECTOR <connector_name>
FOR KAFKA
  BROKER <value>
  SECURITY [=] <security_options>

security_options ::=
  SSL (
    KEY FILE [[=] <value>]
    CERTIFICATE FILE [[=] <value>]
    KEY PASSWORD [[=] <value>] )
| SASL (
    MECHANISMS <value>
    USERNAME [[=] <value> ]
    PASSWORD [[=] <value> ]
    PASSWORD ENV [[=] <value> ] )
| GSSAPI (
    KEYTAB [=] <value>
    KINIT CMD [=] <value>
    RELOGIN TIME [=] <value>
    PRINCIPAL [=] <value>
    SERVICE [=] <value> )
```

### Confluent Schema Registry connector
```
CREATE CONNETOR <connector_name>
FOR CONFLUENT SCHEMA REGISTRY
  <registry_url>
  SECURITY [[=] <security_options>]

security_options ::=
  USERNAME [=] <value>
  PASSWORD [=] <value>
  SSL (
    CERTIFICATE FILE [[=] <value> ]
    KEY FILE [[=] <value> ]
    CA FILE [[=] <value> ] )
```

### PostgreSQL connector
```
CREATE CONNECTOR <connector_name>
FOR POSTGRES
  CONNECTION (
    ( HOST [=] <value>
      PORT [=] <value> )
  | HOST ADDR [[=] <value> ] )
  DB  [[=] <value> ]
  USER  [=] <value>
  SSL (
    MODE [[=] <value>]
    CERTIFICATE FILE [[=] <value>]
    KEY FILE [[=] <value>]
    SNI [[=] <value>] )

```

### AWS connector
```
CREATE CONNECTOR <connector_name>
FOR AWS
  ACCESS KEY ID [[=] <value>]
  SECRET ACCESS KEY [[=] <value>]
  TOKEN [[=] <value>]
  PROFILE [[=] <value>]
  ROLE ARN [[=] <value>]
  REGION [[=] <value>]
```
