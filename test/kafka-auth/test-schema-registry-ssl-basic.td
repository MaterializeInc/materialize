# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_default_connection_validation = true
ALTER SYSTEM SET enable_connection_validation_syntax = true

# ==> Set up. <==

$ set-from-file ca-crt=/share/secrets/ca.crt

> CREATE SECRET password AS 'sekurity'
> CREATE SECRET password_wrong AS 'wrong'

> CREATE CONNECTION kafka to KAFKA (
    BROKER 'kafka:9092',
    SECURITY PROTOCOL PLAINTEXT
  )

$ set schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"}
    ]
  }
$ kafka-create-topic topic=avro-data
$ kafka-ingest topic=avro-data format=avro schema=${schema} timestamp=1
{"a": 1}

# ==> Test invalid configurations. <==

! CREATE CONNECTION schema_registry_invalid TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://ssl-basic.schema-registry.local:8082'
  )
contains:certificate verify failed

! CREATE CONNECTION schema_registry_invalid TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://ssl-basic.schema-registry.local:8082',
    SSL CERTIFICATE AUTHORITY = '${ca-crt}'
  )
contains:server error 401: Unauthorized

! CREATE CONNECTION schema_registry_invalid TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://ssl-basic.schema-registry.local:8082',
    USERNAME 'materialize',
    PASSWORD SECRET password_wrong,
    SSL CERTIFICATE AUTHORITY = '${ca-crt}'
  )
contains:server error 401: Unauthorized

# ==> Test without an SSH tunnel. <==

> CREATE CONNECTION schema_registry TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://ssl-basic.schema-registry.local:8082',
    USERNAME 'materialize',
    PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY = '${ca-crt}'
  )

> CREATE SOURCE avro_data FROM KAFKA CONNECTION kafka (
    TOPIC 'testdrive-avro-data-${testdrive.seed}'
  )

> CREATE TABLE avro_data_tbl FROM SOURCE avro_data (REFERENCE "testdrive-avro-data-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry

> SELECT * FROM avro_data_tbl
a
----
1

# ==> Test with an SSH tunnel. <==

> CREATE CONNECTION schema_registry_ssh TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://ssl-basic.schema-registry.local:8082',
    USERNAME 'materialize',
    PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY = '${ca-crt}',
    SSH TUNNEL testdrive_no_reset_connections.public.ssh
  )

> CREATE SOURCE avro_data_ssh FROM KAFKA CONNECTION kafka (
    TOPIC 'testdrive-avro-data-${testdrive.seed}'
  )

> CREATE TABLE avro_data_ssh_tbl FROM SOURCE avro_data_ssh (REFERENCE "testdrive-avro-data-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry

> SELECT * FROM avro_data_ssh_tbl
a
----
1

# ALTER CONNECTION

## Sink Kafka connection

### Create a new connection in use only by the sink so that we may break it.

> CREATE CONNECTION kafka_backup TO KAFKA (
    BROKER 'kafka:9092',
    SECURITY PROTOCOL PLAINTEXT
  );

#### Create a backup sink

> CREATE SINK snk_backup FROM avro_data_tbl
  INTO KAFKA CONNECTION kafka_backup (TOPIC 'snk_backup')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=avro sink=materialize.public.snk_backup sort-messages=true
{"before": null, "after": {"row":{"a": 1}}}

### Break sink broker connection and produce data

> ALTER CONNECTION kafka_backup SET (BROKER 'kafka:9093') WITH (VALIDATE = false);

$ kafka-ingest topic=avro-data format=avro schema=${schema} timestamp=2
{"a": 2}

> SELECT * FROM avro_data_tbl
1
2

> SELECT count(status) > 0 FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON sink_id = id WHERE name = 'snk_backup' AND status = 'stalled';
true

> ALTER CONNECTION kafka_backup SET (BROKER 'kafka:9092');

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'snk_backup';
running

$ kafka-verify-data format=avro sink=materialize.public.snk_backup sort-messages=true
{"before": null, "after": {"row":{"a": 2}}}

## CSR connection

! ALTER CONNECTION schema_registry SET (URL = 'abc') WITH (VALIDATE = true);
contains:invalid ALTER CONNECTION: parsing schema registry url: relative URL without a base

! ALTER CONNECTION schema_registry RESET (URL);
contains:invalid ALTER CONNECTION: invalid CONNECTION: must specify URL

! ALTER CONNECTION schema_registry SET (SSL KEY = 'x') WITH (VALIDATE = true);
contains:invalid SSL KEY: must provide a secret value

> CREATE SECRET IF NOT EXISTS invalid_secret AS 'x'

! ALTER CONNECTION schema_registry SET (SSL KEY = SECRET invalid_secret) WITH (VALIDATE = true);
contains:requires both SSL KEY and SSL CERTIFICATE

! ALTER CONNECTION schema_registry SET (SSL CERTIFICATE = 'x') WITH (VALIDATE = true);
contains:requires both SSL KEY and SSL CERTIFICATE

! ALTER CONNECTION schema_registry SET (SSL KEY = SECRET invalid_secret), SET (SSL CERTIFICATE = 'x') WITH (VALIDATE = true);
contains:No supported data to decode

! ALTER CONNECTION schema_registry SET (SSL CERTIFICATE AUTHORITY = 'x') WITH (VALIDATE = true);
contains:CERTIFICATE

> ALTER CONNECTION schema_registry RESET (SSL KEY);

> ALTER CONNECTION schema_registry RESET (SSL CERTIFICATE);

! ALTER CONNECTION schema_registry RESET (SSL CERTIFICATE AUTHORITY) WITH (VALIDATE = true);
contains:self-signed certificate in certificate chain

! ALTER CONNECTION schema_registry RESET (SSL KEY), RESET (SSL CERTIFICATE), RESET (SSL CERTIFICATE AUTHORITY);
contains:self-signed certificate in certificate chain

! ALTER CONNECTION schema_registry RESET (USERNAME);
contains:Unauthorized

! ALTER CONNECTION schema_registry RESET (PASSWORD);
contains:Unauthorized

! ALTER CONNECTION schema_registry RESET (USERNAME), RESET (PASSWORD);
contains:Unauthorized

# Break CSR connection
# These won't necessarily stall running sources which might not reach out to the CSR.

> ALTER CONNECTION schema_registry DROP (SSL CERTIFICATE AUTHORITY) WITH (VALIDATE = false);

$ kafka-create-topic topic=data partitions=1

> CREATE SOURCE kafka_broken_csr_connector_source_broken
  FROM KAFKA CONNECTION kafka (TOPIC 'testdrive-data-${testdrive.seed}')
! CREATE TABLE kafka_broken_csr_connector_source_broken_tbl FROM SOURCE kafka_broken_csr_connector_source_broken (REFERENCE "testdrive-data-${testdrive.seed}")
    FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
contains:failed to fetch schema subject

> ALTER CONNECTION schema_registry SET (SSL CERTIFICATE AUTHORITY = '${ca-crt}') WITH (VALIDATE = true);

> CREATE SOURCE kafka_broken_csr_connector_source_fixed FROM KAFKA CONNECTION kafka (
    TOPIC 'testdrive-avro-data-${testdrive.seed}'
  )

> CREATE TABLE kafka_broken_csr_connector_source_fixed_tbl FROM SOURCE kafka_broken_csr_connector_source_fixed (REFERENCE "testdrive-avro-data-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry

> SELECT * FROM kafka_broken_csr_connector_source_fixed_tbl
1
2
