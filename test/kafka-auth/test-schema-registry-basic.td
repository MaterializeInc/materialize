# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# ==> Set up. <==

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
$ kafka-ingest topic=avro-data format=avro schema=${schema}
{"a": 1}

# ==> Test invalid configurations. <==

! CREATE CONNECTION schema_registry_invalid TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://basic.schema-registry.local:8081',
    USERNAME 'materialize',
    PASSWORD SECRET password_wrong
  )
contains:server error 401: Unauthorized

# ==> Test without an SSH tunnel. <==

> CREATE CONNECTION schema_registry TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://basic.schema-registry.local:8081',
    USERNAME 'materialize',
    PASSWORD SECRET password
  )

> BEGIN
> CREATE SOURCE avro_data FROM KAFKA CONNECTION kafka (
    TOPIC 'testdrive-avro-data-${testdrive.seed}'
  )

> CREATE TABLE avro_data_tbl FROM SOURCE avro_data (REFERENCE "testdrive-avro-data-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
> COMMIT

> SELECT * FROM avro_data_tbl
a
----
1

# ==> Test with an SSH tunnel. <==

> CREATE CONNECTION schema_registry_ssh TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://basic.schema-registry.local:8081',
    USERNAME 'materialize',
    PASSWORD SECRET password,
    SSH TUNNEL testdrive_no_reset_connections.public.ssh
  )

> BEGIN
> CREATE SOURCE avro_data_ssh FROM KAFKA CONNECTION kafka (
    TOPIC 'testdrive-avro-data-${testdrive.seed}'
  )

> CREATE TABLE avro_data_ssh_tbl FROM SOURCE avro_data_ssh (REFERENCE "testdrive-avro-data-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
> COMMIT

> SELECT * FROM avro_data_ssh_tbl
a
----
1
