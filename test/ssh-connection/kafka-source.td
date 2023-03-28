# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test creating a Kafka source using SSH, with and without the CSR

$ kafka-create-topic topic=thetopic

$ kafka-ingest topic=thetopic format=bytes
one

# Test the various types of tunnels

> CREATE CONNECTION kafka_conn_using
  TO KAFKA (BROKER '${testdrive.kafka-addr}' USING SSH TUNNEL thancred);

> CREATE CONNECTION kafka_conn_dynamic
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SSH TUNNEL thancred);

> CREATE CONNECTION kafka_conn_keyless
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SSH TUNNEL keyless);

> CREATE SOURCE mz_source
  FROM KAFKA CONNECTION kafka_conn_using (TOPIC 'testdrive-thetopic-${testdrive.seed}')
  FORMAT TEXT
  ENVELOPE NONE

> CREATE SOURCE mz_source_dynamic
  FROM KAFKA CONNECTION kafka_conn_dynamic (TOPIC 'testdrive-thetopic-${testdrive.seed}')
  FORMAT TEXT
  ENVELOPE NONE

# Ensure error reporting works.
! CREATE SOURCE mz_source_broken
  FROM KAFKA CONNECTION kafka_conn_keyless (TOPIC 'testdrive-thetopic-${testdrive.seed}')
  FORMAT TEXT
  ENVELOPE NONE
contains:failed to connect to the remote host

> SELECT * FROM mz_source
text
----
one

$ kafka-ingest topic=thetopic format=bytes
two

# Ensure both types of tunnels work
> SELECT * FROM mz_source
text
----
one
two

> SELECT * FROM mz_source_dynamic
text
----
one
two

> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}',
    SSH TUNNEL thancred
  );

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-create-topic topic=avroavro

$ kafka-ingest format=avro topic=avroavro schema=${schema}
{"f1": "fish", "f2": 1000}

> CREATE SOURCE csr_source
  FROM KAFKA CONNECTION kafka_conn_using (TOPIC 'testdrive-avroavro-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> SELECT * FROM csr_source
f1    f2
----------
fish  1000

# Test csr sources for dynamic connections as well.
> CREATE SOURCE csr_source_dynamic
  FROM KAFKA CONNECTION kafka_conn_dynamic (TOPIC 'testdrive-avroavro-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> SELECT * FROM csr_source_dynamic
f1    f2
----------
fish  1000
