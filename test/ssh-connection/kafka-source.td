# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_source_denylist_with_options = true
ALTER SYSTEM SET enable_kafka_config_denylist_options = true

# Test creating a Kafka source using SSH, with and without the CSR

$ kafka-create-topic topic=thetopic

$ kafka-ingest topic=thetopic format=bytes
one

# Create a dedicated cluster for the source, so we can easily restart the
# source by dropping and recreating the cluster's replica.
#
# We also use a large number of worker threads to protect against past behavior
# in which we opened one SSH tunnel per worker thread per broker, which
# tripped the default `MaxStartups 10` sshd configuration.
> DROP CLUSTER IF EXISTS sc;
> CREATE CLUSTER sc REPLICAS (r1 (SIZE '32'))

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}' USING SSH TUNNEL thancred);

# Test that we can create, validate, and use a dynamic kafka connection
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_default_kafka_ssh_tunnel = true

> CREATE CONNECTION kafka_conn_top_level
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SSH TUNNEL thancred);

> CREATE SOURCE mz_source_top_level IN CLUSTER sc
  FROM KAFKA CONNECTION kafka_conn_top_level (
    TOPIC 'testdrive-thetopic-${testdrive.seed}',
    TOPIC METADATA REFRESH INTERVAL MS 1000
  )
  FORMAT TEXT
  ENVELOPE NONE

> SELECT * FROM mz_source_top_level
text
----
one

# Create source with a faster metadata refresh interval so
# errors are detected every second instead of every minute.
> CREATE SOURCE mz_source IN CLUSTER sc
  FROM KAFKA CONNECTION kafka_conn (
    TOPIC 'testdrive-thetopic-${testdrive.seed}',
    TOPIC METADATA REFRESH INTERVAL MS 1000
  )
  FORMAT TEXT
  ENVELOPE NONE

> SHOW SUBSOURCES ON mz_source
mz_source_progress progress

> SELECT * FROM mz_source
text
----
one

$ kafka-ingest topic=thetopic format=bytes
two

> SELECT * FROM mz_source
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
  IN CLUSTER sc
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-avroavro-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> SELECT * FROM csr_source
f1    f2
----------
fish  1000
