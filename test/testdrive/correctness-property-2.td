# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

$ set count=100000

$ kafka-create-topic topic=correctness-data

$ kafka-ingest format=avro topic=correctness-data key-format=avro key-schema=${keyschema} schema=${schema} repeat=${count} start-iteration=1
{"key": "1"} {"f1": "crustycrab", "f2": ${kafka-ingest.iteration} }

# Create a cluster with no replicas so that we have time to submit queries at the minimum frontier.
> CREATE CLUSTER storage REPLICAS ()

# Create a cluster with no replicas so that we have time to submit queries at the minimum frontier.

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SOURCE correctness_data
  IN CLUSTER storage
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-correctness-data-${testdrive.seed}')
  WITH (RETAIN HISTORY = FOR '365000 days');

> CREATE TABLE correctness_data_tbl FROM SOURCE correctness_data (REFERENCE "testdrive-correctness-data-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

# Prime tokio-postgres with the missing OIDs.
> SELECT mz_now(), * FROM correctness_data_tbl WHERE false AS OF AT LEAST 0

# Grab a cursor at timestamp 0
> BEGIN

> DECLARE c CURSOR FOR SELECT mz_now(), * FROM correctness_data_tbl AS OF 0

# Start ingestion by adding a replica to the cluster. We must do this from a
# different connection to not disturbe the transaction we're in.
$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
CREATE CLUSTER REPLICA storage.r1 SIZE = 'scale=1,workers=1';

# Verify that at timestamp 0 there is only one record whose value is the final value
> FETCH 1 c WITH (timeout = '1d');
0 1 crustycrab ${count}

> COMMIT
