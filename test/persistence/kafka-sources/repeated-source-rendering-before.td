# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"long"}
        ]
    }

$ kafka-create-topic topic=re-created partitions=1

# Make sure that we can render a source (by creating an indexed view), drop
# it, and render it again.

$ kafka-ingest format=avro topic=re-created schema=${schema} repeat=10
{"f1": ${kafka-ingest.iteration}}

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE re_created
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-re-created-${testdrive.seed}')

> CREATE TABLE re_created_tbl FROM SOURCE re_created (REFERENCE "testdrive-re-created-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE PARTITION AS kafka_partition, OFFSET AS mz_offset
  ENVELOPE NONE
> COMMIT

> CREATE VIEW a_view AS SELECT * FROM re_created_tbl;
> CREATE DEFAULT INDEX ON a_view;

> SELECT COUNT(*) FROM a_view;
10

> DROP VIEW a_view;

> CREATE VIEW a_view AS SELECT * FROM re_created_tbl;
> CREATE DEFAULT INDEX ON a_view;

> SELECT COUNT(*) FROM a_view;
10

# Same with DROP INDEX

> DROP INDEX a_view_primary_idx;

> CREATE DEFAULT INDEX ON a_view;

> SELECT COUNT(*) FROM a_view;
10
