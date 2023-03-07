# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Create sources and verify they can ingest data while `environmentd` is online.

$ kafka-create-topic topic=remote1
$ kafka-create-topic topic=remote2

$ kafka-ingest format=bytes topic=remote1
one
$ kafka-ingest format=bytes topic=remote2
one

> CREATE CLUSTER storage_cluster REPLICAS (
    r1 (
      STORAGECTL ADDRESSES ['clusterd1:2100', 'clusterd2:2100'],
      STORAGE ADDRESSES ['clusterd1:2103', 'clusterd2:2103'],
      COMPUTECTL ADDRESSES ['clusterd1:2101', 'clusterd2:2101'],
      COMPUTE ADDRESSES ['clusterd1:2102', 'clusterd2:2102'],
      WORKERS 4
    )
  )

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE remote1
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-remote1-${testdrive.seed}')
  FORMAT TEXT
> CREATE SOURCE remote2
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-remote2-${testdrive.seed}')
  FORMAT TEXT

> SELECT * from remote1
one
> SELECT * from remote2
one
