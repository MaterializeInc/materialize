# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=continual-feedback-repro partitions=1

> CREATE CONNECTION continual_feedback_repro_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT

> CREATE CLUSTER storage_cluster REPLICAS (
    r1 (
      STORAGECTL ADDRESSES ['clusterd1:2100'],
      STORAGE ADDRESSES ['clusterd1:2103'],
      COMPUTECTL ADDRESSES ['clusterd1:2101'],
      COMPUTE ADDRESSES ['clusterd1:2102'],
      WORKERS 1
    ),
    r2 (
      STORAGECTL ADDRESSES ['clusterd2:2100'],
      STORAGE ADDRESSES ['clusterd2:2103'],
      COMPUTECTL ADDRESSES ['clusterd2:2101'],
      COMPUTE ADDRESSES ['clusterd2:2102'],
      WORKERS 1
    )
  )

> BEGIN
> CREATE SOURCE continual_feedback_repro_src
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION continual_feedback_repro_conn (TOPIC
  'testdrive-continual-feedback-repro-${testdrive.seed}'
  )
  WITH (TIMESTAMP INTERVAL '200ms')

> CREATE TABLE continual_feedback_repro_tbl FROM SOURCE continual_feedback_repro_src (REFERENCE "testdrive-continual-feedback-repro-${testdrive.seed}")
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT
> COMMIT
