# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

$ set-arg-default single-replica-cluster=quickstart

$ kafka-create-topic topic=t0 partitions=1

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

! CREATE SOURCE too_many_offsets
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (
      TOPIC 'testdrive-t0-${testdrive.seed}',
      START OFFSET (100, 100)
    )
exact:START OFFSET specified more partitions (2) than topic (testdrive-t0-${testdrive.seed}) contains (1)
