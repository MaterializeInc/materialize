# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# librdkafka rejects a receive.message.max.bytes below fetch.max.bytes + 512 or
# above 2147483647, which would leave the reader consumer uncreated. The source
# clamps the value to that range and ingests.

$ kafka-create-topic topic=receive-max partitions=2
$ kafka-ingest format=bytes topic=receive-max
one
two
three

> CREATE CONNECTION conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET kafka_source_receive_message_max_bytes = 1000

> CREATE SOURCE receive_max_low
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION conn (TOPIC 'testdrive-receive-max-${testdrive.seed}')

> CREATE TABLE receive_max_low_tbl FROM SOURCE receive_max_low (REFERENCE "testdrive-receive-max-${testdrive.seed}")
  FORMAT BYTES

> SELECT * FROM receive_max_low_tbl
one
two
three

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET kafka_source_receive_message_max_bytes = 3000000000

> CREATE SOURCE receive_max_high
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION conn (TOPIC 'testdrive-receive-max-${testdrive.seed}')

> CREATE TABLE receive_max_high_tbl FROM SOURCE receive_max_high (REFERENCE "testdrive-receive-max-${testdrive.seed}")
  FORMAT BYTES

> SELECT * FROM receive_max_high_tbl
one
two
three

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET kafka_source_receive_message_max_bytes
