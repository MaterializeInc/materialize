# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A source through the tunnel that bootstraps from `kafka`, the name the
# workflow later moves to a replacement broker.

$ kafka-create-topic topic=switchover

$ kafka-ingest topic=switchover format=bytes
one
two

> DROP CLUSTER IF EXISTS sc;
> CREATE CLUSTER sc REPLICAS (r1 (SIZE 'scale=1,workers=4'))

> CREATE CONNECTION kafka_conn_switchover
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT, SSH TUNNEL thancred);

> BEGIN
> CREATE SOURCE switchover IN CLUSTER sc
  FROM KAFKA CONNECTION kafka_conn_switchover (TOPIC 'testdrive-switchover-${testdrive.seed}')
> CREATE TABLE switchover_tbl FROM SOURCE switchover (REFERENCE "testdrive-switchover-${testdrive.seed}")
  FORMAT TEXT
  ENVELOPE NONE
> COMMIT

> SELECT * FROM switchover_tbl
text
----
one
two
