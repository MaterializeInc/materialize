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

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE SOURCE remote1
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-remote1-${testdrive.seed}')
  FORMAT TEXT
  WITH (
    REMOTE 'storaged:2100'
  )
> CREATE SOURCE remote2
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-remote2-${testdrive.seed}')
  FORMAT TEXT
  WITH (
    REMOTE 'storaged:2100'
  )

> SELECT * from remote1
one
> SELECT * from remote2
one
