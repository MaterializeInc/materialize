# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify commands do not work on kafka.

$ kafka-create-topic topic=data partitions=1
$ kafka-ingest format=bytes topic=data
one

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE data
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  FORMAT TEXT;

> SELECT * from data
one

!ALTER SOURCE data ADD SUBSOURCE t;
contains:data is a kafka source, which does not support ALTER TABLE...ADD SUBSOURCES

!ALTER SOURCE data DROP SUBSOURCE t;
contains:unknown catalog item 't'
