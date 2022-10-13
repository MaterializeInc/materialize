# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This testdrive file uses the deprecated syntax, but is otherwise identical to upsert-kafka-new.td
#
# This file can be deleted when/if we finish the deprecation and perform the removal of the old syntax.


$ kafka-create-topic topic=topic partitions=1

$ kafka-ingest format=bytes topic=topic
one
two
three
four

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> CREATE SOURCE avroavro
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-topic-${testdrive.seed}'
  )
  FORMAT BYTES
  INCLUDE OFFSET
  ENVELOPE NONE

> SELECT * from avroavro
data    offset
-----------------------------------
one     0
two     1
three   2
four    2
