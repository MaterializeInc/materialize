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

$ kafka-create-topic topic=auto_topic partitions=1

$ kafka-ingest format=bytes topic=auto_topic
one
two
three

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

# Note that auto commit is not really safe to enable, but we have this here to
# test `kafka-verify-commit` until we have correct commit behavior for kafka sources.
> CREATE SOURCE auto_topic
  FROM KAFKA CONNECTION conn (
    TOPIC 'testdrive-auto_topic-${testdrive.seed}',
    ENABLE AUTO COMMIT
  )
  FORMAT BYTES
  INCLUDE OFFSET
  ENVELOPE NONE

> SELECT * from auto_topic
data         offset
-----------------------------------
one          0
two          1
three        2

$ kafka-verify-commit source=auto_topic topic=auto_topic partition=0
3
