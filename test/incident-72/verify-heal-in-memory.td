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
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

# In this version of Materialize we can tell if we're dealing with a legacy
# error or not by the ", legacy: true" suffix on the error string. Since we
# have turned off the storage cluster the source should not have healed yet and
# the legacy error should be still in there.

! SELECT * FROM textavro AS OF AT LEAST 0
exact: Envelope error: Upsert: Value error: Text: avro deserialization error: wrong Confluent-style avro serialization magic: expected 0, got 32 (original bytes: [20, 67, 61, 72, 62, 61, 67, 65]), decoded key: Row{[String("fish")]}, legacy: true

# Turn on the cluster so that the healing logic can run
> ALTER CLUSTER source_cluster SET (REPLICATION FACTOR 1)

# Now let's wait until the since frontier of the source goes beyond the healing
# point. This is when the `AS OF AT LEAST 0` query stops returning the legacy
# error.

! SELECT * FROM textavro AS OF AT LEAST 0
exact: Envelope error: Upsert: Value error: Text: avro deserialization error: wrong Confluent-style avro serialization magic: expected 0, got 32 (original bytes: [20, 67, 61, 72, 62, 61, 67, 65]), decoded key: Row{[String("fish")]}

# Give some more time for the since frontier to be committed to persist as this is a rate limited action.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

# We should be good to compact now

$ set-from-sql var=source-id
SELECT id FROM mz_objects WHERE name = 'textavro';

$ set-from-sql var=source-shard-id
SELECT shard_id FROM mz_dangerous.mz_storage_shards WHERE object_id = '${source-id}';

$ persist-force-compaction shard-id=${source-shard-id}
