# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test consolidation and compaction behavior.
#
# The various tests in this file use the following Debezium-formatted Kafka
# topics. The first topic, `nums`, is a basic data topic that contains one
# bigint field. The second topic, `tx`, mimics a Debezium transactional
# metadata topic that groups updates from `nums` into transactions.
#
# Using a transactional metadata topic like this allows us to tightly control
# the timestamp at which data is ingested into Materialize. Data from the first
# transaction is assigned timestamp 1, data from the second is assigned
# timestamp 2, and so on.

$ set nums-schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [{"name": "num", "type": "long"}]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ set tx-schema={
    "type": "record",
    "name": "TransactionMetadataValue",
    "namespace": "io.debezium.connector.common",
    "fields": [
      {"name": "status", "type": "string"},
      {"name": "id", "type": "string"},
      {
        "name": "event_count",
        "type": ["null", "long"],
        "default": null
      },
      {
        "name": "data_collections",
        "type": [
          "null",
          {
            "type": "array",
            "items": {
              "type": "record",
              "name": "ConnectDefault",
              "namespace": "io.confluent.connect.Avro",
              "fields": [
                {"name": "data_collection", "type": "string"},
                {"name": "event_count", "type": "long"}
              ]
            }
          }
        ],
        "default": null
      }
    ],
    "connect.name": "io.debezium.connector.common.TransactionMetadataValue"
  }

$ kafka-create-topic topic=nums
$ kafka-create-topic topic=tx

> CREATE MATERIALIZED SOURCE nums
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-nums-${testdrive.seed}'
  WITH (consistency_topic = 'testdrive-tx-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${nums-schema}'
  ENVELOPE DEBEZIUM

# Disable logical compaction, to ensure we can view historical detail.
> ALTER INDEX materialize.public.nums_primary_idx
  SET (logical_compaction_window = 'off')

# Create a sink before we ingest any data, to ensure the sink starts AS OF 0
> CREATE SINK nums_sink FROM nums
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'nums-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# ==> Test consolidation.

# Ingest several updates that consolidate. Some of these updates are in one
# transaction, and some of them are in their own transactions.

$ kafka-ingest format=avro topic=nums schema=${nums-schema}
{"before": null, "after": {"row": {"num": 1}}}
{"before": {"row": {"num": 1}}, "after": {"row": {"num": 2}}}
{"before": {"row": {"num": 2}}, "after": {"row": {"num": 3}}}
{"before": {"row": {"num": 3}}, "after": {"row": {"num": 4}}}
{"before": {"row": {"num": 4}}, "after": {"row": {"num": 5}}}

$ kafka-ingest format=avro topic=tx schema=${tx-schema}
{"status": "BEGIN", "id": "1", "event_count": null, "data_collections": null}
{"status": "END", "id": "1", "event_count": {"long": 3}, "data_collections": {"array": [{"event_count": 3, "data_collection": "testdrive-nums-${testdrive.seed}"}]}}
{"status": "BEGIN", "id": "2", "event_count": null, "data_collections": null}
{"status": "END", "id": "2", "event_count": {"long": 1}, "data_collections": {"array": [{"event_count": 1, "data_collection": "testdrive-nums-${testdrive.seed}"}]}}
{"status": "BEGIN", "id": "3", "event_count": null, "data_collections": null}
{"status": "END", "id": "3", "event_count": {"long": 1}, "data_collections": {"array": [{"event_count": 1, "data_collection": "testdrive-nums-${testdrive.seed}"}]}}

# Test that by default updates that occurred at the same time are consolidated,
# but updates that occurred at distinct times are not.

# we know that transactions (timestamps) are emitted in order, but the order
# of emitted records with the same timestamp is not deterministic. We therefore
# verify each transaction separately and sort within each transaction to get
# deterministic results.

$ kafka-verify format=avro sink=materialize.public.nums_sink
{"before": null, "after": {"row": {"num": 3}}}

$ kafka-verify format=avro sink=materialize.public.nums_sink sort-messages=true
{"before": null, "after": {"row": {"num": 4}}}
{"before": {"row": {"num": 3}}, "after": null}

$ kafka-verify format=avro sink=materialize.public.nums_sink sort-messages=true
{"before": null, "after": {"row": {"num": 5}}}
{"before": {"row": {"num": 4}}, "after": null}

# TODO(benesch): re-enable when we support `CREATE SINK ... AS OF`.
# # Test that a Debezium sink created `AS OF 3` (the latest completed timestamp)
# # is fully consolidated.
# > CREATE SINK nums_sink FROM nums
#   INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'nums-sink'
#   FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
#   AS OF 3
#
# $ kafka-verify format=avro sink=materialize.public.nums_sink
# {"before": null, "after": {"row": {"num": 5}}}

# Validate that `TAIL` is similarly consolidated.
# This protects against regression of #5421.

> BEGIN
> DECLARE cur CURSOR FOR TAIL nums AS OF 3
> FETCH ALL cur
mz_timestamp  mz_diff  num
--------------------------
3             1        5
> COMMIT

# ==> Test compaction.

# Each transaction that has been updated so far should be separately visible
# (i.e., not compacted away).

> SELECT * FROM nums AS OF 1
3
> SELECT * FROM nums AS OF 2
4
> SELECT * FROM nums AS OF 3
5

# Decrease the compaction window to 1ms and ingest some new data in transaction
# 4.

> ALTER INDEX materialize.public.nums_primary_idx
  SET (logical_compaction_window = '1ms')

$ kafka-ingest format=avro topic=nums schema=${nums-schema}
{"before": {"row": {"num": 5}}, "after": {"row": {"num": 6}}}

$ kafka-ingest format=avro topic=tx schema=${tx-schema}
{"status": "BEGIN", "id": "4", "event_count": null, "data_collections": null}
{"status": "END", "id": "4", "event_count": {"long": 1}, "data_collections": {"array": [{"event_count": 1, "data_collection": "testdrive-nums-${testdrive.seed}"}]}}

# Data from older transactions should be immediately compacted to the timestamp
# of the latest transaction (i.e., 4).

! SELECT * FROM nums AS OF 2
contains:Timestamp (2) is not valid for all inputs
! SELECT * FROM nums AS OF 3
contains:Timestamp (3) is not valid for all inputs
> SELECT * FROM nums AS OF 4
6

# Reset the compaction window back to default (currently 60s) and advance the
# number in transactions 5 and 6.

> ALTER INDEX materialize.public.nums_primary_idx
  RESET (logical_compaction_window)

# But also create an index that compacts frequently.
> CREATE VIEW nums_compacted AS SELECT * FROM nums
> CREATE DEFAULT INDEX ON nums_compacted WITH (logical_compaction_window = '1ms')

$ kafka-ingest format=avro topic=nums schema=${nums-schema}
{"before": {"row": {"num": 6}}, "after": {"row": {"num": 7}}}
{"before": {"row": {"num": 7}}, "after": {"row": {"num": 8}}}

$ kafka-ingest format=avro topic=tx schema=${tx-schema}
{"status": "BEGIN", "id": "5", "event_count": null, "data_collections": null}
{"status": "END", "id": "5", "event_count": {"long": 1}, "data_collections": {"array": [{"event_count": 1, "data_collection": "testdrive-nums-${testdrive.seed}"}]}}
{"status": "BEGIN", "id": "6", "event_count": null, "data_collections": null}
{"status": "END", "id": "6", "event_count": {"long": 1}, "data_collections": {"array": [{"event_count": 1, "data_collection": "testdrive-nums-${testdrive.seed}"}]}}

# Timestamps 4, 5, and 6 should all be available due to the longer compaction
# window.

> SELECT * FROM nums AS OF 4
6
> SELECT * FROM nums AS OF 5
7
> SELECT * FROM nums AS OF 6
8

! SELECT * FROM nums_compacted AS OF 4
contains:Timestamp (4) is not valid for all inputs
! SELECT * FROM nums_compacted AS OF 5
contains:Timestamp (5) is not valid for all inputs
> SELECT * FROM nums_compacted AS OF 6
8
