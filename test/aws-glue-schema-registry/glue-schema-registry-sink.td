# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# End-to-end test for CREATE SINK ... FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY.
# Covers:
#   - ENVELOPE DEBEZIUM sink with topic-derived schema names
#   - ENVELOPE UPSERT sink (key + value schemas registered) with topic-derived
#     names
#   - Per-side KEY/VALUE SCHEMA NAME and COMPATIBILITY LEVEL overrides, plus a
#     read-back source that decodes by the override VALUE SCHEMA NAME to prove
#     the sink registered its schema under that name
#
# Unlike a source, a sink *registers* its schema: the first publish creates the
# schema in Glue (or reuses an identical existing version) and later publishes
# add versions. Output is framed with the Glue 18-byte header, so verification
# uses `kafka-verify-data ... glue=true`, which resolves each record's
# schema-version UUID from Glue and decodes after stripping the header.
#
# `glue_conn` and `kafka_conn` are created by glue-schema-registry.td, which
# runs immediately before this file in the same testdrive invocation. Sink
# schemas are registered in that connection's registry (`--var registry-name`),
# which the workflow drops on cleanup.
#
# Restart-time register-or-reuse is not asserted here: proving no duplicate
# version was created needs a Glue version-count read the harness does not have.
# The idempotent reuse path is unit-tested in storage-client instead.
#
# This file drops every object it creates at the end. The companion scripts that
# run next against this same Materialize (restart, runtime-errors, unreachable)
# tear down the Glue backend (cut the proxy, kill moto), which would stall any
# still-running sink and trip the final "all sinks running" health check.

# ---------------------------------------------------------------------------
# ENVELOPE DEBEZIUM, topic-derived schema names
# ---------------------------------------------------------------------------

> CREATE MATERIALIZED VIEW dbz_data (a, b) AS VALUES (1, 'one'), (2, 'two')

> CREATE SINK dbz_sink
  IN CLUSTER quickstart
  FROM dbz_data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-glue-dbz-sink-${testdrive.seed}')
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=avro sink=materialize.public.dbz_sink glue=true sort-messages=true
{"before": null, "after": {"row": {"a": 1, "b": "one"}}}
{"before": null, "after": {"row": {"a": 2, "b": "two"}}}

# ---------------------------------------------------------------------------
# ENVELOPE UPSERT, topic-derived schema names (registers both key and value)
# ---------------------------------------------------------------------------

> CREATE MATERIALIZED VIEW upsert_data (k, v) AS VALUES (1, 'one'), (2, 'two')

> CREATE SINK upsert_sink
  IN CLUSTER quickstart
  FROM upsert_data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-glue-upsert-sink-${testdrive.seed}') KEY (k)
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn
  ENVELOPE UPSERT

$ kafka-verify-data format=avro sink=materialize.public.upsert_sink glue=true key=true sort-messages=true
{"k": 1} {"k": 1, "v": "one"}
{"k": 2} {"k": 2, "v": "two"}

# ---------------------------------------------------------------------------
# Per-side SCHEMA NAME + COMPATIBILITY LEVEL overrides
# ---------------------------------------------------------------------------
#
# The names live in the per-run registry, which isolates them, so they need no
# run-id suffix.

> CREATE MATERIALIZED VIEW override_data (k, v) AS VALUES (1, 'one')

> CREATE SINK override_sink
  IN CLUSTER quickstart
  FROM override_data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-glue-override-sink-${testdrive.seed}') KEY (k)
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (
    KEY SCHEMA NAME = 'mz-sink-override-key',
    VALUE SCHEMA NAME = 'mz-sink-override-value',
    KEY COMPATIBILITY LEVEL = 'FORWARD',
    VALUE COMPATIBILITY LEVEL = 'FULL'
  )
  ENVELOPE UPSERT

$ kafka-verify-data format=avro sink=materialize.public.override_sink glue=true key=true
{"k": 1} {"k": 1, "v": "one"}

# Record decoding can't observe compatibility, so read it back from Glue to
# prove the sink applied each side's COMPATIBILITY LEVEL when it created the
# schema. Both values are non-default (Glue defaults to BACKWARD) and distinct
# per side, so each assertion fails if its option is dropped or the two are
# swapped.
$ glue-verify-compatibility registry=${arg.registry-name} name=mz-sink-override-key compatibility=forward

$ glue-verify-compatibility registry=${arg.registry-name} name=mz-sink-override-value compatibility=full

# Read the sink's value records back through a source that names the writer
# schema by the override VALUE SCHEMA NAME. Purification fetches the schema from
# Glue by that name, so a successful CREATE TABLE plus a decoded row proves the
# sink registered its value schema under the override name.
> CREATE SOURCE override_readback
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-glue-override-sink-${testdrive.seed}')

> CREATE TABLE override_readback_tbl FROM SOURCE override_readback (REFERENCE "testdrive-glue-override-sink-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = 'mz-sink-override-value')
  ENVELOPE NONE

> SELECT k, v FROM override_readback_tbl
1 one

# The per-side and compatibility options are sink-only. A source reads a single
# writer schema, so purification rejects them and points at SCHEMA NAME.
! CREATE TABLE override_readback_reject FROM SOURCE override_readback (REFERENCE "testdrive-glue-override-sink-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (KEY SCHEMA NAME = 'mz-sink-override-key')
  ENVELOPE NONE
contains:are not supported for AWS Glue Schema Registry sources

# ---------------------------------------------------------------------------
# Cleanup: leave no running sinks behind for the destructive companion scripts.
# ---------------------------------------------------------------------------

> DROP SINK dbz_sink

> DROP SINK upsert_sink

> DROP SINK override_sink

> DROP SOURCE override_readback CASCADE

> DROP MATERIALIZED VIEW dbz_data

> DROP MATERIALIZED VIEW upsert_data

> DROP MATERIALIZED VIEW override_data
