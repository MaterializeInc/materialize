# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# End-to-end test for FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY against Moto.
# Covers:
#   - Connection validate() path
#   - Simple-schema source decode
#   - Nested-record / array / nullable-union source decode
#   - Schema evolution: records framed with v1 + v2 UUIDs against a v2 reader
#   - KEY FORMAT + VALUE FORMAT (both Glue) under ENVELOPE UPSERT
#   - ENVELOPE DEBEZIUM (Glue key + before/after envelope value)
#   - Negative cases (bad registry, missing/nonexistent SCHEMA NAME,
#     non-Avro data format, wrong connection type, bare format + UPSERT,
#     non-Kafka source)
#
# Each schema is defined once as a pretty-printed `$ set` variable and then
# referenced twice: by `glue-create-schema` (which registers it and returns its
# version UUID) and by `kafka-ingest` (which frames records with that UUID). The
# registries are created by the workflow (mzcompose.py); the schemas inside them
# are registered here so the schema bodies live in exactly one place.
#
# Cross-schema references (CSR's `VALUE REFERENCES (...)`) are *intentionally*
# not exercised here: AWS Glue Schema Registry has no equivalent concept
# (`RegisterSchemaVersion` takes one self-contained JSON blob; there is no
# `references` field on the API). Our `AvroSchema::Glue` grammar has no
# `REFERENCES` clause, so there is nothing to test. Intra-document
# named-type reuse — which is a property of Avro itself, not the registry —
# is exercised by the nested schema below: `inner_t` is defined once and
# reused by name in the `inner2` field.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_glue_schema_registry = true;

# ---------------------------------------------------------------------------
# Schema names
# ---------------------------------------------------------------------------
#
# These need no per-run uniqueness: each schema lives inside a per-run registry
# (created by mzcompose.py), which already isolates it across concurrent runs.
# Only the registries and the shared default-registry schema need run-id-suffixed
# names, so those are passed in as `--var`s by the workflow instead.

$ set simple-schema-name=mz-test-schema
$ set nested-schema-name=mz-nested-schema
$ set evolving-schema-name=mz-evolving-schema
$ set json-schema-name=mz-json-schema
$ set upsert-key-schema-name=mz-upsert-key-schema
$ set upsert-value-schema-name=mz-upsert-value-schema
$ set dbz-key-schema-name=mz-dbz-key-schema
$ set dbz-value-schema-name=mz-dbz-value-schema

# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------
#
# `aws_conn` is created by the workflow (mzcompose.py) before this script
# runs, because real-AWS runs need an optional SESSION TOKEN clause that
# testdrive can't conditionally template.

# Happy path: GSR connection. validate() pings GetRegistry against the
# configured backend (moto or real AWS).
> CREATE CONNECTION glue_conn TO AWS GLUE SCHEMA REGISTRY (
    AWS CONNECTION = aws_conn,
    REGISTRY = '${arg.registry-name}'
  )

> SELECT name, type FROM mz_connections WHERE name = 'glue_conn'
glue_conn glue-schema-registry

# Negative: validate() rejects a registry that doesn't exist. The name is
# passed in so workflow_aws can use a guaranteed-absent UUID-suffixed value.
! CREATE CONNECTION bad_registry TO AWS GLUE SCHEMA REGISTRY (
    AWS CONNECTION = aws_conn,
    REGISTRY = '${arg.nonexistent-registry-name}'
  )
contains:registry

> CREATE CONNECTION kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

# ---------------------------------------------------------------------------
# Happy path 1: simple flat schema
# ---------------------------------------------------------------------------

$ set simple-schema={
    "type": "record",
    "name": "row",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "string"}
    ]
  }

$ kafka-create-topic topic=simple partitions=1

$ glue-create-schema registry=${arg.registry-name} name=${simple-schema-name} set-version-id-var=simple-version-id schema=${simple-schema}

$ kafka-ingest format=avro topic=simple glue-schema-version-id=${simple-version-id} schema=${simple-schema} timestamp=1
{"a": 1, "b": "hello"}
{"a": 2, "b": "world"}
{"a": 42, "b": "glue"}

> CREATE SOURCE simple_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-simple-${testdrive.seed}')

> CREATE TABLE simple_table FROM SOURCE simple_source (REFERENCE "testdrive-simple-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${simple-schema-name}')
  ENVELOPE NONE

> SELECT a, b FROM simple_table ORDER BY a
1 hello
2 world
42 glue

# ---------------------------------------------------------------------------
# Happy path 2: nested record + array + nullable union
# ---------------------------------------------------------------------------

$ set nested-schema={
    "type": "record",
    "name": "outer",
    "fields": [
      {"name": "id", "type": "long"},
      {
        "name": "inner",
        "type": {
          "type": "record",
          "name": "inner_t",
          "fields": [
            {"name": "x", "type": "int"},
            {"name": "y", "type": "string"}
          ]
        }
      },
      {"name": "inner2", "type": "inner_t"},
      {"name": "tags", "type": {"type": "array", "items": "string"}},
      {"name": "note", "type": ["null", "string"], "default": null}
    ]
  }

$ kafka-create-topic topic=nested partitions=1

$ glue-create-schema registry=${arg.registry-name} name=${nested-schema-name} set-version-id-var=nested-version-id schema=${nested-schema}

$ kafka-ingest format=avro topic=nested glue-schema-version-id=${nested-version-id} schema=${nested-schema} timestamp=1
{"id": 1, "inner": {"x": 10, "y": "ten"},   "inner2": {"x": 11, "y": "eleven"},     "tags": ["a", "b"], "note": null}
{"id": 2, "inner": {"x": 20, "y": "twenty"}, "inner2": {"x": 21, "y": "twenty-one"}, "tags": [],         "note": {"string": "hi"}}

> CREATE SOURCE nested_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-nested-${testdrive.seed}')

> CREATE TABLE nested_table FROM SOURCE nested_source (REFERENCE "testdrive-nested-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${nested-schema-name}')
  ENVELOPE NONE

# `tags` is cast to text because materialize's binary pgwire encoding does
# not yet support list types (unrelated to the Glue decode path).
> SELECT id, (inner).x, (inner).y, (inner2).x, (inner2).y, tags::text, note FROM nested_table ORDER BY id
1 10 ten 11 eleven "{a,b}" <null>
2 20 twenty 21 twenty-one "{}" hi

# ---------------------------------------------------------------------------
# Happy path 3: schema evolution — records framed with v1 and v2 UUIDs both
# decode against the v2 reader pulled at planning time. The added nullable
# field with default appears as NULL for v1-framed records.
# ---------------------------------------------------------------------------

$ set evolving-v1-schema={
    "type": "record",
    "name": "evolving",
    "fields": [
      {"name": "id", "type": "long"},
      {"name": "value", "type": "string"}
    ]
  }

$ set evolving-v2-schema={
    "type": "record",
    "name": "evolving",
    "fields": [
      {"name": "id", "type": "long"},
      {"name": "value", "type": "string"},
      {"name": "extra", "type": ["null", "string"], "default": null}
    ]
  }

$ kafka-create-topic topic=evolving partitions=1

# Register v1, then a second version (v2) of the same schema name. Registering
# the same name again creates a new version rather than a new schema; CREATE
# TABLE below pins the latest (v2) as the reader.
$ glue-create-schema registry=${arg.registry-name} name=${evolving-schema-name} set-version-id-var=evolving-v1-id schema=${evolving-v1-schema}

$ glue-create-schema registry=${arg.registry-name} name=${evolving-schema-name} set-version-id-var=evolving-v2-id schema=${evolving-v2-schema}

# Two records framed with V1 (no `extra` field):
$ kafka-ingest format=avro topic=evolving glue-schema-version-id=${evolving-v1-id} schema=${evolving-v1-schema} timestamp=1
{"id": 1, "value": "v1-first"}
{"id": 2, "value": "v1-second"}

# Two records framed with V2 (extra field present, including null):
$ kafka-ingest format=avro topic=evolving glue-schema-version-id=${evolving-v2-id} schema=${evolving-v2-schema} timestamp=1
{"id": 3, "value": "v2-with-extra",    "extra": {"string": "bonus"}}
{"id": 4, "value": "v2-without-extra", "extra": null}

> CREATE SOURCE evolving_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-evolving-${testdrive.seed}')

> CREATE TABLE evolving_table FROM SOURCE evolving_source (REFERENCE "testdrive-evolving-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${evolving-schema-name}')
  ENVELOPE NONE

> SELECT id, value, extra FROM evolving_table ORDER BY id
1 v1-first        <null>
2 v1-second       <null>
3 v2-with-extra   bonus
4 v2-without-extra <null>

# ---------------------------------------------------------------------------
# Happy path 4: KEY FORMAT AVRO + VALUE FORMAT AVRO, both Glue-framed, under
# ENVELOPE UPSERT. Exercises the key decode path (each clause fetches its own
# seed) plus update + tombstone (key-only row) upsert semantics.
# ---------------------------------------------------------------------------

$ set upsert-key-schema={
    "type": "record",
    "name": "upsert_key",
    "fields": [
      {"name": "key", "type": "string"}
    ]
  }

# No `key` field in the value: ENVELOPE UPSERT promotes the key columns into the
# relation, so a `key` field here would collide with the key schema's column.
$ set upsert-value-schema={
    "type": "record",
    "name": "upsert_val",
    "fields": [
      {"name": "f", "type": "long"}
    ]
  }

$ kafka-create-topic topic=upsert partitions=1

$ glue-create-schema registry=${arg.registry-name} name=${upsert-key-schema-name} set-version-id-var=upsert-key-version-id schema=${upsert-key-schema}

$ glue-create-schema registry=${arg.registry-name} name=${upsert-value-schema-name} set-version-id-var=upsert-value-version-id schema=${upsert-value-schema}

$ kafka-ingest format=avro topic=upsert key-format=avro key-glue-schema-version-id=${upsert-key-version-id} key-schema=${upsert-key-schema} glue-schema-version-id=${upsert-value-version-id} schema=${upsert-value-schema} timestamp=1
{"key": "fish"} {"f": 1000}
{"key": "bird"} {"f": 1}
{"key": "fish"} {"f": 2000}
{"key": "bird"}
{"key": "cat"} {"f": 7}

> CREATE SOURCE upsert_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-${testdrive.seed}')

> CREATE TABLE upsert_table FROM SOURCE upsert_source (REFERENCE "testdrive-upsert-${testdrive.seed}")
  KEY FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${upsert-key-schema-name}')
  VALUE FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${upsert-value-schema-name}')
  ENVELOPE UPSERT

# fish updated to 2000, bird tombstoned, cat inserted.
> SELECT key, f FROM upsert_table ORDER BY key
cat  7
fish 2000

# ---------------------------------------------------------------------------
# Happy path 5: ENVELOPE DEBEZIUM with a Glue-framed key + a Glue-framed
# before/after envelope value. CSR can use a bare format for Debezium, but
# Glue resolves one named schema per FORMAT clause, so KEY FORMAT + VALUE
# FORMAT are required. Exercises create/update (after set) and delete
# (after = null).
# ---------------------------------------------------------------------------

$ set dbz-key-schema={
    "type": "record",
    "name": "dbz_key",
    "fields": [
      {"name": "id", "type": "long"}
    ]
  }

# The planner only requires an `after` record (and a matching `before` if
# present); `op` / `source` are not needed.
$ set dbz-value-schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "type": "record",
            "name": "row",
            "fields": [
              {"name": "id", "type": "long"},
              {"name": "name", "type": "string"}
            ]
          },
          "null"
        ]
      },
      {"name": "after", "type": ["row", "null"]}
    ]
  }

$ kafka-create-topic topic=dbz partitions=1

$ glue-create-schema registry=${arg.registry-name} name=${dbz-key-schema-name} set-version-id-var=dbz-key-version-id schema=${dbz-key-schema}

$ glue-create-schema registry=${arg.registry-name} name=${dbz-value-schema-name} set-version-id-var=dbz-value-version-id schema=${dbz-value-schema}

$ kafka-ingest format=avro topic=dbz key-format=avro key-glue-schema-version-id=${dbz-key-version-id} key-schema=${dbz-key-schema} glue-schema-version-id=${dbz-value-version-id} schema=${dbz-value-schema} timestamp=1
{"id": 1} {"before": null, "after": {"row": {"id": 1, "name": "first"}}}
{"id": 2} {"before": null, "after": {"row": {"id": 2, "name": "second"}}}
{"id": 1} {"before": {"row": {"id": 1, "name": "first"}}, "after": {"row": {"id": 1, "name": "first-updated"}}}
{"id": 2} {"before": {"row": {"id": 2, "name": "second"}}, "after": null}

> CREATE SOURCE dbz_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-${testdrive.seed}')

> CREATE TABLE dbz_table FROM SOURCE dbz_source (REFERENCE "testdrive-dbz-${testdrive.seed}")
  KEY FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${dbz-key-schema-name}')
  VALUE FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${dbz-value-schema-name}')
  ENVELOPE DEBEZIUM

# id 1 updated to first-updated, id 2 deleted (after = null).
> SELECT id, name FROM dbz_table ORDER BY id
1 first-updated

# ---------------------------------------------------------------------------
# Negative: a bare (value-only) Glue FORMAT under ENVELOPE UPSERT must be
# rejected — UPSERT requires a key, so a KEY FORMAT clause is mandatory.
# ---------------------------------------------------------------------------

> CREATE SOURCE neg_upsert_no_key_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-simple-${testdrive.seed}')

! CREATE TABLE neg_upsert_no_key_table FROM SOURCE neg_upsert_no_key_source (REFERENCE "testdrive-simple-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${simple-schema-name}')
  ENVELOPE UPSERT
contains:requires that KEY FORMAT be specified

> DROP SOURCE neg_upsert_no_key_source

# ---------------------------------------------------------------------------
# Negative: SCHEMA NAME points to a schema that doesn't exist in the registry.
# Purification should fail with a clear error before CREATE TABLE commits.
# ---------------------------------------------------------------------------

> CREATE SOURCE neg_missing_schema_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-simple-${testdrive.seed}')

! CREATE TABLE neg_missing_schema_table FROM SOURCE neg_missing_schema_source (REFERENCE "testdrive-simple-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = 'no-such-schema')
  ENVELOPE NONE
contains:Glue schema lookup failed

> DROP SOURCE neg_missing_schema_source

# ---------------------------------------------------------------------------
# Negative: SCHEMA NAME option is required by the planner.
# ---------------------------------------------------------------------------

> CREATE SOURCE neg_no_schema_name_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-simple-${testdrive.seed}')

! CREATE TABLE neg_no_schema_name_table FROM SOURCE neg_no_schema_name_source (REFERENCE "testdrive-simple-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn
  ENVELOPE NONE
contains:SCHEMA NAME option is required

> DROP SOURCE neg_no_schema_name_source

# ---------------------------------------------------------------------------
# Negative: schema exists in the registry but its data-format is not AVRO.
# Purification must reject this with a clear SQL error before any record
# decode is attempted — the runtime path only handles Avro. The JSON-format
# schema is registered just to be rejected, so it has no version-id var and is
# never ingested. The contents are irrelevant — the data-format check fires
# before the body is parsed.
# ---------------------------------------------------------------------------

$ glue-create-schema registry=${arg.registry-name} name=${json-schema-name} data-format=json compatibility=none
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {"id": {"type": "integer"}}
}

> CREATE SOURCE neg_json_format_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-simple-${testdrive.seed}')

! CREATE TABLE neg_json_format_table FROM SOURCE neg_json_format_source (REFERENCE "testdrive-simple-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${json-schema-name}')
  ENVELOPE NONE
contains:unsupported data format JSON

> DROP SOURCE neg_json_format_source

# ---------------------------------------------------------------------------
# Negative: FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY pointed at a
# non-Glue connection (here the Kafka connection itself). Purification
# rejects this before any AWS call.
# ---------------------------------------------------------------------------

> CREATE SOURCE neg_wrong_conn_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-simple-${testdrive.seed}')

! CREATE TABLE neg_wrong_conn_table FROM SOURCE neg_wrong_conn_source (REFERENCE "testdrive-simple-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION kafka_conn (SCHEMA NAME = '${simple-schema-name}')
  ENVELOPE NONE
contains:is not an AWS GLUE SCHEMA REGISTRY CONNECTION

> DROP SOURCE neg_wrong_conn_source

# ---------------------------------------------------------------------------
# Negative: Glue is only supported on Kafka sources. The parser accepts a
# FORMAT clause on any source (it only sees the source name), so the
# restriction is enforced during purification. A load-generator source is the
# cheapest non-Kafka source to drive this — no external system required.
# ---------------------------------------------------------------------------

> CREATE SOURCE neg_loadgen_source
  IN CLUSTER quickstart
  FROM LOAD GENERATOR COUNTER (UP TO 5)

! CREATE TABLE neg_non_kafka_table FROM SOURCE neg_loadgen_source
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${simple-schema-name}')
  ENVELOPE NONE
contains:only supported with Kafka sources

> DROP SOURCE neg_loadgen_source

# ---------------------------------------------------------------------------
# Runtime registry enforcement: a record framed with a UUID from a schema
# that has the SAME name but lives in a DIFFERENT registry must be rejected
# at decode time. Glue schema-version UUIDs are globally unique within an
# account and GetSchemaVersion(uuid) is not registry-scoped, so without
# this check the source would silently accept records from any registry
# the credentials can read.
# ---------------------------------------------------------------------------

# Same schema NAME as the primary registry's simple schema, but a different
# definition (extra `c` field), registered in the OTHER registry.
$ set other-samename-schema={
    "type": "record",
    "name": "row",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "string"},
      {"name": "c", "type": "string"}
    ]
  }

$ glue-create-schema registry=${arg.other-registry-name} name=${simple-schema-name} set-version-id-var=other-samename-version-id schema=${other-samename-schema}

$ kafka-create-topic topic=cross-registry partitions=1

# One legit record (UUID belongs to the configured registry):
$ kafka-ingest format=avro topic=cross-registry glue-schema-version-id=${simple-version-id} schema=${simple-schema} timestamp=1
{"a": 100, "b": "good"}

# One bogus record (same schema name, but UUID belongs to OTHER_REGISTRY):
$ kafka-ingest format=avro topic=cross-registry glue-schema-version-id=${other-samename-version-id} schema=${other-samename-schema} timestamp=1
{"a": 200, "b": "wrong-registry", "c": "extra"}

> CREATE SOURCE cross_registry_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-cross-registry-${testdrive.seed}')

> CREATE TABLE cross_registry_table FROM SOURCE cross_registry_source (REFERENCE "testdrive-cross-registry-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${simple-schema-name}')
  ENVELOPE NONE

# The bogus record surfaces as a permanent decode error on the table; the
# legit record is unobservable from the error-bearing query but the error
# message itself proves the cross-registry UUID was caught.
! SELECT * FROM cross_registry_table
contains:lives in registry "${arg.other-registry-name}"

# ---------------------------------------------------------------------------
# Glue's implicit default registry, addressed by the literal name
# "default-registry". Verifies the no-RegistryId code path works end to end.
# ---------------------------------------------------------------------------

$ set default-schema={
    "type": "record",
    "name": "default_row",
    "fields": [
      {"name": "id", "type": "long"},
      {"name": "label", "type": "string"}
    ]
  }

# Register the schema (no registry= → the implicit default registry) *before*
# creating the connection: the connection's validate() pings
# GetRegistry('default-registry'), and moto only materializes the implicit
# default registry once something has been created in it.
$ glue-create-schema name=${arg.default-schema-name} set-version-id-var=default-version-id schema=${default-schema}

> CREATE CONNECTION default_glue_conn TO AWS GLUE SCHEMA REGISTRY (
    AWS CONNECTION = aws_conn,
    REGISTRY = '${arg.default-registry-name}'
  )

$ kafka-create-topic topic=default-registry partitions=1

$ kafka-ingest format=avro topic=default-registry glue-schema-version-id=${default-version-id} schema=${default-schema} timestamp=1
{"id": 1, "label": "first"}
{"id": 2, "label": "second"}

> CREATE SOURCE default_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-default-registry-${testdrive.seed}')

> CREATE TABLE default_table FROM SOURCE default_source (REFERENCE "testdrive-default-registry-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION default_glue_conn (SCHEMA NAME = '${arg.default-schema-name}')
  ENVELOPE NONE

> SELECT id, label FROM default_table ORDER BY id
1 first
2 second
