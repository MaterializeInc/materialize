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
#   - Connection validate() path (Stage 3c)
#   - Simple-schema source decode
#   - Nested-record / array / nullable-union source decode
#   - Schema evolution: records framed with v1 + v2 UUIDs against a v2 reader
#   - Negative cases (bad registry, missing/nonexistent SCHEMA NAME)
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

$ kafka-create-topic topic=simple partitions=1

$ kafka-ingest format=avro topic=simple glue-schema-version-id=${arg.simple-version-id} schema={"type":"record","name":"row","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]} timestamp=1
{"a": 1, "b": "hello"}
{"a": 2, "b": "world"}
{"a": 42, "b": "glue"}

> CREATE SOURCE simple_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-simple-${testdrive.seed}')

> CREATE TABLE simple_table FROM SOURCE simple_source (REFERENCE "testdrive-simple-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${arg.simple-schema-name}')
  ENVELOPE NONE

> SELECT a, b FROM simple_table ORDER BY a
1 hello
2 world
42 glue

# ---------------------------------------------------------------------------
# Happy path 2: nested record + array + nullable union
# ---------------------------------------------------------------------------

$ kafka-create-topic topic=nested partitions=1

$ kafka-ingest format=avro topic=nested glue-schema-version-id=${arg.nested-version-id} schema={"type":"record","name":"outer","fields":[{"name":"id","type":"long"},{"name":"inner","type":{"type":"record","name":"inner_t","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}},{"name":"inner2","type":"inner_t"},{"name":"tags","type":{"type":"array","items":"string"}},{"name":"note","type":["null","string"],"default":null}]} timestamp=1
{"id": 1, "inner": {"x": 10, "y": "ten"},   "inner2": {"x": 11, "y": "eleven"},     "tags": ["a", "b"], "note": null}
{"id": 2, "inner": {"x": 20, "y": "twenty"}, "inner2": {"x": 21, "y": "twenty-one"}, "tags": [],         "note": {"string": "hi"}}

> CREATE SOURCE nested_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-nested-${testdrive.seed}')

> CREATE TABLE nested_table FROM SOURCE nested_source (REFERENCE "testdrive-nested-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${arg.nested-schema-name}')
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

$ kafka-create-topic topic=evolving partitions=1

# Two records framed with V1 (no `extra` field):
$ kafka-ingest format=avro topic=evolving glue-schema-version-id=${arg.evolving-v1-id} schema={"type":"record","name":"evolving","fields":[{"name":"id","type":"long"},{"name":"value","type":"string"}]} timestamp=1
{"id": 1, "value": "v1-first"}
{"id": 2, "value": "v1-second"}

# Two records framed with V2 (extra field present, including null):
$ kafka-ingest format=avro topic=evolving glue-schema-version-id=${arg.evolving-v2-id} schema={"type":"record","name":"evolving","fields":[{"name":"id","type":"long"},{"name":"value","type":"string"},{"name":"extra","type":["null","string"],"default":null}]} timestamp=1
{"id": 3, "value": "v2-with-extra",    "extra": {"string": "bonus"}}
{"id": 4, "value": "v2-without-extra", "extra": null}

> CREATE SOURCE evolving_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-evolving-${testdrive.seed}')

> CREATE TABLE evolving_table FROM SOURCE evolving_source (REFERENCE "testdrive-evolving-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${arg.evolving-schema-name}')
  ENVELOPE NONE

> SELECT id, value, extra FROM evolving_table ORDER BY id
1 v1-first        <null>
2 v1-second       <null>
3 v2-with-extra   bonus
4 v2-without-extra <null>

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
# decode is attempted — the runtime path only handles Avro.
# ---------------------------------------------------------------------------

> CREATE SOURCE neg_json_format_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-simple-${testdrive.seed}')

! CREATE TABLE neg_json_format_table FROM SOURCE neg_json_format_source (REFERENCE "testdrive-simple-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${arg.json-schema-name}')
  ENVELOPE NONE
contains:unsupported data format JSON

> DROP SOURCE neg_json_format_source

# ---------------------------------------------------------------------------
# Runtime registry enforcement: a record framed with a UUID from a schema
# that has the SAME name but lives in a DIFFERENT registry must be rejected
# at decode time. Glue schema-version UUIDs are globally unique within an
# account and GetSchemaVersion(uuid) is not registry-scoped, so without
# this check the source would silently accept records from any registry
# the credentials can read.
# ---------------------------------------------------------------------------

$ kafka-create-topic topic=cross-registry partitions=1

# One legit record (UUID belongs to the configured registry):
$ kafka-ingest format=avro topic=cross-registry glue-schema-version-id=${arg.simple-version-id} schema={"type":"record","name":"row","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]} timestamp=1
{"a": 100, "b": "good"}

# One bogus record (same schema name, but UUID belongs to OTHER_REGISTRY):
$ kafka-ingest format=avro topic=cross-registry glue-schema-version-id=${arg.other-samename-version-id} schema={"type":"record","name":"row","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"},{"name":"c","type":"string"}]} timestamp=1
{"a": 200, "b": "wrong-registry", "c": "extra"}

> CREATE SOURCE cross_registry_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-cross-registry-${testdrive.seed}')

> CREATE TABLE cross_registry_table FROM SOURCE cross_registry_source (REFERENCE "testdrive-cross-registry-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${arg.simple-schema-name}')
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

> CREATE CONNECTION default_glue_conn TO AWS GLUE SCHEMA REGISTRY (
    AWS CONNECTION = aws_conn,
    REGISTRY = '${arg.default-registry-name}'
  )

$ kafka-create-topic topic=default-registry partitions=1

$ kafka-ingest format=avro topic=default-registry glue-schema-version-id=${arg.default-version-id} schema={"type":"record","name":"default_row","fields":[{"name":"id","type":"long"},{"name":"label","type":"string"}]} timestamp=1
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
