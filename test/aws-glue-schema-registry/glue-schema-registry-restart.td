# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Companion to glue-schema-registry.td, run AFTER the workflow restarts
# Materialize. Tests that the Glue interaction survives a restart, not the Avro
# decode (covered elsewhere):
#
#   - glue_conn and the pre-restart simple_source/simple_table rehydrate from
#     the catalog.
#   - A table planned post-restart re-runs purification (GetSchemaVersion by
#     name, cold caches) and decodes the pre-restart records.
#
# consistent_seed pins the topic name to the one glue-schema-registry.td used.

$ set simple-schema-name=mz-test-schema

# Pre-restart table rehydrates from persist, no Glue call. Confirms the catalog
# object survived.
> SELECT a, b FROM simple_table ORDER BY a
1 hello
2 world
42 glue

# A table planned post-restart re-resolves its schema against the registry
# (GetSchemaVersion by name) and decodes the pre-restart topic contents.
> CREATE SOURCE simple_source_after_restart
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-simple-${testdrive.seed}')

> CREATE TABLE simple_table_after_restart FROM SOURCE simple_source_after_restart (REFERENCE "testdrive-simple-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${simple-schema-name}')
  ENVELOPE NONE

> SELECT a, b FROM simple_table_after_restart ORDER BY a
1 hello
2 world
42 glue
