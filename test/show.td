# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

$ set names-schema={
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                {
                    "name": "row",
                    "type": "record",
                    "fields": [
                        {"name": "num", "type": "long"},
                        {"name": "name", "type": "string"}
                    ]
                },
                "null"
            ]
        },
        { "name": "after", "type": ["row", "null"] }
    ]
  }

$ set mods-schema={
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                {
                    "name": "row",
                    "type": "record",
                    "fields": [
                        {"name": "num", "type": "long"},
                        {"name": "mod", "type": "string"}
                    ]
                },
                "null"
            ]
        },
        { "name": "after", "type": ["row", "null"] }
    ]
  }

$ set plurals-schema={
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                {
                    "name": "row",
                    "type": "record",
                    "fields": [
                        {"name": "num", "type": "string"},
                        {"name": "noun", "type": "string"}
                    ]
                },
                "null"
            ]
        },
        { "name": "after", "type": ["row", "null"] }
    ]
  }

> CREATE SOURCE names FROM 'kafka://${testdrive.kafka-addr}/testdrive-names-${testdrive.seed}'
  USING SCHEMA '${names-schema}'

> CREATE SOURCE mods FROM 'kafka://${testdrive.kafka-addr}/testdrive-mods-${testdrive.seed}'
  USING SCHEMA '${mods-schema}'

> CREATE SOURCE plurals FROM 'kafka://${testdrive.kafka-addr}/testdrive-plurals-${testdrive.seed}'
  USING SCHEMA '${plurals-schema}'

> CREATE MATERIALIZED VIEW test1 AS SELECT 1

> SHOW SOURCES
SOURCES
----
names
mods
plurals


> SHOW FULL SOURCES
SOURCES | TYPE
----
names USER
mods USER
plurals USER


> SHOW VIEWS
VIEWS
----
 test1
 mz_arrangement_sharing
 mz_arrangement_sizes
 mz_catalog_names
 mz_dataflow_channels
 mz_dataflow_operator_addresses
 mz_dataflow_operators
 mz_materialization_dependencies
 mz_materializations
 mz_peek_active
 mz_peek_durations
 mz_scheduling_elapsed
 mz_scheduling_histogram
 mz_scheduling_parks
 mz_view_foreign_keys
 mz_view_frontiers
 mz_view_keys

> SHOW FULL VIEWS
VIEWS TYPE
----
 mz_arrangement_sharing SYSTEM
 mz_arrangement_sizes SYSTEM
 mz_catalog_names SYSTEM
 mz_dataflow_channels SYSTEM
 mz_dataflow_operator_addresses SYSTEM
 mz_dataflow_operators SYSTEM
 mz_materialization_dependencies SYSTEM
 mz_materializations SYSTEM
 mz_peek_active SYSTEM
 mz_peek_durations SYSTEM
 mz_scheduling_elapsed SYSTEM
 mz_scheduling_histogram SYSTEM
 mz_scheduling_parks SYSTEM
 mz_view_foreign_keys SYSTEM
 mz_view_frontiers SYSTEM
 mz_view_keys SYSTEM
 test1 USER
