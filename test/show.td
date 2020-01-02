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
materialize.public.names
materialize.public.mods
materialize.public.plurals


> SHOW FULL SOURCES
SOURCES                  TYPE
-----------------------------
materialize.public.names   USER
materialize.public.mods    USER
materialize.public.plurals USER


> SHOW VIEWS
VIEWS
-----
materialize.public.test1
mz_catalog.mz_arrangement_sharing
mz_catalog.mz_arrangement_sizes
mz_catalog.mz_catalog_names
mz_catalog.mz_dataflow_channels
mz_catalog.mz_dataflow_operator_addresses
mz_catalog.mz_dataflow_operators
mz_catalog.mz_materialization_dependencies
mz_catalog.mz_materializations
mz_catalog.mz_peek_active
mz_catalog.mz_peek_durations
mz_catalog.mz_scheduling_elapsed
mz_catalog.mz_scheduling_histogram
mz_catalog.mz_scheduling_parks
mz_catalog.mz_view_foreign_keys
mz_catalog.mz_view_frontiers
mz_catalog.mz_view_keys

> SHOW FULL VIEWS
VIEWS                                      TYPE
--------------------------------------------------------
mz_catalog.mz_arrangement_sharing          SYSTEM
mz_catalog.mz_arrangement_sizes            SYSTEM
mz_catalog.mz_catalog_names                SYSTEM
mz_catalog.mz_dataflow_channels            SYSTEM
mz_catalog.mz_dataflow_operator_addresses  SYSTEM
mz_catalog.mz_dataflow_operators           SYSTEM
mz_catalog.mz_materialization_dependencies SYSTEM
mz_catalog.mz_materializations             SYSTEM
mz_catalog.mz_peek_active                  SYSTEM
mz_catalog.mz_peek_durations               SYSTEM
mz_catalog.mz_scheduling_elapsed           SYSTEM
mz_catalog.mz_scheduling_histogram         SYSTEM
mz_catalog.mz_scheduling_parks             SYSTEM
mz_catalog.mz_view_foreign_keys            SYSTEM
mz_catalog.mz_view_frontiers               SYSTEM
mz_catalog.mz_view_keys                    SYSTEM
materialize.public.test1                     USER
