# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

> CREATE SOURCE names FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-names-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${names-schema}'
  ENVELOPE DEBEZIUM

> CREATE SOURCE mods FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-mods-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${mods-schema}'
  ENVELOPE DEBEZIUM

> CREATE SOURCE plurals FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-plurals-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${plurals-schema}'
  ENVELOPE DEBEZIUM

> CREATE MATERIALIZED VIEW names_view as SELECT * from names;

> CREATE MATERIALIZED VIEW mods_view as SELECT * from mods;

> CREATE VIEW plurals_view as SELECT * from plurals;

> CREATE VIEW test1 AS
  SELECT * FROM names_view, mods_view WHERE names_view.num = mods_view.num AND mods_view.mod = 'even';

> CREATE VIEW test2 AS
  SELECT * FROM names_view, mods_view, plurals_view WHERE names_view.num = mods_view.num AND names_view.name = plurals_view.num;

> SHOW SOURCES
SOURCES
----
names
mods
plurals


> SHOW FULL SOURCES
SOURCES  TYPE
-----------------------------
names    USER
mods     USER
plurals  USER


> SHOW VIEWS
VIEWS
-----
mods_view
names_view
plurals_view
test1
test2

> SHOW VIEWS FROM public
VIEWS
-----
mods_view
names_view
plurals_view
test1
test2

> SHOW VIEWS FROM materialize.public
VIEWS
-----
mods_view
names_view
plurals_view
test1
test2

! SHOW VIEWS FROM noexist
schema 'noexist' does not exist

! SHOW VIEWS FROM noexist.noexist
unknown database 'noexist'

! SHOW EXTENDED VIEWS
Expected one of SCHEMAS or INDEX or INDEXES or KEYS or TABLES or COLUMNS or FULL, found: VIEWS

! SHOW EXTENDED FULL VIEWS
Expected one of SCHEMAS or COLUMNS or TABLES, found: VIEWS

> SHOW FULL VIEWS
VIEWS        TYPE QUERYABLE MATERIALIZED
----------------------------------
mods_view    USER true      true
names_view   USER true      true
plurals_view USER false     false
test1        USER true      false
test2        USER false     false

> SHOW FULL MATERIALIZED VIEWS
VIEWS        TYPE
-----------------
mods_view    USER
names_view   USER

> SHOW MATERIALIZED VIEWS LIKE '%name%'
VIEWS
-----
names_view

> SHOW FULL VIEWS FROM mz_catalog
VIEWS                             TYPE   QUERYABLE MATERIALIZED
---------------------------------------------------------------
mz_arrangement_sharing            SYSTEM true      true
mz_arrangement_sizes              SYSTEM true      true
mz_catalog_names                  SYSTEM true      true
mz_dataflow_channels              SYSTEM true      true
mz_dataflow_operator_addresses    SYSTEM true      true
mz_dataflow_operators             SYSTEM true      true
mz_materialization_dependencies   SYSTEM true      true
mz_materialization_frontiers      SYSTEM true      true
mz_materializations               SYSTEM true      true
mz_peek_active                    SYSTEM true      true
mz_peek_durations                 SYSTEM true      true
mz_scheduling_elapsed             SYSTEM true      true
mz_scheduling_histogram           SYSTEM true      true
mz_scheduling_parks               SYSTEM true      true
mz_view_foreign_keys              SYSTEM true      true
mz_view_keys                      SYSTEM true      true
mz_addresses_with_unit_length     SYSTEM true      true
mz_dataflow_names                 SYSTEM true      true
mz_dataflow_operator_dataflows    SYSTEM true      true
mz_perf_arrangement_records       SYSTEM true      true
mz_perf_dependency_frontiers      SYSTEM true      true
mz_perf_peek_durations_aggregates SYSTEM true      true
mz_perf_peek_durations_bucket     SYSTEM true      true
mz_perf_peek_durations_core       SYSTEM true      true
mz_records_per_dataflow           SYSTEM true      true
mz_records_per_dataflow_global    SYSTEM true      true
mz_records_per_dataflow_operator  SYSTEM true      true

# test that information in shows correctly responds to materialization and unmaterialization

> CREATE INDEX plurals_idx ON plurals_view(noun)

> SHOW FULL VIEWS
VIEWS        TYPE QUERYABLE MATERIALIZED
----------------------------------
mods_view    USER true      true
names_view   USER true      true
plurals_view USER true      true
test1        USER true      false
test2        USER true      false

> SHOW FULL MATERIALIZED VIEWS
VIEWS        TYPE
-----------------
mods_view    USER
names_view   USER
plurals_view USER

> SHOW MATERIALIZED VIEWS
VIEWS
--------
mods_view
names_view
plurals_view

> DROP INDEX names_view_primary_idx

> SHOW FULL VIEWS
VIEWS        TYPE QUERYABLE MATERIALIZED
----------------------------------
mods_view    USER true      true
names_view   USER false     false
plurals_view USER true      true
test1        USER false     false
test2        USER false     false

> SHOW FULL MATERIALIZED VIEWS
VIEWS        TYPE
-----------------
mods_view    USER
plurals_view USER

> SHOW MATERIALIZED VIEWS
VIEWS
--------
mods_view
plurals_view
