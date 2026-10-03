# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check, externally_idempotent

# Row packing canonicalizes floats (-0.0 packs as +0.0, every NaN packs as one
# bit pattern) so that byte equality of packed rows agrees with SQL float
# equality. Data written by versions without that canonicalization carries the
# raw bit patterns, so these checks exercise the cross-version story: rows
# written by an older version must cancel against retractions written by a
# newer one, and must land in the same DISTINCT/GROUP BY/index groups.
#
# In multi-version scenarios validate() also runs on versions without the
# canonicalization, where -0.0 is a distinct arrangement key, so the
# assertions that depend on it are gated on version 26.33 (which is when the
# canonicalization was introduced). Row counts and upsert results hold on all
# versions and are asserted unconditionally.
#
# NOTE: a genuine -0.0 needs a text->float cast ('-0'), a -0.0 literal goes
# through numeric (no signed zero) and arrives as +0.0.


class FloatCanonicalizationTable(Check):
    """-0.0 and NaN in a table across versions: retraction of old-encoding
    rows, DISTINCT/GROUP BY collapse, and index point lookups."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                > CREATE TABLE float_canon_table (id INT, f DOUBLE PRECISION);
                > INSERT INTO float_canon_table VALUES
                  (1, '-0'), (2, '0'), (3, 'NaN'), (4, '-0'), (5, 1.5);
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW float_canon_table_mv AS
                  SELECT f, COUNT(*) AS c FROM float_canon_table GROUP BY f;
                > CREATE DEFAULT INDEX ON float_canon_table;
                > INSERT INTO float_canon_table VALUES (6, '-0');
                """,
                """
                > DELETE FROM float_canon_table WHERE id = 4;
                > INSERT INTO float_canon_table VALUES (7, '0'), (8, 'NaN');
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                > SELECT count(*) FROM float_canon_table;
                7

                >[version>=2603300] SELECT count(*) FROM (SELECT DISTINCT f FROM float_canon_table);
                3

                >[version>=2603300] SELECT f::text, c FROM float_canon_table_mv;
                0 4
                1.5 1
                NaN 2

                >[version>=2603300] SELECT id FROM float_canon_table WHERE f = 0;
                1
                2
                6
                7

                >[version>=2603300] SELECT id FROM float_canon_table WHERE f = 'NaN';
                3
                8
                """))


@externally_idempotent(False)
class FloatCanonicalizationPgCdc(Check):
    """-0.0 and NaN ingested from a Postgres source across versions: an
    upstream DELETE/UPDATE after an upgrade must retract rows whose additions
    were written with the old float encoding."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                CREATE USER postgres_float_canon WITH SUPERUSER PASSWORD 'postgres';
                ALTER USER postgres_float_canon WITH replication;
                DROP PUBLICATION IF EXISTS float_canon_publication;
                DROP TABLE IF EXISTS float_canon_pg_table;
                CREATE TABLE float_canon_pg_table (id INT PRIMARY KEY, f DOUBLE PRECISION);
                ALTER TABLE float_canon_pg_table REPLICA IDENTITY FULL;
                INSERT INTO float_canon_pg_table VALUES (1, '-0'), (2, '0'), (3, 'NaN'), (4, '-0');
                CREATE PUBLICATION float_canon_publication FOR ALL TABLES;

                > CREATE SECRET float_canon_pgpass AS 'postgres';

                > CREATE CONNECTION float_canon_pg_conn FOR POSTGRES
                  HOST 'postgres',
                  DATABASE postgres,
                  USER postgres_float_canon,
                  PASSWORD SECRET float_canon_pgpass;

                > CREATE SOURCE float_canon_pg_source
                  FROM POSTGRES CONNECTION float_canon_pg_conn
                  (PUBLICATION 'float_canon_publication');
                > CREATE TABLE float_canon_pg FROM SOURCE float_canon_pg_source
                  (REFERENCE float_canon_pg_table);

                # Wait for the snapshot so the initial rows are ingested (and
                # thus encoded) by the version running this phase.
                > SELECT count(*) FROM float_canon_pg;
                4
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO float_canon_pg_table VALUES (5, '-0'), (6, 'NaN');
                """,
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DELETE FROM float_canon_pg_table WHERE id IN (1, 6);
                UPDATE float_canon_pg_table SET f = '0' WHERE id = 4;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                > SELECT count(*) FROM float_canon_pg;
                4

                >[version>=2603300] SELECT count(*) FROM (SELECT DISTINCT f FROM float_canon_pg);
                2

                >[version>=2603300] SELECT id FROM float_canon_pg WHERE f = 0;
                2
                4
                5

                >[version>=2603300] SELECT id FROM float_canon_pg WHERE f = 'NaN';
                3
                """))


def float_canon_schemas() -> str:
    return dedent("""
        $ set float-canon-keyschema={
            "type": "record",
            "name": "Key",
            "fields": [ {"name": "key1", "type": "double"} ]
          }

        $ set float-canon-schema={
            "type" : "record",
            "name" : "test",
            "fields" : [ {"name": "f1", "type": "double"} ]
          }
        """)


class FloatCanonicalizationUpsert(Check):
    """-0.0 in a Kafka upsert source's key and value across versions: a -0.0
    and a +0.0 key are the same key, and a post-upgrade tombstone must retract
    a value row written with the old float encoding."""

    def initialize(self) -> Testdrive:
        return Testdrive(float_canon_schemas() + dedent("""
                $ kafka-create-topic topic=float-canon-upsert

                $ kafka-ingest format=avro key-format=avro topic=float-canon-upsert key-schema=${float-canon-keyschema} schema=${float-canon-schema}
                {"key1": -0.0} {"f1": 1.0}
                {"key1": 2.0} {"f1": -0.0}

                > CREATE SOURCE float_canon_upsert_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-float-canon-upsert-${testdrive.seed}')
                > CREATE TABLE float_canon_upsert FROM SOURCE float_canon_upsert_src (REFERENCE "testdrive-float-canon-upsert-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                # Wait for the snapshot so the initial rows are ingested (and
                # thus encoded) by the version running this phase.
                > SELECT count(*) FROM float_canon_upsert;
                2
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(float_canon_schemas() + dedent(s))
            for s in [
                """
                # The +0.0 key is the same key as the -0.0 key, so this
                # replaces the (0, 1) row rather than adding a third row.
                $ kafka-ingest format=avro key-format=avro topic=float-canon-upsert key-schema=${float-canon-keyschema} schema=${float-canon-schema}
                {"key1": 0.0} {"f1": 3.0}
                """,
                """
                # Tombstone the key whose value row (f1 = -0.0) may have been
                # written with the old float encoding.
                $ kafka-ingest format=avro key-format=avro topic=float-canon-upsert key-schema=${float-canon-keyschema} schema=${float-canon-schema}
                {"key1": 2.0}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                > SELECT key1::text, f1::text FROM float_canon_upsert;
                0 3
                """))
