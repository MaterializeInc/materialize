# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from random import getrandbits
from textwrap import dedent

from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario
from materialize.mz_version import MzVersion


class SubscribeParallel(Scenario):
    """Feature benchmarks related to SUBSCRIBE"""

    SCALE = (
        2  # So 100 concurrent SUBSCRIBEs by default, limited by database-issues#5376
    )
    FIXED_SCALE = True

    def benchmark(self) -> MeasurementSource:
        return Td(
            self.create_subscribe_source()
            + "\n".join([dedent(f"""
                        $ postgres-connect name=conn{i} url=postgres://materialize:materialize@${{testdrive.materialize-sql-addr}}
                        $ postgres-execute connection=conn{i}
                        # STRICT SERIALIZABLE is affected by database-issues#5407
                        START TRANSACTION ISOLATION LEVEL SERIALIZABLE;
                        DECLARE c{i} CURSOR FOR SUBSCRIBE s1
                        """) for i in range(0, self.n())])
            + self.insert()
            # We measure from here ...
            + dedent("""
                > SELECT COUNT(*) FROM s1;
                  /* A */
                1
                """)
            + "\n".join([dedent(f"""
                        $ postgres-execute connection=conn{i}
                        FETCH ALL FROM c{i};
                        """) for i in range(0, self.n())])
            # ... to here
            + dedent("""
                > SELECT 1
                  /* B */
                1
                """)
        )

    def create_subscribe_source(self) -> str:
        raise NotImplementedError

    def insert(self) -> str:
        raise NotImplementedError


# Only leaf scenarios run, so a scenario and its dataflow-pinned counterpart
# are siblings sharing these rather than one inheriting from the other.
TABLE_SOURCE = dedent("""
     > DROP TABLE IF EXISTS s1;
     > CREATE TABLE s1 (f1 TEXT);
     """)

TABLE_INSERT = "> INSERT INTO s1 VALUES (REPEAT('x', 1024))\n"

INDEX_SOURCE = dedent("""
     > DROP TABLE IF EXISTS s1;
     > CREATE TABLE s1 (f1 INTEGER);
     > CREATE DEFAULT INDEX ON s1;
     """)

INDEX_INSERT = "> INSERT INTO s1 VALUES (123)\n"

PIN_DATAFLOW_PATH = dedent("""
     $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
     ALTER SYSTEM SET enable_subscribe_persist_fast_path = false
     """)


class PinnedToDataflow:
    """Mixin for the counterpart of a subscribe scenario that pins the persist
    fast path off, so the dataflow path stays measured while CI defaults the
    flag on. Materialize is restarted between scenarios, so the setting does
    not leak. Not a `Scenario` itself, which would make its siblings non-leaf
    and drop them from the run.
    """

    @classmethod
    def can_run(cls, version: MzVersion) -> bool:
        # Requires `enable_subscribe_persist_fast_path`, which lands in
        # 26.41.0. Skips on both sides while we're still on `26.41.0-dev.0`
        # because `MzVersion` strips the git hash and can't distinguish dev
        # builds with the flag from dev builds without it.
        return version > MzVersion.create(26, 41, 0)


class SubscribeParallelTable(SubscribeParallel):
    def create_subscribe_source(self) -> str:
        return TABLE_SOURCE

    def insert(self) -> str:
        return TABLE_INSERT


class SubscribeParallelTableWithIndex(SubscribeParallel):
    def create_subscribe_source(self) -> str:
        return INDEX_SOURCE

    def insert(self) -> str:
        return INDEX_INSERT


class SubscribeParallelTableDataflow(PinnedToDataflow, SubscribeParallel):
    def create_subscribe_source(self) -> str:
        return PIN_DATAFLOW_PATH + TABLE_SOURCE

    def insert(self) -> str:
        return TABLE_INSERT


class SubscribeParallelTableWithIndexDataflow(PinnedToDataflow, SubscribeParallel):
    def create_subscribe_source(self) -> str:
        return PIN_DATAFLOW_PATH + INDEX_SOURCE

    def insert(self) -> str:
        return INDEX_INSERT


class SubscribeParallelKafka(SubscribeParallel):
    def create_subscribe_source(self) -> str:
        # As we are doing `kafka-ingest` in the middle of the benchmark() method
        # we must always use a unique topic to ensure isolation between the individal
        # measurements
        self._unique_topic_id = getrandbits(64)
        return dedent(f"""
             # Separate topic for each Mz instance
             $ kafka-create-topic topic=subscribe-kafka-{self._unique_topic_id}

             > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT);

             > DROP CLUSTER IF EXISTS source_cluster CASCADE;
             > CREATE CLUSTER source_cluster SIZE 'scale={self._default_size},workers=1', REPLICATION FACTOR 1;

             > DROP SOURCE IF EXISTS s1 CASCADE;

             > CREATE SOURCE s1_source
               IN CLUSTER source_cluster
               FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-subscribe-kafka-{self._unique_topic_id}-${{testdrive.seed}}');

             > CREATE TABLE s1 FROM SOURCE s1_source (REFERENCE "testdrive-subscribe-kafka-{self._unique_topic_id}-${{testdrive.seed}}")
               FORMAT BYTES ENVELOPE NONE;

             > CREATE DEFAULT INDEX ON s1;
             """)

    def insert(self) -> str:
        return dedent(f"""
            $ kafka-ingest format=bytes topic=subscribe-kafka-{self._unique_topic_id}
            123
            """)


class SubscribeParallelLarge(Scenario):
    """Feature benchmarks related to SUBSCRIBE with a large initial dataset."""

    SCALE = 6

    def benchmark(self) -> MeasurementSource:
        total_size = self.n()
        step = min(100_000, total_size)
        return Td(
            dedent(
                dedent("""
                    > DROP TABLE IF EXISTS s1;
                    > CREATE TABLE s1 (f2 INTEGER);
                    """)
                + "\n".join(
                    f"> INSERT INTO s1 SELECT generate_series FROM generate_series({n+1}, {n+step});"
                    for n in range(0, total_size, step)
                )
                # Note that we use a separate connection for the subscribe, largely so we ignore the (large) result
                # set instead of matching against it in testdrive code; we're interested in performance and not validating
                # the actual contents.
                + dedent(f"""
                    > SELECT COUNT(*) FROM s1;
                      /* A */
                    {total_size}

                    $ postgres-execute connection=postgres://materialize@${{testdrive.materialize-sql-addr}}
                    START TRANSACTION ISOLATION LEVEL SERIALIZABLE;
                    DECLARE c1 CURSOR FOR SUBSCRIBE (SELECT * FROM s1);
                    """)
                + "\n".join(
                    f"FETCH {step} c1 WITH (TIMEOUT = '60s');"
                    for n in range(0, total_size, step)
                )
                + dedent("""
                    COMMIT;

                    > SELECT 1;
                      /* B */
                    1
                    """)
            )
        )


class SubscribeNoSnapshotTable(Scenario):
    """Feature benchmarks related to SUBSCRIBE without a snapshot"""

    SCALE = 6  # Controls the size of the snapshot we skip over.

    def benchmark(self) -> MeasurementSource:
        subscribes = "\n".join(["""
             > START TRANSACTION ISOLATION LEVEL SERIALIZABLE;
             > DECLARE c1 CURSOR FOR SUBSCRIBE s1 WITH (SNAPSHOT false) AS OF ${mz_now};
             > FETCH 1 c1 WITH (TIMEOUT = '10s');
             <TIMESTAMP> 1 wow!
             > COMMIT;""" for _ in range(30)])

        return Td(f"""
             $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
             ALTER SYSTEM SET max_result_size = '100GB';

             $ set-regex match=\\d{{13}} replacement=<TIMESTAMP>

             > DROP TABLE IF EXISTS s1;
             > CREATE TABLE s1 (f1 TEXT) WITH (RETAIN HISTORY FOR '1hr');
             > INSERT INTO s1 SELECT generate_series::text from generate_series(1, {self.n()});
             > SELECT COUNT(*) FROM s1;
             {self.n()}

             $ set-from-sql var=mz_now
             SELECT mz_now()::text;

             > INSERT INTO s1 VALUES ('wow!');
               /* A */

             {subscribes}

             > SELECT 1
               /* B */
             1
            """)


class SubscribeNoSnapshotIndex(Scenario):
    """Feature benchmarks related to SUBSCRIBE without a snapshot"""

    SCALE = 6  # Controls the size of the snapshot we skip over.

    def benchmark(self) -> MeasurementSource:
        return Td(f"""
             $ set-regex match=\\d{{13}} replacement=<TIMESTAMP>

             $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
             ALTER SYSTEM SET enable_index_options = true;
             ALTER SYSTEM SET max_result_size = '100GB';

             > DROP TABLE IF EXISTS s1;
             > CREATE TABLE s1 (f1 TEXT);
             > CREATE DEFAULT INDEX ON s1 WITH (RETAIN HISTORY FOR '1hr');
             > INSERT INTO s1 SELECT generate_series::text from generate_series(1, {self.n()});
             > SELECT COUNT(*) FROM s1;
             {self.n()}

             $ set-from-sql var=mz_now
             SELECT mz_now()::text;

             > INSERT INTO s1 VALUES ('wow!');
               /* A */

             > START TRANSACTION ISOLATION LEVEL SERIALIZABLE;
             > DECLARE c1 CURSOR FOR SUBSCRIBE s1 WITH (SNAPSHOT false) AS OF ${{mz_now}};
             > FETCH 1 c1 WITH (TIMEOUT = '10s');
             <TIMESTAMP> 1 wow!
             > COMMIT;

             > SELECT 1
               /* B */
             1
            """)


def first_fetch_large_snapshot(rows: int, pin: str) -> str:
    """Create a table of `rows` rows, then time a first `FETCH` of its
    snapshot: what a client waits for before it holds any rows at all."""
    return pin + dedent(f"""
        $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
        ALTER SYSTEM SET max_result_size = '100GB';

        $ set-regex match=\\d{{13}} replacement=<TIMESTAMP>

        > DROP TABLE IF EXISTS s1;
        > CREATE TABLE s1 (f1 TEXT);
        # Zero padded so the first row of the snapshot is the same on both
        # paths, which order a timestamp's rows the same way.
        > INSERT INTO s1 SELECT lpad(generate_series::text, 9, '0') FROM generate_series(1, {rows});
        > SELECT COUNT(*) FROM s1;
        {rows}

        > SELECT 1
          /* A */
        1

        > START TRANSACTION ISOLATION LEVEL SERIALIZABLE;
        > DECLARE c1 CURSOR FOR SUBSCRIBE s1;
        > FETCH 1 c1 WITH (TIMEOUT = '300s');
        <TIMESTAMP> 1 000000001
        > COMMIT;

        > SELECT 1
          /* B */
        1
        """)


class SubscribeFirstFetchLargeSnapshot(Scenario):
    """The wait for the first rows of a large snapshot, which the persist fast
    path hands out in pieces and the dataflow path delivers whole."""

    SCALE = 6  # Rows in the snapshot, enough to span several pieces.

    def benchmark(self) -> MeasurementSource:
        return Td(first_fetch_large_snapshot(self.n(), ""))


class SubscribeFirstFetchLargeSnapshotDataflow(PinnedToDataflow, Scenario):
    """`SubscribeFirstFetchLargeSnapshot` on the dataflow path."""

    SCALE = 6

    def benchmark(self) -> MeasurementSource:
        return Td(first_fetch_large_snapshot(self.n(), PIN_DATAFLOW_PATH))
